from pyquokka.dataset.base_dataset import *

class InputSortedEC2ParquetDataset:

    def __init__(self, files, partitioner, columns=None, filters=None, mode = "stride", workers = 4, name_column = None) -> None:

        self.files = files
        self.partitioner = partitioner

        self.num_channels = None
        self.columns = columns
        self.filters = filters

        self.length = 0
        self.workers = workers

        self.s3 = None
        self.iterator = None
        self.count = 0
        self.bounds = None

        assert mode in ["stride", "range"]
        self.mode = mode
        self.name_column = name_column
        if self.name_column is not None and self.columns is not None and self.name_column in self.columns:
            self.columns.remove(self.name_column)

    def _get_bounds(self, num_channels):

        @ray.remote
        def get_stats_for_fragments(fragments):
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
            def get_stats_for_fragment(fragment):
                field_index = fragment.physical_schema.get_field_index(self.partitioner)
                metadata = fragment.metadata
                min_timestamp = None
                max_timestamp = None
                for row_group_index in range(metadata.num_row_groups):
                    stats = metadata.row_group(row_group_index).column(field_index).statistics
                    # Parquet files can be created without statistics
                    if stats is None:
                        raise Exception("Parquet files must have statistics!")
                    row_group_max = stats.max
                    row_group_min = stats.min
                    if max_timestamp is None or row_group_max > max_timestamp:
                        max_timestamp = row_group_max
                    if min_timestamp is None or row_group_min < min_timestamp:
                        min_timestamp = row_group_min
                assert min_timestamp is not None and max_timestamp is not None 
                return fragment.path, min_timestamp, max_timestamp
            futures = []
            for fragment in fragments:
                futures.append(executor.submit(get_stats_for_fragment, fragment))
            return [future.result() for future in futures]
        
        channel_infos = {}
        fragments = []
        self.num_channels = num_channels
        s3fs = S3FileSystem()
        dataset = ds.dataset(self.files, format = "parquet", filesystem=s3fs )
        fragments = list(dataset.get_fragments())

        ips = [k for k in ray.available_resources() if 'node' in k]
        fragments_per_ip = len(fragments) // len(ips) + 1
        fragments_futs = []
        for i in range(len(ips)):
            ip = ips[i]
            fragments_futs.append(get_stats_for_fragments.options(resources = {ip : 0.001}).\
                remote(fragments[i * fragments_per_ip : (i+1) * fragments_per_ip]))
        
        fragments = ray.get(fragments_futs)
        # fragments is a list of lists, unnest the lists
        fragments = [item for sublist in fragments for item in sublist]
        fragments = sorted(fragments, key = lambda x: x[1])

        # print(fragments)

        for k in range(1, len(fragments)):
            assert overlap([fragments[k-1][1], fragments[k-1][2]], [fragments[k][1], fragments[k][2]]) <= 0, \
                "positive overlap, data is not sorted! {} {} {} {}".format(fragments[k-1][0], fragments[k-1][1], fragments[k-1][2], fragments[k][0], fragments[k][1], fragments[k][2])

        fragments_per_channel = math.ceil(len(fragments) / num_channels)

        if self.mode == "range":
            channel_bounds = {}
            for channel in range(num_channels):
                channel_infos[channel] = fragments[channel * fragments_per_channel : channel * fragments_per_channel + fragments_per_channel]
                channel_bounds[channel] = (channel_infos[channel][0][1], channel_infos[channel][-1][-1])

            self.bounds = channel_infos
            return channel_bounds
        elif self.mode == "stride":
            for channel in range(num_channels):
                channel_infos[channel] = []
                for k in range(channel * self.workers, len(fragments), num_channels * self.workers):
                    for j in range(self.workers):
                        if k + j < len(fragments):
                            channel_infos[channel].append(fragments[k + j])
            self.bounds = channel_infos
            return {}
    
    def get_own_state(self, num_channels):

        channel_bounds = self._get_bounds(num_channels)
        self.channel_bounds = channel_bounds

        assert self.bounds is not None
        channel_infos = {}
        for channel in self.bounds:
            my_files = [k[0] for k in self.bounds[channel]]
            channel_infos[channel] = []
            for pos in range(0, len(my_files), self.workers):
                channel_infos[channel].append( my_files[pos : pos + self.workers])
        
        del self.bounds
        return channel_infos

    def execute(self, mapper_id, files_to_do=None):

        if self.s3 is None:
            self.s3 = S3FileSystem()
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.workers)

        def download(file):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                result = pq.read_table( file, columns=self.columns, filters=self.filters, use_threads= True, filesystem = self.s3)
                if self.name_column is not None:
                    result = result.append_column(self.name_column, pa.array([file.split("/")[-1].replace(".parquet","")] * len(result)))
                return result

        assert self.num_channels is not None

        if files_to_do is None:
            raise Exception("dynamic lineage for inputs not supported anymore")

        if len(files_to_do) == 0:
            return None, None
        
        # we must not return things out of order

        future_to_url = {self.executor.submit(download, files_to_do[i]): i for i in range(len(files_to_do))}
        dfs = []
        for future in concurrent.futures.as_completed(future_to_url):
            dfs.append((future.result(), future_to_url[future]))
        
        sorted_dfs = sorted(dfs, key = lambda x: x[1])
        
        return None, pa.concat_tables([k[0] for k in sorted_dfs])

class InputEC2CoPartitionedSortedParquetDataset:

    def __init__(self, bucket, prefix, partitioner, columns=None, filters=None, name_column = None) -> None:

        self.bucket = bucket
        self.prefix = prefix
        self.partitioner = partitioner
        assert self.prefix is not None

        self.num_channels = None
        self.columns = columns
        self.filters = filters

        self.length = 0
        self.workers = 4

        self.s3 = None
        self.iterator = None
        self.count = 0
        self.bounds = None
        self.name_column = name_column
        if self.name_column is not None and self.columns is not None and self.name_column in self.columns:
            self.columns.remove(self.name_column)

    def get_bounds(self, num_channels, channel_bounds):

        def overlap(a, b):
            return max(-1, min(a[1], b[1]) - max(a[0], b[0]))
        
        channel_infos = {channel: [] for channel in channel_bounds}
        assert len(channel_bounds) == num_channels, "must provide bounds for all the channel"
        self.num_channels = num_channels
        s3fs = S3FileSystem()
        dataset = ds.dataset(self.bucket + "/" + self.prefix, format = "parquet", filesystem=s3fs )
        for fragment in dataset.get_fragments():
            field_index = fragment.physical_schema.get_field_index(self.partitioner)
            metadata = fragment.metadata
            min_timestamp = None
            max_timestamp = None
            for row_group_index in range(metadata.num_row_groups):
                stats = metadata.row_group(row_group_index).column(field_index).statistics
                # Parquet files can be created without statistics
                if stats is None:
                    raise Exception("Copartitioned Parquet files must have statistics!")
                row_group_max = stats.max
                row_group_min = stats.min
                if max_timestamp is None or row_group_max > max_timestamp:
                    max_timestamp = row_group_max
                if min_timestamp is None or row_group_min < min_timestamp:
                    min_timestamp = row_group_min
            assert min_timestamp is not None and max_timestamp is not None 

            # find which channel you belong. This is inclusive interval intersection.
            for channel in channel_bounds:
                if overlap([min_timestamp, max_timestamp], channel_bounds[channel]) >= 0:
                    channel_infos[channel].append((fragment.path, min_timestamp, max_timestamp))
            
        for channel in channel_infos:
            channel_infos[channel] = sorted(channel_infos[channel], key = lambda x : x[1])
            for k in range(1, len(channel_infos[channel])):
                assert overlap([channel_infos[channel][k-1][1], channel_infos[channel][k-1][2]], \
                    [channel_infos[channel][k][1], channel_infos[channel][k][2]]) <= 0, "positive overlap, data is not sorted!"
        
        self.bounds = channel_infos
    
    def get_own_state(self, num_channels):

        assert self.bounds is not None
        channel_infos = {}
        for channel in self.bounds:
            my_files = [k[0] for k in self.bounds[channel]]
            channel_infos[channel] = []
            for pos in range(0, len(my_files), self.workers):
                channel_infos[channel].append( my_files[pos : pos + self.workers])
        
        del self.bounds
        return channel_infos
    
    def execute(self, mapper_id, files_to_do=None):

        if self.s3 is None:
            self.s3 = S3FileSystem()
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.workers)

        def download(file):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                result = pq.read_table( file, columns=self.columns, filters=self.filters, use_threads= True, filesystem = self.s3)
                if self.name_column is not None:
                    result = result.append_column(self.name_column, pa.array([file.split("/")[-1].replace(".parquet","")] * len(result)))

                return result
            
        assert self.num_channels is not None

        if files_to_do is None:
            raise Exception("dynamic lineage for inputs not supported anymore")

        if len(files_to_do) == 0:
            return None, None
        
        # this will return things out of order, but that's ok!

        future_to_url = {self.executor.submit(download, file): file for file in files_to_do}
        dfs = []
        for future in concurrent.futures.as_completed(future_to_url):
            dfs.append(future.result())
        
        return None, pa.concat_tables(dfs)