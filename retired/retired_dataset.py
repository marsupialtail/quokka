class SortPhase2Dataset:

    def __init__(self, channel_files, key, record_batch_rows) -> None:
        self.channel_files = channel_files
        self.record_batch_rows = record_batch_rows
        self.key = key

    def set_num_channels(self, num_channels):
        pass

    def get_next_batch(self, mapper_id, pos = None):
        # let's not support fault tolerance for now.

        if pos is not None:
            raise Exception

        import os, psutil   
        
        sources = self.channel_files[mapper_id]
        print(sources)
        number_of_batches_in_sources = [pa.ipc.open_file(pa.memory_map(source,'rb')).num_record_batches for source in sources]
        next_batch_to_gets = [1 for i in sources]
        
        process = psutil.Process(os.getpid())
        print("mem usage", process.memory_info().rss, pa.total_allocated_bytes())

        cached_batches_in_mem = [polars.from_arrow(pa.Table.from_batches([pa.ipc.open_file(pa.memory_map(source,'rb')).get_batch(0)])) for source in sources]

        while sum([len(i) != 0 for i in cached_batches_in_mem]) > 0:
        
            print("mem usage", process.memory_info().rss,  pa.total_allocated_bytes())

            disk_portions = [batch[:self.record_batch_rows] for batch in cached_batches_in_mem]
            for j in range(len(disk_portions)):
                disk_portions[j]["asdasd"] = np.ones(len(disk_portions[j])) * j
            
            result_idx = polars.concat([portion.select([self.key, "asdasd"]) for portion in disk_portions]).sort(self.key)[:self.record_batch_rows]
            disk_contribs = [(result_idx["asdasd"] == j).sum() for j in range(len(sources))]
            result = polars.concat([disk_portions[j][:disk_contribs[j]] for j in range(len(sources))]).sort(self.key)
            result.drop_in_place("asdasd")

            for j in range(len(cached_batches_in_mem)):
                cached_batches_in_mem[j] = cached_batches_in_mem[j][disk_contribs[j]:]
                
                if len(cached_batches_in_mem[j]) < self.record_batch_rows and next_batch_to_gets[j] < number_of_batches_in_sources[j]:
                    source = pa.ipc.open_file(pa.memory_map(sources[j], 'rb'))
                    next_batch = polars.from_arrow(pa.Table.from_batches([source.get_batch(next_batch_to_gets[j])]))
                    next_batch_to_gets[j] += 1
                    cached_batches_in_mem[j].vstack(next_batch, in_place = True)
                    del next_batch
            
            print(gc.collect())
            yield None, result

# the only difference here is that we move the fragment construction inside get_batches
# this is because on cluster setting we want to do that instead of initializing locally
# on local setting you want to do the reverse! 
class InputEC2ParquetDataset:
    def __init__(self, filepath, mode = "local", columns = None, filters = None) -> None:
        
        self.filepath = filepath
        self.columns = columns
        if filters is not None:
            if type(filters) == list:
                self.filters = filters_to_expression(filters)
            elif type(filters) == ds.Expression:
                self.filters = filters
            else:
                raise Exception("cannot understand filters format.")
        else:
            self.filters = None
        self.mode = mode
        self.num_channels = None
    
    def set_num_channels(self, num_channels):
        assert self.num_channels == num_channels

    def get_own_state(self, num_channels):

        print("Initializing Parquet dataset. This is currently done locally and serially, which might take a while.")

        self.num_channels = num_channels
        if self.mode == "s3":
            s3 = S3FileSystem()
            dataset = ds.dataset(self.filepath, filesystem = s3)
        else:
            dataset = ds.dataset(self.filepath)


        self.schema = dataset.schema
        total_rows = dataset.count_rows()
        print("Parquet dataset at ", self.filepath, " has total ", total_rows, " rows")
        row_group_fragments = [fragment.split_by_row_group() for fragment in dataset.get_fragments()]
        row_group_fragments_with_size = [(item.count_rows(), item) for sublist in row_group_fragments for item in sublist]
        row_group_fragments_with_size.sort(key = lambda x: x[0])

        self.channel_assigments = {i: [] for i in range(num_channels)}
        channel_lengths = np.array([0] * num_channels)

        '''
        Hey we encounter the Partition Problem! We would like to evenly divide the row groups based on length.
        We will use Greedy number partitioning. The easiest to implement approximate algorithm.
        '''
        
        for size, fragment in row_group_fragments_with_size:
            channel = np.argmin(channel_lengths)
            self.channel_assigments[channel].append((fragment.path, fragment.partition_expression))
            channel_lengths[channel] += size

    def get_next_batch(self, mapper_id, pos=None):
        assert self.num_channels is not None
        if pos is None:
            pos = 0
        format = ParquetFileFormat()
        filesystem = S3FileSystem() if self.mode == "s3" else LocalFileSystem()
        fragments = [
            format.make_fragment(
                file,
                filesystem=filesystem,
                partition_expression=part_expression,
            )
            for file, part_expression in self.channel_assigments[mapper_id]
        ]

        self.dataset = FileSystemDataset(fragments[pos:], self.schema, format , filesystem)
        for batch in self.dataset.to_batches(filter= self.filters,columns=self.columns ):
            pos += 1
            yield pos, batch

class InputSingleParquetDataset:

    def __init__(self, filename, columns=None, filters = None) -> None:
        
        self.filename = filename
        self.num_channels = None
        self.columns = columns
        self.filters = filters

    def set_num_channels(self, num_channels):

        self.parquet_file = pq.ParquetFile(self.filename)
        self.num_row_groups = self.parquet_file.num_row_groups
        self.num_channels = num_channels

    def get_next_batch(self, mapper_id, pos=None):
        assert self.num_channels is not None
        if pos is None:
            curr_row_group = mapper_id
        else:
            curr_row_group = pos
        while curr_row_group < len(self.num_row_groups):
            a = self.parquet_file.read_row_group(
                curr_row_group, columns=self.columns)
            curr_row_group += self.num_channels
            yield curr_row_group, a

class InputDiskHDF5Dataset:

    def __init__(self, filename, key) -> None:
        
        self.filename = filename
        self.key = key

        self.num_channels = None
        
    def set_num_channels(self, num_channels):
        self.num_channels = num_channels
        self.h5file = h5py.File(self.filename)
        self.dataset = self.h5file[self.key]
        self.chunk_size = self.dataset.chunks
        self.dataset_shape = self.dataset.shape

        assert self.chunk_size[1] == self.dataset_shape[1]
        self.num_chunks = (self.dataset_shape[0]-1) // self.chunk_size[0] + 1

    def get_next_batch(self, mapper_id, pos=None):
        assert self.num_channels is not None
        if pos is None:
            curr_chunk = mapper_id
        else:
            curr_chunk = pos
        while curr_chunk < self.num_chunks:
            chunk_start = curr_chunk * self.chunk_size[0]            
            result = self.dataset[chunk_start:chunk_start + self.chunk_size[0]]
            curr_chunk += self.num_channels
            yield curr_chunk, result

class InputS3ParquetDataset:

    def __init__(self, bucket, prefix = None, key = None, columns=None, filters=None) -> None:
        
        self.bucket = bucket
        self.prefix = prefix
        self.key = key
        assert (self.key is None and self.prefix is not None) or (self.prefix is None and self.key is not None)
        
        self.num_channels = None
        self.columns = columns
        self.filters = filters

        self.length = 0

    def set_num_channels(self, num_channels):
        assert self.num_channels == num_channels
        self.s3 = boto3.client('s3')

    def get_own_state(self, num_channels):
        self.num_channels = num_channels
        s3 = boto3.client('s3')
        z = s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)
        self.files = [i['Key'] for i in z['Contents'] if i['Key'].endswith(".parquet")]

        assert len(self.files) > 0

        self.length += sum([i['Size'] for i in z['Contents'] if i['Key'].endswith(".parquet")])
        while 'NextContinuationToken' in z.keys():
            z = self.s3.list_objects_v2(
                Bucket=self.bucket, Prefix=self.prefix, ContinuationToken=z['NextContinuationToken'])
            self.files.extend([i['Key'] for i in z['Contents']
                              if i['Key'].endswith(".parquet")])
            self.length += sum([i['Size'] for i in z['Contents']
                              if i['Key'].endswith(".parquet")])
        

    def get_next_batch(self, mapper_id, pos=None):
        assert self.num_channels is not None
        if pos is None:
            start_pos = mapper_id
        else:
            start_pos = pos
        for curr_pos in range(start_pos, len(self.files), self.num_channels):
            #print("input batch", (curr_pos - mapper_id) / self.num_channels)
            #print("starting reading ",time.time())
            #a = pq.read_table("s3://" + self.bucket + "/" + self.files[curr_pos],columns=self.columns, filters = self.filters).to_pandas()
            a = pq.read_table("s3://" + self.bucket + "/" +
                              self.files[curr_pos], columns=self.columns, filters=self.filters)
            #print("ending reading ",time.time())
            curr_pos += self.num_channels
            yield curr_pos, a

# use this if you have a lot of small parquet files

# I love this
# currently only support chunking in one dimension. two-D chunking actually requires different algos
class InputHDF5Dataset:

    def __init__(self, bucket, filename, key) -> None:

        self.bucket = bucket
        self.filename = filename
        self.key = key

        self.num_channels = None

    def set_num_channels(self, num_channels):
        import h5py
        self.num_channels = num_channels
        s3 = s3fs.S3FileSystem()
        self.h5file = h5py.File(s3.open("s3://" + self.bucket + "/" + self.filename, "rb"))
        self.dataset = self.h5file[self.key]
        self.chunk_size = self.dataset.chunks
        self.dataset_shape = self.dataset.shape

        assert self.chunk_size[1] == self.dataset_shape[1]
        self.num_chunks = (self.dataset_shape[0]-1) // self.chunk_size[0] + 1

    def get_next_batch(self, mapper_id, pos=None):
        assert self.num_channels is not None
        if pos is None:
            curr_chunk = mapper_id
        else:
            curr_chunk = pos
        while curr_chunk < self.num_chunks:
            chunk_start = curr_chunk * self.chunk_size[0]
            result = self.dataset[chunk_start:chunk_start + self.chunk_size[0]]
            curr_chunk += self.num_channels
            yield curr_chunk, result
