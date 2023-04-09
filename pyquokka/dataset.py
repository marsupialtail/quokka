import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import json
import pyarrow.json as pajson
from io import BytesIO
import boto3
import os
import redis
from collections import deque
import polars
import numpy as np
import gc
from pyarrow.fs import S3FileSystem, LocalFileSystem
from pyarrow.dataset import FileSystemDataset, ParquetFileFormat
from pyquokka.sql_utils import filters_to_expression
import multiprocessing
import concurrent.futures
import time
import warnings
import random
import ray
import math
import asyncio

# computes the overlap of two intervals (a1, a2) and (b1, b2)
def overlap(a, b):
    return max(-1, min(a[1], b[1]) - max(a[0], b[0]))

# this is actually fault tolerant, since the dataframe is pickled and stored in Redis
# this is obviously not efficient in a cloud environment, but the intended use is for local development anyways
class InputPolarsDataset:

    def __init__(self, df):
        self.df = df

    def get_own_state(self, num_channels):
        assert num_channels == 1
        return {0: [0]}

    def execute(self, mapper_id, state=None):
        return None, self.df

# this is the only non fault tolerant operator here. This is because the Ray object store is not fault tolerant and not replayable.
class InputRayDataset:

    # objects_dict is a dictionary of IP address to a list of Ray object references
    def __init__(self, objects_dict):
        self.objects_dict = objects_dict

    def execute(self, mapper_id, state=None):
        # state will be a tuple of IP, index
        try:
            # print("Getting object from {} at index {}.".format(state[0], state[1]), self.objects_dict[state[0]][state[1]])
            result = ray.get(ray.cloudpickle.loads(self.objects_dict[state[0]][state[1]]), timeout=10)
            # print(result)
        except:
            raise Exception("Unable to get object from {} at index {}.".format(state[0], state[1]))
        
        return None, result

# this dataset will generate a sequence of numbers, from 0 to limit. 
class InputRestGetAPIDataset:
    def __init__(self, url, arguments, headers, projection, batch_size = 10000) -> None:
        
        self.url = url
        self.headers = headers
        self.arguments = arguments
        self.projection = projection

        # how many requests each execute will fetch.
        self.batch_size = batch_size

    def get_own_state(self, num_channels):
        channel_info = {}

        # stride the arguments across the channels in chunks of batch_size
        for channel in range(num_channels):
            channel_info[channel] = [(i, i+self.batch_size) for i in range(channel * self.batch_size, len(self.arguments), num_channels*self.batch_size)]
        print(channel_info)
        return channel_info

    def execute(self, channel, state = None):
        import aiohttp
        async def get(url, session):
            try:
                async with session.get(url=self.url + url, headers = self.headers) as response:
                    resp = await response.json()
                    for projection in self.projection:
                        if projection not in resp:
                            resp[projection] = None
                    return resp
            except Exception as e:
                print("Unable to get url {} due to {}.".format(url, e.__class__))

        async def main(urls):
            async with aiohttp.ClientSession() as session:
                ret = await asyncio.gather(*[get(url, session) for url in urls])
            return ret

        results = asyncio.run(main(self.arguments[state[0]: state[1]]))
        # find the first non-None result
        results = [i for i in results if i is not None] 

        return None, polars.from_records(results).select(self.projection)

class InputRestPostAPIDataset:
    def __init__(self, url, arguments, headers, projection, batch_size = 1000) -> None:
        
        self.url = url
        self.headers = headers
        self.arguments = arguments
        self.projection = projection
        self.batch_size = batch_size

    def get_own_state(self, num_channels):
        channel_info = {}

        # stride the arguments across the channels in chunks of batch_size
        for channel in range(num_channels):
            channel_info[channel] = [(i, i+self.batch_size) for i in range(channel * self.batch_size, len(self.arguments), num_channels*self.batch_size)]
        print(channel_info)
        return channel_info

    def execute(self, channel, state = None):
        import aiohttp
        async def get(data, session):
            try:
                async with session.post(url=self.url, data = json.dumps(data), headers = self.headers) as response:
                    resp = await response.json()
                    return polars.from_records(resp['result'])
            except Exception as e:
                print("Unable to post url {} payload {} headers {} due to {}.".format(self.url, json.dumps(data), json.dumps(self.headers), e.__class__))

        async def main(datas):
            async with aiohttp.ClientSession() as session:
                ret = await asyncio.gather(*[get(data, session) for data in datas])
            return ret
        
        results = asyncio.run(main(self.arguments[state[0]: state[1]]))
        # find the first non-None result
        results = polars.concat([i for i in results if i is not None]).select(self.projection)
        return None, results

class InputLanceQVDataset:

    """
    Let's hope this works!
    """

    def __init__(self, uri, vec_column, query_vectors, k, columns = None, filters = None, batch_size = 100) -> None:
        
        assert type(columns) == list, "columns must be a list of strings"
        self.columns = columns
        assert type(filters) == str, "sql predicate supported"
        self.filters = filters
        self.query_vector = None
        self.k = None
        assert type(query_vectors) == np.ndarray
        assert len(query_vectors.shape) == 2, "must supply 2d query_vectors"
        assert type(k) == int
        self.query_vectors = query_vectors
        self.k = k
        self.uri = uri
        self.batch_size = batch_size
        self.vec_column = vec_column
        
    def get_own_state(self, num_channels):

        self.num_channels = num_channels
        vectors_per_channel = len(self.query_vectors) // num_channels + 1
        channel_infos = {}
        for channel in range(num_channels):
            my_query_vectors = self.query_vectors[channel * vectors_per_channel : (channel + 1) * vectors_per_channel]
            channel_infos[channel] = []
            for pos in range(0, len(my_query_vectors), self.batch_size):
                channel_infos[channel].append(my_query_vectors[pos:pos+self.batch_size])
        return channel_infos
    
    def execute(self, mapper_id, query_vec_batch):

        import lance
        dataset = lance.dataset(self.uri)
        return dataset.to_table(columns = self.columns, filters = self.filters, nearest={"column": self.vec_column, "k": self.k, "q": query_vec_batch})

class InputEC2ParquetDataset:

    # filter pushdown could be profitable in the future, especially when you can skip entire Parquet files
    # but when you can't it seems like you still read in the entire thing anyways
    # might as well do the filtering at the Pandas step. Also you need to map filters to the DNF form of tuples, which could be
    # an interesting project in itself. Time for an intern?

    def __init__(self, files = None, columns=None, filters=None) -> None:

        self.files = files

        self.num_channels = None
        self.columns = columns
        self.filters = filters

        self.length = 0
        self.workers = 4

        self.s3 = None
        self.iterator = None
        self.count = 0

    def get_own_state(self, num_channels):
        self.num_channels = num_channels
        self.files = [i.replace("s3://", "") for i in self.files]
        channel_infos = {}
        for channel in range(num_channels):
            my_files = [self.files[k] for k in range(channel, len(self.files), self.num_channels)]
            channel_infos[channel] = []
            for pos in range(0, len(my_files), self.workers):
                channel_infos[channel].append( my_files[pos : pos + self.workers])
        return channel_infos


    def execute(self, mapper_id, files_to_do=None):

        if self.s3 is None:
            self.s3 = S3FileSystem()
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.workers)

        def download(file):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                if self.columns is not None:
                    return pq.read_table( file, columns=self.columns, filters=self.filters, use_threads= True, filesystem = self.s3)
                else:
                    return pq.read_table( file, filters=self.filters, use_threads= True, filesystem = self.s3)

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


class InputSortedEC2ParquetDataset:

    def __init__(self, files, partitioner, columns=None, filters=None, mode = "stride") -> None:

        self.files = files
        self.partitioner = partitioner
        assert self.prefix is not None

        self.num_channels = None
        self.columns = columns
        self.filters = filters

        self.length = 0
        self.workers = 1

        self.s3 = None
        self.iterator = None
        self.count = 0
        self.bounds = None

        assert mode in ["stride", "range"]
        self.mode = mode

    def _get_bounds(self, num_channels):
        
        channel_infos = {}
        fragments = []
        self.num_channels = num_channels
        s3fs = S3FileSystem()
        dataset = pq.ParquetDataset(self.files, filesystem=s3fs )
        for fragment in dataset.fragments:
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
            fragments.append((fragment.path, min_timestamp, max_timestamp))
            
        fragments = sorted(fragments, key = lambda x: x[1])
        for k in range(1, len(fragments)):
            assert overlap([fragments[k-1][1], fragments[k-1][2]], [fragments[k][1], fragments[k][2]]) <= 0, \
                "positive overlap, data is not sorted!"

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
                return pq.read_table(file, columns=self.columns, filters=self.filters, use_threads= False, use_legacy_dataset = True, filesystem = self.s3)

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

    def __init__(self, bucket, prefix, partitioner, columns=None, filters=None) -> None:

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

    def get_bounds(self, num_channels, channel_bounds):

        def overlap(a, b):
            return max(-1, min(a[1], b[1]) - max(a[0], b[0]))
        
        channel_infos = {channel: [] for channel in channel_bounds}
        assert len(channel_bounds) == num_channels, "must provide bounds for all the channel"
        self.num_channels = num_channels
        s3fs = S3FileSystem()
        dataset = pq.ParquetDataset(self.bucket + "/" + self.prefix, filesystem=s3fs )
        for fragment in dataset.fragments:
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
                return pq.read_table(file, columns=self.columns, filters=self.filters, use_threads= False, use_legacy_dataset = True, filesystem = self.s3)

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

class InputParquetDataset:

    def __init__(self, filename, columns=None, filters = None) -> None:

        self.filename = filename
        self.num_channels = None
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
        
        self.parquet_file = None

    def get_own_state(self, num_channels):
        
        return {0 : [self.filename]}

    def execute(self, mapper_id, filename = None):
        
        dataset = ds.dataset(filename)
        return None, dataset.to_table(filter= self.filters,columns=self.columns )

# this works for a directoy of objects.
class InputS3FilesDataset:

    def __init__(self, bucket, prefix= None) -> None:
        
        self.bucket = bucket
        self.prefix = prefix
        
        self.num_channels = None

    def get_own_state(self, num_channels):
        self.num_channels = num_channels
        s3 = boto3.client('s3')
        if self.prefix is not None:
            z = s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)
            self.files = [i['Key'] for i in z['Contents']]
            while 'NextContinuationToken' in z.keys():
                z = s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix, ContinuationToken=z['NextContinuationToken'])
                self.files.extend([i['Key'] for i in z['Contents']])
        else:
            z = s3.list_objects_v2(Bucket=self.bucket)
            self.files = [i['Key'] for i in z['Contents']]
            while 'NextContinuationToken' in z.keys():
                z = s3.list_objects_v2(Bucket=self.bucket, ContinuationToken=z['NextContinuationToken'])
                self.files.extend([i['Key'] for i in z['Contents']])

    def get_next_batch(self, mapper_id, pos=None):
        assert self.num_channels is not None
        if pos is None:
            curr_pos = mapper_id
        else:
            curr_pos = pos
        s3 = boto3.client('s3')
        while curr_pos < len(self.files):
            #print("input batch", (curr_pos - mapper_id) / self.num_channels)
            # since these are arbitrary byte files (most likely some image format), it is probably useful to keep the filename around or you can't tell these things apart
            a = pa.Table.from_pydict({"filename" : [self.files[curr_pos]], "object": [s3.get_object(Bucket=self.bucket, Key=self.files[curr_pos])['Body'].read()]})
            #print("ending reading ",time.time())
            curr_pos += self.num_channels
            yield curr_pos, a

# this works for a directoy of objects on disk.
# Could have combined this with the S3FilesDataset but the code is so different might as well
class InputDiskFilesDataset:

    def __init__(self, directory) -> None:
        
        self.directory = directory
        assert os.path.isdir(directory), "must supply directory, try absolute path"
        
        self.num_channels = None

    def get_own_state(self, num_channels):
        self.num_channels = num_channels
        self.files = os.listdir(self.directory)

    def get_next_batch(self, mapper_id, pos=None):
        assert self.num_channels is not None
        if pos is None:
            curr_pos = mapper_id
        else:
            curr_pos = pos
        while curr_pos < len(self.files):
            a = pa.Table.from_pydict({"filename" : [self.files[curr_pos]], "object": [open(self.directory + "/" + self.files[curr_pos],"rb").read()]})
            curr_pos += self.num_channels
            yield curr_pos, a
       
# this should work for 1 CSV up to multiple
class InputDiskCSVDataset:
    def __init__(self, filepath , names = None , sep=",", stride=16 * 1024 * 1024, header = False, window = 1024 * 4, columns = None, sort_info = None) -> None:
        self.filepath = filepath

        self.num_channels = None
        self.names = names
        self.sep = sep
        self.stride = stride
        self.header = header
        self.columns = columns
        
        self.window = window
        assert not (names is None and header is False), "if header is False, must supply column names"

        self.length = 0
        self.file_sizes = None
        if sort_info is not None:
            self.sort_key = sort_info[0]
            self.sort_mode = sort_info[1]
            assert self.sort_mode in {"stride", "range"}
        else:
            self.sort_key = None
            self.sort_mode = None
        #self.sample = None
    

    def get_own_state(self, num_channels):
        
        # figure out what you have to read
        if os.path.isfile(self.filepath):
            files = deque([self.filepath])
            sizes = deque([os.path.getsize(self.filepath)])
        else:
            assert os.path.isdir(self.filepath), "Does not support prefix, must give absolute directory path for a list of files, will read everything in there!"
            files = deque([self.filepath + "/" + file for file in os.listdir(self.filepath)])
            sizes = deque([os.path.getsize(file) for file in files])

        # if there is a header row then we need to read the first line to get the column names
        # if the user supplied column names, we should check that they match
        if self.header:
            resp = open(files[0],"r").read(self.window)
            first_newline = resp.find("\n")
            if first_newline == -1:
                raise Exception("could not detect the first line break. try setting the window argument to a large number")
            if self.names is None:
                self.names = resp[:first_newline].split(self.sep)
            else:
                detected_names = resp[:first_newline].split(self.sep)
                if self.names != detected_names:
                    print("Warning, detected column names from header row not the same as supplied column names! This could also be because your rows end with the delimiter.")
                    print("Detected", detected_names)
                    print("Supplied", self.names)
        
        # if the sort info is not None, we should reorder the files based on the sort key
        if self.sort_key is not None:
            file_stats = []
            for file, size in zip(files, sizes):
                # for each file, read the first line and the last line to figure out what is the min and max of the sort key
                # then we can sort the files based on the min and max of the sort key
                resp = open(file, "rb").read(self.window)
                if self.header:
                    first_newline = resp.find(bytes('\n','utf-8'))
                    resp = resp[first_newline + 1:]
                first_newline = resp.find(bytes('\n','utf-8'))
                resp = resp[:first_newline]
                min_key = csv.read_csv(BytesIO(resp), read_options=csv.ReadOptions(
                    column_names=self.names), parse_options=csv.ParseOptions(delimiter=self.sep))[self.sort_key][0].as_py()
                f = open(file, "rb")
                f.seek(size - self.window)
                resp = f.read(self.window)
                first_newline = resp.find(bytes('\n','utf-8'))
                resp = resp[first_newline + 1:]
                max_key = csv.read_csv(BytesIO(resp), read_options=csv.ReadOptions(
                    column_names=self.names), parse_options=csv.ParseOptions(delimiter=self.sep))[self.sort_key][-1].as_py()
                file_stats.append((file, min_key, max_key, size))
            
            file_stats = sorted(file_stats, key=lambda x: x[1])
            for i in range(1, len(file_stats)):
                assert overlap([file_stats[i-1][1], file_stats[i-1][2]], [file_stats[i][1], file_stats[i][2]]) <= 0, "data is not sorted!"
            files = deque([file[0] for file in file_stats])
            sizes = deque([file[3] for file in file_stats])

        total_size = sum(sizes)
        assert total_size > 0
        size_per_partition = min(int(self.stride), math.ceil(total_size / num_channels))
        self.window = min(self.window, size_per_partition)
        # size_per_partition = int(self.stride * workers)
        # print(size_per_partition)

        partitions = {}
        curr_partition_num = 0

        for curr_file, curr_size in zip(files, sizes):
            num_partitions = math.ceil(curr_size / size_per_partition)
            for i in range(num_partitions):
                partitions[curr_partition_num + i] = (curr_file, i * size_per_partition)
            curr_partition_num += num_partitions

        # refinement
        start = time.time()
        for partition in partitions:
            curr_file, start_byte = partitions[partition]
            if start_byte == 0:
                partitions[partition] = (curr_file, start_byte, b'', size_per_partition)
            else:
                f = open(curr_file, 'rb')
                f.seek(start_byte - self.window)
                window = f.read(self.window)
                pos = window.rfind(b'\n')
                prefix = window[pos + 1:]
                partitions[partition] = (curr_file, start_byte, prefix, size_per_partition)

        #assign partitions
        # print(curr_partition_num)
        
        channel_info = {}
        if self.sort_key is None:
            # if there is no sort key we should assign partitions in a contiguous manner
            # for cache friendliness
            partitions_per_channel = math.ceil(curr_partition_num / num_channels) 
            for channel in range(num_channels):
                channel_info[channel] = [partitions[channel * partitions_per_channel + i] for i in range(partitions_per_channel)\
                    if (channel * partitions_per_channel + i) in partitions]
        else:
            # if there is a sort key, we should assign partitions based on the sort key
            # we have to round robin the partitions
            for channel in range(num_channels):
                channel_info[channel] = [partitions[i] for i in range(channel, len(partitions), num_channels)\
                    if i in partitions]

        self.file_sizes = {files[i] : sizes[i] for i in range(len(files))}
        return channel_info
    
    def execute(self, mapper_id, state = None):
        assert self.file_sizes is not None
        assert state is not None, "dynamic lineage for inputs deprecated"

        file, start_byte, prefix, size_per_partition = state
        end = self.file_sizes[file]

        f = open(file, "rb")   
        assert start_byte < end
        f.seek(start_byte)         

        bytes_to_read = min(min(start_byte + self.stride, end) - start_byte, size_per_partition)
        resp = f.read(bytes_to_read)
        
        #print(pos, bytes_to_read)
        
        last_newline = resp.rfind(bytes('\n', 'utf-8'))
        

        if last_newline == -1:
            raise Exception
        else:
            resp = prefix + resp[: last_newline]

            if self.header and start_byte == 0:
                first_newline = resp.find(bytes('\n','utf-8'))
                if first_newline == -1:
                    raise Exception
                resp = resp[first_newline + 1:]

            # bump = csv.read_csv(BytesIO(resp), read_options=csv.ReadOptions(
            #     column_names=self.names), parse_options=csv.ParseOptions(delimiter=self.sep))
            bump = polars.read_csv(resp, new_columns = self.names, sep = self.sep, has_header = False, try_parse_dates=True)
            
            bump = bump.select(self.columns) if self.columns is not None else bump

            return None, bump


class InputDiskJSONDataset:
    def __init__(self, filepath , names = None , stride=16 * 1024 * 1024, window = 1024 * 4, keys = None, sort_info = None, schema = None) -> None:
        self.filepath = filepath

        self.num_channels = None
        self.names = names
        self.stride = stride
        self.keys = keys
        
        self.window = window
        
        self.length = 0
        self.file_sizes = None

        # Schema needs to be be a pyarrow.schema object
        if schema is not None:
            self.parse_options = pajson.ParseOptions(pajson.ParseOptions(explicit_schema = schema, newlines_in_values = False))
        else:
            self.parse_options = pajson.ParseOptions(pajson.ParseOptions(newlines_in_values = False))


        if sort_info is not None:
            self.sort_key = sort_info[0]
            self.sort_mode = sort_info[1]
            assert self.sort_mode in {"stride", "range"}
        else:
            self.sort_key = None
            self.sort_mode = None
    

    def get_own_state(self, num_channels):
        
        # get files to read and their sizes
        if os.path.isfile(self.filepath):
            files = deque([self.filepath])
            sizes = deque([os.path.getsize(self.filepath)])
        else:
            assert os.path.isdir(self.filepath), "Does not support prefix, must give absolute directory path for a list of files, will read everything in there!"
            files = deque([self.filepath + "/" + file for file in os.listdir(self.filepath)])
            sizes = deque([os.path.getsize(file) for file in files])
        
        # if the sort info is not None, we should reorder the files based on the sort key
        if self.sort_key is not None:
            file_stats = []
            for file, size in zip(files, sizes):
                # for each file, read the first line and the last line to figure out what is the min and max of the sort key
                # then we can sort the files based on the min and max of the sort key

                # Get min key
                resp = open(file, "rb").read(self.window)

                first_newline = resp.find(bytes('\n','utf-8'))
                resp = resp[:first_newline]
                min_key = pajson.read_json(BytesIO(resp), read_options=pajson.ReadOptions(
                    use_threads=True), parse_options=self.parse_options)[self.sort_key][0].as_py()
                
                f = open(file, "rb")
                f.seek(size - self.window)
                resp = f.read(self.window)
                first_newline = resp.find(bytes('\n','utf-8'))
                resp = resp[first_newline + 1:]
                max_key = pajson.read_json(BytesIO(resp), read_options=pajson.ReadOptions(
                    use_threads=True), parse_options=self.parse_options)[self.sort_key][-1].as_py()
                file_stats.append((file, min_key, max_key, size))
            
            file_stats = sorted(file_stats, key=lambda x: x[1])
            for i in range(1, len(file_stats)):
                assert overlap([file_stats[i-1][1], file_stats[i-1][2]], [file_stats[i][1], file_stats[i][2]]) <= 0, "data is not sorted!"
            files = deque([file[0] for file in file_stats])
            sizes = deque([file[3] for file in file_stats])

        total_size = sum(sizes)
        assert total_size > 0
        size_per_partition = min(int(self.stride), math.ceil(total_size / num_channels))
        self.window = min(self.window, size_per_partition)

        partitions = {}
        curr_partition_num = 0

        for curr_file, curr_size in zip(files, sizes):
            num_partitions = math.ceil(curr_size / size_per_partition)
            for i in range(num_partitions):
                partitions[curr_partition_num + i] = (curr_file, i * size_per_partition)
            curr_partition_num += num_partitions

 
        start = time.time()
        for partition in partitions:
            curr_file, start_byte = partitions[partition]
            if start_byte == 0:
                partitions[partition] = (curr_file, start_byte, b'', size_per_partition)
            else:
                f = open(curr_file, 'rb')
                f.seek(start_byte - self.window)
                window = f.read(self.window)
                pos = window.rfind(b'\n')
                prefix = window[pos + 1:]
                partitions[partition] = (curr_file, start_byte, prefix, size_per_partition)
        
        """
        Assign partitions to channels:
        Take generated partitions and distribute them equally among channels
        """
        channel_info = {}
        if self.sort_key is None:
            # if there is no sort key we should assign partitions in a contiguous manner
            # for cache friendliness
            partitions_per_channel = math.ceil(curr_partition_num / num_channels) 
            for channel in range(num_channels):
                channel_info[channel] = [partitions[channel * partitions_per_channel + i] for i in range(partitions_per_channel)\
                    if (channel * partitions_per_channel + i) in partitions]
        else:
            # if there is a sort key, we should assign partitions based on the sort key
            # we have to round robin the partitions
            for channel in range(num_channels):
                channel_info[channel] = [partitions[i] for i in range(channel, len(partitions), num_channels)\
                    if i in partitions]

        self.file_sizes = {files[i] : sizes[i] for i in range(len(files))}
        return channel_info
    
    def execute(self, mapper_id, state = None):
        assert self.file_sizes is not None
        assert state is not None, "dynamic lineage for inputs deprecated"

        file, start_byte, prefix, size_per_partition = state
        end = self.file_sizes[file]

        f = open(file, "rb")   
        assert start_byte < end
        f.seek(start_byte)         

        bytes_to_read = min(min(start_byte + self.stride, end) - start_byte, size_per_partition)
        resp = f.read(bytes_to_read)
        
        last_newline = resp.rfind(bytes('\n', 'utf-8'))
        

        if last_newline == -1:
            raise Exception
        else:
            resp = prefix + resp[: last_newline]

            bump = pajson.read_pajson(BytesIO(resp), read_options=pajson.ReadOptions(
                    use_threads=True), parse_options=self.parse_options)
            bump = bump.select(self.keys) if self.keys is not None else bump

            return None, bump


class FakeFile:
    def __init__(self, buffers, last_newline, prefix, end_file, skip_header = False):
        self.prefix = prefix
        self.buffers = buffers
        self.closed = False
        self.which_file = 0
        self.file_cursor = 0
        self.last_newline = last_newline 
        self.end = len(buffers[0]) if end_file != 0 else last_newline
        self.is_first_read = True
        self.end_file = end_file
        self.skip_header = skip_header

    def read(self, length):
        if self.file_cursor + length < self.end:
            if self.is_first_read:
                self.file_cursor += length - len(self.prefix)
                self.is_first_read = False
                buf = self.prefix + self.buffers[0][:self.file_cursor]
                #print(self.prefix)
                #print(buf[:100])
                if self.skip_header:
                    first_linebreak = buf.find(b'\n')
                    buf = buf[first_linebreak + 1:]
                return buf
            else:
                self.file_cursor += length
                return self.buffers[self.which_file][self.file_cursor - length: self.file_cursor]
        else:

            buf = self.buffers[self.which_file][self.file_cursor : self.end ]

            if self.which_file == self.end_file:
                self.file_cursor = self.last_newline
            else:
                self.file_cursor = self.file_cursor + length - self.end
                self.which_file += 1
                buf += self.buffers[self.which_file][:self.file_cursor]
                if self.which_file == self.end_file:
                    self.end = self.last_newline
                else:
                    self.end = len(self.buffers[self.which_file])
            return buf

    def get_end(self):
        return self.buffers[self.end_file][self.last_newline: len(self.buffers[self.end_file])]
    
    def seek(self):
        raise NotImplementedError

# this should work for 1 CSV up to multiple
# this should work for 1 CSV up to multiple
class InputS3CSVDataset:
    def __init__(self, bucket, names = None, prefix = None, key = None, sep=",", stride=2e8, header = False, window = 1024 * 4, columns = None, sort_info = None) -> None:
        self.bucket = bucket
        self.prefix = prefix
        self.key = key
        self.num_channels = None
        self.names = names
        self.sep = sep
        self.stride = stride
        self.header = header
        self.columns = columns

        self.window = window
        assert not (names is None and header is False), "if header is False, must supply column names"

        self.length = 0
        self.sample = None

        self.workers = 8
        self.s3 = None
        if sort_info is not None:
            self.sort_key = sort_info[0]
            self.sort_mode = sort_info[1]
            assert self.sort_mode in {"stride", "range"}
        else:
            self.sort_key = None
            self.sort_mode = None
    
    # we need to rethink this whole setting num channels business. For this operator we don't want each node to do redundant work!
    def get_own_state(self, num_channels):
        
        samples = []
        # print("intializing CSV reading strategy. This is currently done locally, which might take a while.")
        self.num_channels = num_channels

        s3 = boto3.client('s3')  # needs boto3 client, however it is transient and is not part of own state, so Ray can send this thing! 
        if self.key is not None:
            files = [self.key]
            response = s3.head_object(Bucket=self.bucket, Key=self.key)
            sizes = [response['ContentLength']]
        else:
            if self.prefix is not None:
                z = s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)
                files = z['Contents']
                while 'NextContinuationToken' in z.keys():
                    z = s3.list_objects_v2(
                        Bucket=self.bucket, Prefix=self.prefix, ContinuationToken=z['NextContinuationToken'])
                    files.extend(z['Contents'])
            else:
                z = s3.list_objects_v2(Bucket=self.bucket)
                files = z['Contents']
                while 'NextContinuationToken' in z.keys():
                    z = s3.list_objects_v2(
                        Bucket=self.bucket, ContinuationToken=z['NextContinuationToken'])
                    files.extend(z['Contents'])
            sizes = [i['Size'] for i in files]
            files = [i['Key'] for i in files]

        if self.header:
            resp = s3.get_object(
                Bucket=self.bucket, Key=files[0], Range='bytes={}-{}'.format(0, self.window))['Body'].read()
            first_newline = resp.find(bytes('\n', 'utf-8'))
            if first_newline == -1:
                raise Exception("could not detect the first line break. try setting the window argument to a large number")
            if self.names is None:
                self.names = resp[:first_newline].decode("utf-8").split(",")
            else:
                detected_names = resp[:first_newline].decode("utf-8").split(",")
                if self.names != detected_names:
                    print("Warning, detected column names from header row not the same as supplied column names! This could also be because your rows end with the delimiter.")
                    print("Detected", detected_names)
                    print("Supplied", self.names)

         # if the sort info is not None, we should reorder the files based on the sort key
        if self.sort_key is not None:
            raise NotImplementedError

        total_size = sum(sizes)
        assert total_size > 0
        size_per_partition = min(int(self.stride * self.workers), math.ceil(total_size / num_channels))
        # size_per_partition = int(self.stride * workers)

        partitions = {}
        curr_partition_num = 0

        for curr_file, curr_size in zip(files, sizes):
            num_partitions = math.ceil(curr_size / size_per_partition)
            for i in range(num_partitions):
                partitions[curr_partition_num + i] = (curr_file, i * size_per_partition)
            curr_partition_num += num_partitions
        
        
        @ray.remote
        def download_ranges(inputs):
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
            boto3.client('s3')
            def download_range(bucket, file, start_byte, end_byte):
                s3 = boto3.client('s3')
                resp = s3.get_object(Bucket=bucket, Key=file, Range='bytes={}-{}'.format(start_byte, end_byte))['Body'].read()
                last_newline = resp.rfind(b'\n')
                return resp[last_newline + 1:]
            futures = {}
            for partition in inputs:
                bucket, file, start_byte, end_byte = inputs[partition]
                futures[partition] = executor.submit(download_range, bucket, file, start_byte, end_byte)
            return {partition:futures[partition].result() for partition in inputs}

        # refinement
        start = time.time()
        inputs = {}
        for partition in partitions:
            curr_file, start_byte = partitions[partition]
            if start_byte == 0:
                partitions[partition] = (curr_file, start_byte, b'')
            else:
                inputs[partition] = (self.bucket, curr_file, start_byte - self.window, start_byte - 1)
                partitions[partition] = (curr_file, start_byte, b'1')

        ips = [k for k in ray.available_resources() if 'node' in k]
        prefixes_per_ip = len(inputs) // len(ips) + 1
        prefixes_futs = []
        partition_list = list(inputs.keys())
        for i in range(len(ips)):
            ip = ips[i]
            prefixes_futs.append(download_ranges.options(resources = {ip : 0.001}).\
                remote({partition: inputs[partition] for partition in partition_list[i * prefixes_per_ip : (i + 1) * prefixes_per_ip ]}))

        # print("DISPATCH TIME", time.time() - start)
        start = time.time()
        prefixes = {}
        results = ray.get(prefixes_futs)
        for result in results:
            for key in result:
                prefixes[key] = result[key]

        for partition in partitions:
            curr_file, start_byte, fut = partitions[partition]
            if fut == b'':
                partitions[partition] = (curr_file, start_byte, b'', size_per_partition)
            else:
                partitions[partition] = (curr_file, start_byte, prefixes[partition], size_per_partition)
    
        # print("GATHER TIME", time.time() - start)
        #assign partitions
        # print(curr_partition_num)
        partitions_per_channel = math.ceil(curr_partition_num / num_channels) 
        channel_info = {}
        for channel in range(num_channels):
            channel_info[channel] = [partitions[channel * partitions_per_channel + i] for i in range(partitions_per_channel)\
                if (channel * partitions_per_channel + i) in partitions]

        self.file_sizes = {files[i] : sizes[i] for i in range(len(files))}
        print("initialized CSV reading strategy for ", total_size // 1024 // 1024 // 1024, " GB of CSV on S3")
        return channel_info

    def execute(self, mapper_id, state = None):

        if self.s3 is None:
            self.s3 = boto3.client("s3")
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.workers)

        assert self.file_sizes is not None
        
        if state is None:
            raise Exception("Input lineage is now static.")
        else:
            file, pos, prefix, partition_size = state
                    
        end = self.file_sizes[file]
        
        def download(x):
            
            end_byte = min(pos + x * self.stride + self.stride - 1, end - 1, pos + partition_size - 1)
            start_byte = pos + x * self.stride
            if start_byte > end_byte:
                return None
            return self.s3.get_object(Bucket=self.bucket, Key= file, Range='bytes={}-{}'.format(start_byte, end_byte))['Body'].read()

        future_to_url = {self.executor.submit(download, x): x for x in range(self.workers)}
        results = {}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future] 
            data = future.result()
            results[url] = data

        last_file = self.workers - 1

        for z in range(self.workers):
            if results[z] is None:
                last_file = z -1
                break
        
        if last_file == -1:
            raise Exception("something is wrong, try changing the stride")

        last_newline = results[last_file].rfind(bytes('\n', 'utf-8'))

        skip_header = self.header and pos == 0

        fake_file = FakeFile(results, last_newline, prefix, last_file, skip_header)
        bump = csv.read_csv(fake_file, read_options=csv.ReadOptions(column_names=self.names), parse_options=csv.ParseOptions(delimiter=self.sep))
        # bump = polars.read_csv(fake_file, columns = self.names, sep = self.sep)
        del fake_file

        bump = bump.select(self.columns) if self.columns is not None else bump

        return None, bump
