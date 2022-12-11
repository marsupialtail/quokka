import pickle
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow.dataset as ds
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

class InputEC2ParquetDataset:

    # filter pushdown could be profitable in the future, especially when you can skip entire Parquet files
    # but when you can't it seems like you still read in the entire thing anyways
    # might as well do the filtering at the Pandas step. Also you need to map filters to the DNF form of tuples, which could be
    # an interesting project in itself. Time for an intern?

    def __init__(self, bucket, prefix, columns=None, filters=None) -> None:

        self.bucket = bucket
        self.prefix = prefix
        assert self.prefix is not None

        self.num_channels = None
        self.columns = columns
        self.filters = filters

        self.length = 0
        self.workers = 8

        self.s3 = None
        self.iterator = None
        self.count = 0

    def get_own_state(self, num_channels):
        self.num_channels = num_channels
        s3 = boto3.client('s3')
        z = s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)
        self.files = [i['Key'] for i in z['Contents'] if i['Key'].endswith(".parquet")]
        self.length += sum([i['Size'] for i in z['Contents'] if i['Key'].endswith(".parquet")])
        while 'NextContinuationToken' in z.keys():
            z = self.s3.list_objects_v2(
                Bucket=self.bucket, Prefix=self.prefix, ContinuationToken=z['NextContinuationToken'])
            self.files.extend([i['Key'] for i in z['Contents']
                              if i['Key'].endswith(".parquet")])
            self.length += sum([i['Size'] for i in z['Contents']
                              if i['Key'].endswith(".parquet")])
        channel_infos = {}
        for channel in range(num_channels):
            my_files = [self.files[k] for k in range(channel, len(self.files), self.num_channels)]
            channel_infos[channel] = []
            for pos in range(0, len(my_files), self.workers):
                channel_infos[channel].append(my_files[pos : pos + self.workers])
        return channel_infos


    def execute(self, mapper_id, files_to_do=None):

        if self.s3 is None:
            self.s3 = S3FileSystem()
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.workers)

        def download(file):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return polars.from_arrow(pq.read_table(self.bucket + "/" +
                              file, columns=self.columns, filters=self.filters, use_threads= False, use_legacy_dataset = True, filesystem = self.s3))

        assert self.num_channels is not None

        if files_to_do is None:
            raise Exception("dynamic lienage not supported anymore")

        if len(files_to_do) == 0:
            return None, None
        
        # this will return things out of order, but that's ok!

        future_to_url = {self.executor.submit(download, file): file for file in files_to_do}
        dfs = []
        for future in concurrent.futures.as_completed(future_to_url):
            dfs.append(future.result())
        
        return None, polars.concat(dfs)
        

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

    def get_own_state(self, num_channels):

        self.parquet_file = pq.ParquetFile(self.filename)
        self.num_row_groups = self.parquet_file.num_row_groups
        self.num_channels = num_channels

    def get_next_batch(self, mapper_id, pos=None):
        assert self.num_channels is not None
        if pos is None:
            curr_row_group = mapper_id
        else:
            curr_row_group = pos

        a = self.parquet_file.read_row_group(
            curr_row_group, columns=self.columns)
        if self.filters is not None:
            a = a.filter(self.filters)
        curr_row_group += self.num_channels
        
        if curr_row_group < len(self.num_row_groups):
            return curr_row_group, a
        else:
            return None, a


# class InputParquetDataset:
#     def __init__(self, filepath, mode = "local", columns = None, filters = None) -> None:
        
#         self.filepath = filepath
#         self.columns = columns
#         if filters is not None:
#             if type(filters) == list:
#                 self.filters = filters_to_expression(filters)
#             elif type(filters) == ds.Expression:
#                 self.filters = filters
#             else:
#                 raise Exception("cannot understand filters format.")
#         else:
#             self.filters = None
#         self.mode = mode
#         self.num_channels = None

#     def get_own_state(self, num_channels):

#         self.num_channels = num_channels
#         if self.mode == "s3":
#             s3 = S3FileSystem()
#             dataset = ds.dataset(self.filepath, filesystem = s3)
#         else:
#             dataset = ds.dataset(self.filepath)

#         self.schema = dataset.schema
#         total_rows = dataset.count_rows()
#         print("Parquet dataset at ", self.filepath, " has total ", total_rows, " rows")
#         row_group_fragments = [fragment.split_by_row_group() for fragment in dataset.get_fragments()]
#         row_group_fragments_with_size = [(item.count_rows(), item) for sublist in row_group_fragments for item in sublist]
#         row_group_fragments_with_size.sort(key = lambda x: x[0])

#         self.channel_assigments = {i: [] for i in range(num_channels)}
#         channel_lengths = np.array([0] * num_channels)

#         '''
#         Hey we encounter the Partition Problem! We would like to evenly divide the row groups based on length.
#         We will use Greedy number partitioning. The easiest to implement approximate algorithm.
#         '''
        
#         for size, fragment in row_group_fragments_with_size:
#             channel = np.argmin(channel_lengths)
#             self.channel_assigments[channel].append(fragment)
#             channel_lengths[channel] += size

#     def get_next_batch(self, mapper_id, pos=None):
#         assert self.num_channels is not None
#         if pos is None:
#             pos = 0

#         format = ParquetFileFormat()
#         filesystem = S3FileSystem() if self.mode == "s3" else LocalFileSystem()
#         # fragments = [
#         #     format.make_fragment(
#         #         file,
#         #         filesystem=filesystem,
#         #         partition_expression=part_expression,
#         #     )
#         #     for file, part_expression in self.channel_assigments[mapper_id]
#         # ]

#         if mapper_id not in self.channel_assigments:
#             yield None, None
#         else:
#             self.dataset = FileSystemDataset(self.channel_assigments[mapper_id][pos:], self.schema, format , filesystem)
#             for batch in self.dataset.to_batches(filter= self.filters,columns=self.columns ):
#                 pos += 1
#                 yield pos, batch

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
            a = polars.from_dict({"filename" : [self.files[curr_pos]], "object": [s3.get_object(Bucket=self.bucket, Key=self.files[curr_pos])['Body'].read()]})
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
            a = polars.from_dict({"filename" : [self.files[curr_pos]], "object": [open(self.directory + "/" + self.files[curr_pos],"rb").read()]})
            curr_pos += self.num_channels
            yield curr_pos, a
       
# this should work for 1 CSV up to multiple
class InputDiskCSVDataset:
    def __init__(self, filepath , names = None , sep=",", stride=16 * 1024 * 1024, header = False, window = 1024 * 4, columns = None) -> None:
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
        #self.sample = None
    
    # we need to rethink this whole setting num channels business. For this operator we don't want each node to do redundant work!
    def get_own_state(self, num_channels):
        
        #samples = []
        print("Initializing Disk CSV dataset. This is currently done locally and serially, which might take a while.")
        self.num_channels = num_channels

        '''
        Filepath can be either a single file or a directory. We need to figure out what is the case, and get our files and sizes list.
        '''

        if os.path.isfile(self.filepath):
            files = deque([self.filepath])
            sizes = deque([os.path.getsize(self.filepath)])
        else:
            assert os.path.isdir(self.filepath), "Does not support prefix, must give absolute directory path for a list of files, will read everything in there!"
            files = deque([self.filepath + "/" + file for file in os.listdir(self.filepath)])
            sizes = deque([os.path.getsize(file) for file in files])

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
                    print("Warning, detected column names from header row not the same as supplied column names!")
                    print("Detected", detected_names)
                    print("Supplied", self.names)

        total_size = sum(sizes)
        size_per_channel = total_size // num_channels
        channel_infos = {}

        start_byte = 0
        real_off = 0

        for channel in range(num_channels):
            my_file = []
            curr_size = 0
            done = False
            while curr_size + sizes[0] <= size_per_channel:
                my_file.append(files.popleft())
                end_byte = sizes.popleft()
                if len(sizes) == 0:
                    done = True
                    break # we are taking off a bit more work each time so the last channel might leave before filling up size per channel
                curr_size += end_byte
                real_off = 0
        
            if done:
                channel_infos[channel] = (start_byte, my_file.copy(), real_off + end_byte)
                break

            #curr_size + size[0] > size_per_channel, the leftmost file has enough bytes to satisfy the channel
            # this never gonna happen in real life
            if curr_size == size_per_channel:
                channel_infos[channel] = (start_byte, my_file.copy(), real_off + end_byte)
                continue

            my_file.append(files[0])
            candidate = size_per_channel - curr_size

            start = max(0, candidate - self.window)
            end = min(candidate + self.window, sizes[0])

            f = open(files[0],"rb")
            f.seek(real_off + start)
            resp = f.read(end - start)
            
            first_newline = resp.find(bytes('\n', 'utf-8'))
            last_newline = resp.rfind(bytes('\n', 'utf-8'))
            if last_newline == -1:
                raise Exception
            else:
                bytes_to_take = start + last_newline + 1
                #samples.append(csv.read_csv(BytesIO(resp[first_newline: last_newline]), read_options=csv.ReadOptions(
                #    column_names=self.names), parse_options=csv.ParseOptions(delimiter=self.sep)))

            sizes[0] -= bytes_to_take
            end_byte = real_off + bytes_to_take
            real_off += bytes_to_take
            channel_infos[channel] = (start_byte, my_file.copy(), end_byte)
            start_byte = real_off
        
        self.length = total_size
        #self.sample = pa.concat_tables(samples)
        self.channel_infos = channel_infos
        print("initialized CSV reading strategy for ", total_size // 1024 // 1024 // 1024, " GB of CSV")


    def execute(self, mapper_id, state = None):
        assert self.num_channels is not None

        if mapper_id not in self.channel_infos:
            return None, None
        else:
            files = self.channel_infos[mapper_id][1]
            
            if state is None:
                curr_pos = 0
                pos = self.channel_infos[mapper_id][0]
            else:
                curr_pos, pos = state
                
                
            file = files[curr_pos]
            if curr_pos != len(files) - 1:
                end = os.path.getsize(file)
            else:
                end = self.channel_infos[mapper_id][2]

            f = open(file, "rb")   
            assert pos < end
            f.seek(pos)         

            bytes_to_read = min(pos+self.stride, end) - pos
            resp = f.read(bytes_to_read)
            
            #print(pos, bytes_to_read)
            
            last_newline = resp.rfind(bytes('\n', 'utf-8'))

            if last_newline == -1:
                raise Exception
            else:
                resp = resp[:last_newline]

                if self.header and pos == 0:
                    first_newline = resp.find(bytes('\n','utf-8'))
                    if first_newline == -1:
                        raise Exception
                    resp = resp[first_newline + 1:]

                bump = csv.read_csv(BytesIO(resp), read_options=csv.ReadOptions(
                    column_names=self.names), parse_options=csv.ParseOptions(delimiter=self.sep))
                bump = bump.select(self.columns) if self.columns is not None else bump
                
                # if random.random() > 0.9 and redis.Redis('localhost',port=6800).get("input_already_failed") is None:
                #     redis.Redis('localhost',port=6800).set("input_already_failed", 1)
                #     ray.actor.exit_actor()

                pos += last_newline + 1
                if pos >= end:
                    curr_pos += 1
                    pos = 0

                if curr_pos < len(files):                
                    return (curr_pos, pos) , polars.from_arrow(bump)
                else:
                    return None, polars.from_arrow(bump)


class FakeFile:
    def __init__(self, buffers, last_newline, prefix, end_file):
        self.prefix = prefix
        self.buffers = buffers
        self.closed = False
        self.which_file = 0
        self.file_cursor = 0
        self.last_newline = last_newline 
        self.end = len(buffers[0]) if end_file != 0 else last_newline
        self.is_first_read = True
        self.end_file = end_file

    def read(self, length):
        if self.file_cursor + length < self.end:
            if self.is_first_read:
                self.file_cursor += length - len(self.prefix)
                self.is_first_read = False
                buf = self.prefix + self.buffers[0][:self.file_cursor]
                #print(self.prefix)
                #print(buf[:100])
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
    def __init__(self, bucket, names = None, prefix = None, key = None, sep=",", stride=2e8, header = False, window = 1024 * 4, columns = None) -> None:
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
    
    # we need to rethink this whole setting num channels business. For this operator we don't want each node to do redundant work!
    def get_own_state(self, num_channels):
        
        samples = []
        print("intializing CSV reading strategy. This is currently done locally, which might take a while.")
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
                    print("Warning, detected column names from header row not the same as supplied column names!")
                    print("Detected", detected_names)
                    print("Supplied", self.names)

        total_size = sum(sizes)
        assert total_size > 0
        size_per_partition = min(int(self.stride * self.workers), math.ceil(total_size / num_channels))
        # size_per_partition = int(self.stride * workers)
        print(size_per_partition)

        partitions = {}
        curr_partition_num = 0

        for curr_file, curr_size in zip(files, sizes):
            num_partitions = math.ceil(curr_size / size_per_partition)
            for i in range(num_partitions):
                partitions[curr_partition_num + i] = (curr_file, i * size_per_partition)
            curr_partition_num += num_partitions
        
        @ray.remote(num_cpus=0.001)
        def download_range(bucket, file, start_byte, end_byte):
            s3 = boto3.client('s3')
            resp = s3.get_object(Bucket=bucket, Key=file, Range='bytes={}-{}'.format(start_byte, end_byte))['Body'].read()
            last_newline = resp.rfind(b'\n')
            return resp[last_newline + 1:]

        # refinement
        start = time.time()
        for partition in partitions:
            curr_file, start_byte = partitions[partition]
            if start_byte == 0:
                partitions[partition] = (curr_file, start_byte, b'')
            else:
                prefix_fut = download_range.remote(self.bucket, curr_file, start_byte - self.window, start_byte - 1)
                partitions[partition] = (curr_file, start_byte, prefix_fut)
        print("DISPATCH TIME", time.time() - start)
        start = time.time()
        for partition in partitions:
            curr_file, start_byte, fut = partitions[partition]
            if fut == b'':
                partitions[partition] = (curr_file, start_byte, b'', size_per_partition)
            else:
                partitions[partition] = (curr_file, start_byte, ray.get(fut), size_per_partition)
        print("GATHER TIME", time.time() - start)
        #assign partitions
        print(curr_partition_num)
        partitions_per_channel = math.ceil(curr_partition_num / num_channels) 
        channel_info = {}
        for channel in range(num_channels):
            channel_info[channel] = [partitions[channel * partitions_per_channel + i] for i in range(partitions_per_channel)\
                if (channel * partitions_per_channel + i) in partitions]

        self.file_sizes = {files[i] : sizes[i] for i in range(len(files))}
        print("initialized CSV reading strategy for ", total_size // 1024 // 1024 // 1024, " GB of CSV")
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

        fake_file = FakeFile(results, last_newline, prefix, last_file)
        bump = csv.read_csv(fake_file, read_options=csv.ReadOptions(column_names=self.names), parse_options=csv.ParseOptions(delimiter=self.sep))
        del fake_file

        bump = bump.select(self.columns) if self.columns is not None else bump

        return None, polars.from_arrow(bump)