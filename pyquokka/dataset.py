import pickle
import pyarrow.csv as csv
import pyarrow.parquet as pq
from io import BytesIO, StringIO
import boto3
import s3fs
import os
import redis
#import h5py
from collections import deque
import pyarrow as pa
import polars
import numpy as np
import gc

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


class RedisObjectsDataset:

    # expects objects as a dict of channel : list of tuples of (ip, key, size)
    def __init__(self, channel_objects, ip_set) -> None:
        self.channel_objects = channel_objects
        self.rs = {}
        self.ip_set = ip_set
        for ip in self.ip_set:
            self.rs[ip] = redis.Redis(host=ip, port=6800, db=0)

    def get_next_batch(self, mapper_id, pos=None):
        if mapper_id not in self.channel_objects:
            raise Exception(
                "ERROR: I dont know about where this channel is. Autoscaling here not supported yet. Will it ever be?")

        total_objects = len(self.channel_objects[mapper_id])

        if pos is None:
            pos = 0

        while pos < total_objects:
            object = self.channel_objects[mapper_id][pos]
            bump = self.rs[object[0]].get(object[1])
            pos += 1
            yield pos, pickle.loads(bump)


class InputSingleParquetDataset:

    def __init__(self, bucket, filename, columns=None) -> None:
        
        self.bucket = bucket
        self.filename = filename
        self.num_channels = None
        self.columns = columns

    def set_num_channels(self, num_channels):

        s3 = s3fs.S3FileSystem()
        self.parquet_file = pq.ParquetFile(s3.open(self.bucket + self.filename, "rb"))
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

class InputMultiParquetDataset:

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

    def set_num_channels(self, num_channels):
        self.num_channels = num_channels
        self.s3 = boto3.client('s3')
        z = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)
        self.files = [i['Key'] for i in z['Contents'] if i['Key'].endswith(".parquet")]
        while 'NextContinuationToken' in z.keys():
            z = self.s3.list_objects_v2(
                Bucket=self.bucket, Prefix=self.prefix, ContinuationToken=z['NextContinuationToken'])
            self.files.extend([i['Key'] for i in z['Contents']
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

# this works for a directoy of objects.
class InputS3FilesDataset:

    def __init__(self, bucket, prefix= None) -> None:
        
        self.bucket = bucket
        self.prefix = prefix
        
        self.num_channels = None

    def set_num_channels(self, num_channels):
        self.num_channels = num_channels
        self.s3 = boto3.client('s3')
        if self.prefix is not None:
            z = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)
            self.files = [i['Key'] for i in z['Contents']]
            while 'NextContinuationToken' in z.keys():
                z = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix, ContinuationToken=z['NextContinuationToken'])
                self.files.extend([i['Key'] for i in z['Contents']])
        else:
            z = self.s3.list_objects_v2(Bucket=self.bucket)
            self.files = [i['Key'] for i in z['Contents']]
            while 'NextContinuationToken' in z.keys():
                z = self.s3.list_objects_v2(Bucket=self.bucket, ContinuationToken=z['NextContinuationToken'])
                self.files.extend([i['Key'] for i in z['Contents']])

    def get_next_batch(self, mapper_id, pos=None):
        assert self.num_channels is not None
        if pos is None:
            curr_pos = mapper_id
        else:
            curr_pos = pos
        while curr_pos < len(self.files):
            #print("input batch", (curr_pos - mapper_id) / self.num_channels)
            # since these are arbitrary byte files (most likely some image format), it is probably useful to keep the filename around or you can't tell these things apart
            a = (self.files[curr_pos], self.s3.get_object(Bucket=self.bucket, Key=self.files[curr_pos])['Body'].read())
            #print("ending reading ",time.time())
            curr_pos += self.num_channels
            yield curr_pos, a

class InputDiskCSVDataset:

    def __init__(self, filepath , names, sep=",", stride=16 * 1024 * 1024, header = False) -> None:
       
        self.filepath = filepath
        assert os.path.exists(filepath)
        self.num_channels = None
        self.names = names
        self.sep = sep
        self.stride = stride
        self.header = header

    def set_num_channels(self, num_channels):
        assert self.num_channels == num_channels

    def get_own_state(self,num_channels, window=1024):

        self.num_channels = num_channels
        splits = self.num_channels * 2

        length = os.path.getsize(self.filepath)
        assert length // splits > window * 2
        potential_splits = [length//splits * i for i in range(splits)]
        # adjust the splits now
        adjusted_splits = []

        # the first value is going to be the start of the second row
        # -- we assume there's a header and skip it!

        f = open(self.filepath,"rb")
        resp = f.read(window)

        first_newline = resp.find(bytes('\n', 'utf-8'))
        if first_newline == -1:
            raise Exception
        else:
            adjusted_splits.append(first_newline)

        for i in range(1, len(potential_splits)):
            potential_split = potential_splits[i]
            start = max(0, potential_split - window)
            end = min(potential_split + window, length)

            f.seek(start)
            resp = f.read(end-start)

            last_newline = resp.rfind(bytes('\n', 'utf-8'))
            if last_newline == -1:
                raise Exception
            else:
                adjusted_splits.append(start + last_newline)

        adjusted_splits[-1] = length

        print(length, adjusted_splits)
        self.length = length
        self.adjusted_splits = adjusted_splits
        f.close()

    # default is to get 16 KB batches at a time.
    def get_next_batch(self, mapper_id, pos=None):

        if self.num_channels is None:
            raise Exception(
                "I need to know the total number of channels you are planning on using.")

        splits = len(self.adjusted_splits)
        assert self.num_channels < splits + 1
        assert mapper_id < self.num_channels + 1
        chunks = splits // self.num_channels
        if pos is None:
            start = self.adjusted_splits[chunks * mapper_id]
            pos = start

        if mapper_id == self.num_channels - 1:
            end = self.adjusted_splits[splits - 1]
        else:
            end = self.adjusted_splits[chunks * mapper_id + chunks]

        f = open(self.filepath,"rb")
        while pos < end-1:

            f.seek(pos)
            bytes_to_read = min(pos+self.stride, end) - pos
            resp = f.read(bytes_to_read)
            last_newline = resp.rfind(bytes('\n', 'utf-8'))

            #import pdb;pdb.set_trace()

            if last_newline == -1:
                raise Exception
            else:
                resp = resp[:last_newline]

                if self.header and pos == 0:
                    first_newline = resp.find(bytes('\n','utf-8'))
                    if first_newline == -1:
                        raise Exception
                    resp = resp[first_newline + 1:]
                #print("start convert,",time.time())
                #bump = pd.read_csv(BytesIO(resp), names =self.names, sep = self.sep, index_col = False)
                bump = csv.read_csv(BytesIO(resp), read_options=csv.ReadOptions(
                    column_names=self.names), parse_options=csv.ParseOptions(delimiter=self.sep))
                
                pos += last_newline

                #print("done convert,",time.time())
                yield pos, bump
        f.close()


# this should work for 1 CSV up to multiple
class InputS3CSVDataset:
    def __init__(self, bucket, names, prefix = None, key = None, sep=",", stride=64 * 1024 * 1024, header = False) -> None:
        self.bucket = bucket
        self.prefix = prefix
        self.key = key
        self.num_channels = None
        self.names = names
        self.sep = sep
        self.stride = stride
        self.header = header
    

    def set_num_channels(self, num_channels):
        assert self.num_channels == num_channels
        self.s3 = boto3.client('s3')  # needs boto3 client
    
    # we need to rethink this whole setting num channels business. For this operator we don't want each node to do redundant work!
    def get_own_state(self, num_channels, window = 1024 * 32):
        print("intiializing CSV reading strategy. This is currently done locally, which might take a while.")
        self.num_channels = num_channels

        s3 = boto3.client('s3')  # needs boto3 client, however it is transient and is not part of own state, so Ray can send this thing! 
        if self.key is not None:
            files = deque([self.key])
            response = s3.head_object(Bucket=self.bucket, Key=self.key)
            sizes = deque([response['ContentLength']])
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
            sizes = deque([i['Size'] for i in files])
            files = deque([i['Key'] for i in files])
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

            start = max(0, candidate - window)
            end = min(candidate + window, sizes[0])

            resp = s3.get_object(
                Bucket=self.bucket, Key=files[0], Range='bytes={}-{}'.format(real_off + start,real_off + end))['Body'].read()
            last_newline = resp.rfind(bytes('\n', 'utf-8'))
            if last_newline == -1:
                raise Exception
            else:
                bytes_to_take = start + last_newline

            sizes[0] -= bytes_to_take
            end_byte = real_off + bytes_to_take
            real_off += bytes_to_take
            channel_infos[channel] = (start_byte, my_file.copy(), end_byte)
            start_byte = real_off
        
        self.channel_infos = channel_infos
        print("initialized CSV reading strategy for ", total_size // 1024 // 1024 // 1024, " GB of CSV")


    def get_next_batch(self, mapper_id, state = None):
        assert self.num_channels is not None
        files = self.channel_infos[mapper_id][1]
        
        if state is None:
            curr_pos = 0
            pos = self.channel_infos[mapper_id][0]
        else:
            curr_pos, pos = state
            
        while curr_pos < len(files):
            
            file = files[curr_pos]
            if curr_pos != len(files) - 1:
                response = self.s3.head_object(
                    Bucket=self.bucket,
                    Key= file
                )
                length = response['ContentLength']
                end = length
            else:
                end = self.channel_infos[mapper_id][2]

            
            while pos < end-1:

                resp = self.s3.get_object(Bucket=self.bucket, Key=file, Range='bytes={}-{}'.format(
                    pos, min(pos+self.stride, end)))['Body'].read()
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
                    
                    pos += last_newline
                    
                    yield (curr_pos, pos) , bump

            pos = 0
            curr_pos += 1



