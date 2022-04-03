import pickle
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pandas as pd
from io import BytesIO, StringIO
import boto3
import s3fs
import time
import redis
import h5py


# this is used to convert an RDD into streams
# gonna do some intelligent stuff by maximizing data locality


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
        s3 = s3fs.S3FileSystem()
        self.parquet_file = pq.ParquetFile(s3.open(bucket + filename, "rb"))
        self.num_row_groups = self.parquet_file.num_row_groups
        self.num_mappers = None
        self.columns = columns

    def set_num_mappers(self, num_mappers):
        self.num_mappers = num_mappers

    def get_next_batch(self, mapper_id, pos=None):
        assert self.num_mappers is not None
        if pos is None:
            curr_row_group = mapper_id
        else:
            curr_row_group = pos
        while curr_row_group < len(self.num_row_groups):
            a = self.parquet_file.read_row_group(
                curr_row_group, columns=self.columns)
            yield curr_row_group, a

# use this if you have a lot of small parquet files

# I love this
# currently only support chunking in one dimension. two-D chunking actually requires different algos
class InputHDF5Dataset:

    def __init__(self, bucket, filename, key, columns=None) -> None:
        s3 = s3fs.S3FileSystem()
        self.h5file = h5py.File(s3.open("s3://" + bucket + "/" + filename, "rb"))
        self.dataset = self.h5file[key]
        self.chunk_size = self.dataset.chunks
        self.dataset_shape = self.dataset.shape

        assert self.chunk_size[1] == self.dataset_shape[1]
        self.num_chunks = (self.dataset_shape[0]-1) // self.chunk_size[0] + 1

        self.num_mappers = None
        
    def set_num_mappers(self, num_mappers):
        self.num_mappers = num_mappers

    def get_next_batch(self, mapper_id, pos=None):
        assert self.num_mappers is not None
        if pos is None:
            curr_chunk = mapper_id
        else:
            curr_chunk = pos
        while curr_chunk < self.num_chunks:
            chunk_start = curr_chunk * self.chunk_size[0]            
            result = self.dataset[chunk_start:chunk_start + self.chunk_size[0]]
            yield curr_chunk, result


class InputMultiParquetDataset:

    # filter pushdown could be profitable in the future, especially when you can skip entire Parquet files
    # but when you can't it seems like you still read in the entire thing anyways
    # might as well do the filtering at the Pandas step. Also you need to map filters to the DNF form of tuples, which could be
    # an interesting project in itself. Time for an intern?

    def __init__(self, bucket, prefix, columns=None, filters=None) -> None:
        self.s3 = boto3.client('s3')
        self.bucket = bucket
        z = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        self.files = [i['Key']
                      for i in z['Contents'] if i['Key'].endswith(".parquet")]
        self.num_mappers = None
        self.columns = columns
        self.filters = filters
        while 'NextContinuationToken' in z.keys():
            z = self.s3.list_objects_v2(
                Bucket=bucket, Prefix=prefix, ContinuationToken=z['NextContinuationToken'])
            self.files.extend([i['Key'] for i in z['Contents']
                              if i['Key'].endswith(".parquet")])

    def set_num_mappers(self, num_mappers):
        self.num_mappers = num_mappers

    def get_next_batch(self, mapper_id, pos=None):
        assert self.num_mappers is not None
        if pos is None:
            curr_pos = mapper_id
        else:
            curr_pos = pos
        while curr_pos < len(self.files):
            print("input batch", (curr_pos - mapper_id) / self.num_mappers)
            #print("starting reading ",time.time())
            #a = pq.read_table("s3://" + self.bucket + "/" + self.files[curr_pos],columns=self.columns, filters = self.filters).to_pandas()
            a = pq.read_table("s3://" + self.bucket + "/" +
                              self.files[curr_pos], columns=self.columns, filters=self.filters)
            #print("ending reading ",time.time())
            curr_pos += self.num_mappers
            yield curr_pos, a


class InputCSVDataset:

    def __init__(self, bucket, key, names, id, sep=",", stride=64 * 1024 * 1024) -> None:

        self.s3 = boto3.client('s3')  # needs boto3 client
        self.bucket = bucket
        self.key = key
        self.num_mappers = None
        self.names = names
        self.id = id
        self.sep = sep
        self.stride = stride

    def set_num_mappers(self, num_mappers):
        self.num_mappers = num_mappers
        self.length, self.adjusted_splits = self.get_csv_attributes()

    def get_csv_attributes(self, window=1024 * 32):

        if self.num_mappers is None:
            raise Exception
        splits = 64

        response = self.s3.head_object(
            Bucket=self.bucket,
            Key=self.key
        )
        length = response['ContentLength']
        assert length // splits > window * 2
        potential_splits = [length//splits * i for i in range(splits)]
        # adjust the splits now
        adjusted_splits = []

        # the first value is going to be the start of the second row
        # -- we assume there's a header and skip it!

        resp = self.s3.get_object(Bucket=self.bucket, Key=self.key,
                                  Range='bytes={}-{}'.format(0, window))['Body'].read()

        first_newline = resp.find(bytes('\n', 'utf-8'))
        if first_newline == -1:
            raise Exception
        else:
            adjusted_splits.append(first_newline)

        for i in range(1, len(potential_splits)):
            potential_split = potential_splits[i]
            start = max(0, potential_split - window)
            end = min(potential_split + window, length)

            resp = self.s3.get_object(
                Bucket=self.bucket, Key=self.key, Range='bytes={}-{}'.format(start, end))['Body'].read()
            last_newline = resp.rfind(bytes('\n', 'utf-8'))
            if last_newline == -1:
                raise Exception
            else:
                adjusted_splits.append(start + last_newline)

        adjusted_splits[-1] = length

        print(length, adjusted_splits)
        return length, adjusted_splits

    # default is to get 16 KB batches at a time.
    def get_next_batch(self, mapper_id, pos=None):

        if self.num_mappers is None:
            raise Exception(
                "I need to know the total number of mappers you are planning on using.")

        splits = len(self.adjusted_splits)
        assert self.num_mappers < splits + 1
        assert mapper_id < self.num_mappers + 1
        chunks = splits // self.num_mappers
        if pos is None:
            start = self.adjusted_splits[chunks * mapper_id]
            pos = start

        if mapper_id == self.num_mappers - 1:
            end = self.adjusted_splits[splits - 1]
        else:
            end = self.adjusted_splits[chunks * mapper_id + chunks]

        while pos < end-1:

            resp = self.s3.get_object(Bucket=self.bucket, Key=self.key, Range='bytes={}-{}'.format(
                pos, min(pos+self.stride, end)))['Body'].read()
            last_newline = resp.rfind(bytes('\n', 'utf-8'))

            #import pdb;pdb.set_trace()

            if last_newline == -1:
                raise Exception
            else:
                resp = resp[:last_newline]
                pos += last_newline
                #print("start convert,",time.time())
                #bump = pd.read_csv(BytesIO(resp), names =self.names, sep = self.sep, index_col = False)
                bump = csv.read_csv(BytesIO(resp), read_options=csv.ReadOptions(
                    column_names=self.names), parse_options=csv.ParseOptions(delimiter=self.sep))
                #print("done convert,",time.time())
                yield pos, bump


class InputMultiCSVDataset:
    def __init__(self, bucket, prefix, names, id, sep=",", stride=64 * 1024 * 1024) -> None:

        self.s3 = boto3.client('s3')  # needs boto3 client
        self.bucket = bucket
        self.prefix = prefix
        self.num_mappers = None
        self.names = names
        self.id = id
        self.sep = sep
        self.stride = stride

        if prefix is not None:
            z = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            self.files = [i['Key'] for i in z['Contents']]
            while 'NextContinuationToken' in z.keys():
                z = self.s3.list_objects_v2(
                    Bucket=bucket, Prefix=prefix, ContinuationToken=z['NextContinuationToken'])
                self.files.extend([i['Key'] for i in z['Contents']])
        else:
            z = self.s3.list_objects_v2(Bucket=bucket)
            self.files = [i['Key'] for i in z['Contents']]
            while 'NextContinuationToken' in z.keys():
                z = self.s3.list_objects_v2(
                    Bucket=bucket, ContinuationToken=z['NextContinuationToken'])
                self.files.extend([i['Key'] for i in z['Contents']])
        
        self.files = sorted(self.files)
        print(len(self.files))
    def set_num_mappers(self, num_mappers):
        self.num_mappers = num_mappers

    def get_next_batch(self, mapper_id, curr_pos = None, pos=None):
        assert self.num_mappers is not None
        
        if curr_pos is None and pos is None:
            curr_pos = mapper_id
            pos = 0
        else:
            assert curr_pos is not None and pos is not None # restart
            
        while curr_pos < len(self.files):
            
            response = self.s3.head_object(
                Bucket=self.bucket,
                Key=self.files[curr_pos]
            )
            length = response['ContentLength']
            pos = 0
            end = length

            while pos < end-1:

                resp = self.s3.get_object(Bucket=self.bucket, Key=self.files[curr_pos], Range='bytes={}-{}'.format(
                    pos, min(pos+self.stride, end)))['Body'].read()
                last_newline = resp.rfind(bytes('\n', 'utf-8'))

                #import pdb;pdb.set_trace()

                if last_newline == -1:
                    raise Exception
                else:
                    resp = resp[:last_newline]
                    pos += last_newline
                    #print("start convert,",time.time())
                    #bump = pd.read_csv(BytesIO(resp), names =self.names, sep = self.sep, index_col = False)
                    bump = csv.read_csv(BytesIO(resp), read_options=csv.ReadOptions(
                        column_names=self.names), parse_options=csv.ParseOptions(delimiter=self.sep))
                    #print("done convert,",time.time())
                    yield (curr_pos, pos) , bump

            curr_pos += self.num_mappers
