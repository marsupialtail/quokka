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
