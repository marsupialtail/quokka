from pyquokka.quokka_runtime import TaskGraph
from pyquokka.utils import LocalCluster

cluster = LocalCluster()

# this dataset will generate a sequence of numbers, from 0 to limit. Channel 
class SimpleDataset:
    def __init__(self, bucket, limit, prefix= None) -> None:
        
        self.bucket = bucket
        self.prefix = prefix
        
        self.num_channels = None

    def set_num_channels(self, num_channels):
        self.num_channels = num_channels

    def get_next_batch(self, mapper_id, pos=None):
        # let's ignore the keyword pos = None, which is only relevant for fault tolerance capabilities.
        assert self.num_channels is not None
        curr_number = 0
        while curr_pos < len(self.files):
            #print("input batch", (curr_pos - mapper_id) / self.num_channels)
            # since these are arbitrary byte files (most likely some image format), it is probably useful to keep the filename around or you can't tell these things apart
            a = (self.files[curr_pos], self.s3.get_object(Bucket=self.bucket, Key=self.files[curr_pos])['Body'].read())
            #print("ending reading ",time.time())
            curr_pos += self.num_channels
            yield curr_pos, a