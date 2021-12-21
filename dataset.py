import pandas as pd
from io import BytesIO
import boto3

class CSVDataset:

    def __init__(self, bucket, key, names) -> None:

        self.s3 = boto3.client('s3') # needs boto3 client
        self.bucket = bucket
        self.key = key
        self.num_mappers = None
        self.names = names
        self.length, self.adjusted_splits = self.get_csv_attributes()
        

    def get_csv_attributes(self, splits = 16, window = 1024):

        if self.num_mappers is not None:
            splits = self.num_mappers

        response = self.s3.head_object(
            Bucket=self.bucket,
            Key=self.key
        )
        length = response['ContentLength']
        assert length // splits > window * 2
        potential_splits = [length//splits * i for i in range(splits)]
        #adjust the splits now
        adjusted_splits = []

        # the first value is going to be the start of the second row 
        # -- we assume there's a header and skip it!
        resp = self.s3.get_object(Bucket=self.bucket,Key=self.key, Range='bytes={}-{}'.format(0, window))['Body'].read()
        first_newline = resp.find(bytes('\n','utf-8'))
        if first_newline == -1:
            raise Exception
        else:
            adjusted_splits.append(first_newline)

        for i in range(1, len(potential_splits)):
            potential_split = potential_splits[i]
            start = max(0,potential_split - window)
            end = min(potential_split + window, length)
            resp = self.s3.get_object(Bucket=self.bucket,Key=self.key, Range='bytes={}-{}'.format(start, end))['Body'].read()
            last_newline = resp.rfind(bytes('\n','utf-8'))
            if last_newline == -1:
                raise Exception
            else:
                adjusted_splits.append(start + last_newline)
        
        return length, adjusted_splits

    def get_next_batch(self, mapper_id, stride=1024 * 16): #default is to get 16 KB batches at a time. 
        
        if self.num_mappers is None:
            raise Exception("I need to know the total number of mappers you are planning on using.")

        splits = len(self.adjusted_splits)
        assert self.num_mappers < splits + 1
        assert mapper_id < self.num_mappers + 1
        chunks = splits // self.num_mappers
        start = self.adjusted_splits[chunks * mapper_id]
        if mapper_id == self.num_mappers - 1:
            end = self.adjusted_splits[splits - 1]
        else:
            end = self.adjusted_splits[chunks * mapper_id + chunks] 

        print(start, end)
        pos = start
        while pos < end:
            resp = self.s3.get_object(Bucket='yugan',Key='quote.csv', Range='bytes={}-{}'.format(pos,min(pos+stride,end)))['Body'].read()
            last_newline = resp.rfind(bytes('\n','utf-8'))
            
            if last_newline == -1:
                raise Exception
            else:
                resp = resp[:last_newline]
                pos += last_newline
                yield pd.read_csv(BytesIO(resp), names =self.names )