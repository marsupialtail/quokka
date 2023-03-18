import pyarrow
import duckdb
import ray
import sqlglot
import os
import numpy as np
import polars
from pyarrow.fs import S3FileSystem
from pyquokka.sql_utils import filters_to_expression
import pyarrow.parquet as pq
import boto3

@ray.remote
class Catalog:
    def __init__(self) -> None:
        self.table_id = 0
        self.samples = {}
        self.ratio = {}
        self.con = duckdb.connect().execute('PRAGMA threads=%d' % 8)
    
    def register_table_data_and_return_ticket(self, sample, ratio):
        assert type(sample) == pyarrow.Table
        self.samples[self.table_id] = sample
        self.ratio[self.table_id] = ratio
        self.table_id += 1
        return self.table_id - 1
    
    def register_s3_csv_source(self, bucket, key, schema, sep, total_size):

        s3 = boto3.client('s3')
        response = s3.head_object(Bucket= bucket, Key=key)
        size = response['ContentLength']
        
        start_pos = np.random.randint(0, max(size - 10 * 1024 * 1024,1))
        sample = s3.get_object(Bucket=bucket, Key=key, Range='bytes={}-{}'.format(start_pos, min(start_pos + 10 * 1024 * 1024, size - 1)))['Body'].read()
        first_new_line = sample.find(b'\n')
        last_new_line = sample.rfind(b'\n')
        sample = sample[first_new_line + 1 : last_new_line]
        sample = polars.read_csv(sample, new_columns = schema, sep = sep, has_header = False).to_arrow()
        return self.register_table_data_and_return_ticket(sample, ratio = total_size / (last_new_line - first_new_line))

    def register_disk_csv_source(self, filename, schema, sep):

        if os.path.isfile(filename):
            files = [filename]
            sizes = [os.path.getsize(filename)]
        else:
            assert os.path.isdir(filename), "Does not support prefix, must give absolute directory path for a list of files, will read everything in there!"
            files = [filename + "/" + file for file in os.listdir(filename)]
            sizes = [os.path.getsize(file) for file in files]
        
        # let's now sample from one file. This can change in the future
        # we will sample 10 MB from the file

        file_to_do = np.random.choice(files, p = np.array(sizes) / sum(sizes))
        start_pos = np.random.randint(0, max(os.path.getsize(file_to_do) - 10 * 1024 * 1024,1))
        with open(file_to_do, 'rb') as f:
            f.seek(start_pos)
            sample = f.read(10 * 1024 * 1024)
        first_new_line = sample.find(b'\n')
        last_new_line = sample.rfind(b'\n')
        sample = sample[first_new_line + 1 : last_new_line]
        sample = polars.read_csv(sample, new_columns = schema, sep = sep, has_header = False).to_arrow()
        return self.register_table_data_and_return_ticket(sample, ratio = sum(sizes) / (last_new_line - first_new_line))
    
    def register_s3_parquet_source(self, filepath, total_files):
        s3fs = S3FileSystem()
        dataset = pq.ParquetDataset(filepath, filesystem=s3fs )
        # very cursory estimate
        sample = dataset.fragments[0].to_table()
        return self.register_table_data_and_return_ticket(sample, ratio = total_files)
    
    def register_disk_parquet_source(self, filepath):
        dataset = pq.ParquetDataset(filepath)
        sample = dataset.fragments[0].to_table() # this very likely will be the entire thing haha
        return self.register_table_data_and_return_ticket(sample, ratio = len(dataset.fragments))


    def estimate_cardinality(self, table_id, predicate, filters_list = None):
        assert issubclass(type(predicate) , sqlglot.exp.Expression)

        sample = self.samples[table_id]
        if filters_list is not None:
            assert type(filters_list) == list
            sample = sample.filter(filters_to_expression(filters_list))
        if predicate == sqlglot.exp.TRUE:
            count = len(sample)
        else:
            sql_statement = "select count(*) from sample where " + predicate.sql()
            con = duckdb.connect().execute('PRAGMA threads=%d' % 8)
            print(sql_statement)
            count = con.execute(sql_statement).fetchall()[0][0]
        
        estimated_cardinality = count * self.ratio[table_id]
        return estimated_cardinality
        