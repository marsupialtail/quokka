import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.executors import JoinExecutor, CountExecutor
from pyquokka.target_info import TargetInfo, PassThroughPartitioner, HashPartitioner
from pyquokka.placement_strategy import SingleChannelStrategy
#from pyquokka.dataset import InputDiskJSONDataset
from pyquokka.dataset import InputDiskCSVDataset
import sqlglot
import pandas as pd

from pyquokka.utils import LocalCluster, QuokkaClusterManager


import pickle
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.json as json
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

        if schema is not None:
            self.parse_options = json.ParseOptions(json.ParseOptions(explicit_schema = schema, newlines_in_values = False))
        else:
            self.parse_options = json.ParseOptions(json.ParseOptions(newlines_in_values = False))


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
                min_key = json.read_json(BytesIO(resp), read_options=json.ReadOptions(
                    use_threads=True), parse_options=self.parse_options)[self.sort_key][0].as_py()
                
                f = open(file, "rb")
                f.seek(size - self.window)
                resp = f.read(self.window)
                first_newline = resp.find(bytes('\n','utf-8'))
                resp = resp[first_newline + 1:]
                max_key = json.read_json(BytesIO(new_resp), read_options=json.ReadOptions(
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

            bump = json.read_json(BytesIO(resp), read_options=json.ReadOptions(
                    use_threads=True), parse_options=self.parse_options)
            bump = bump.select(self.keys) if self.keys is not None else bump

            return None, bump


manager = QuokkaClusterManager()
cluster = LocalCluster()

task_graph = TaskGraph(cluster)

a_reader = InputDiskJSONDataset("a.json")
b_reader = InputDiskCSVDataset("b.csv", header = True ,  stride =  1024)
a = task_graph.new_input_reader_node(a_reader)
b = task_graph.new_input_reader_node(b_reader)

join_executor = JoinExecutor(left_on="key_a", right_on = "key_b")
joined = task_graph.new_non_blocking_node({0:a,1:b},join_executor,
    source_target_info={0:TargetInfo(partitioner = HashPartitioner("key_a"), 
                                    predicate = sqlglot.exp.TRUE,
                                    projection = ["key_a"],
                                    batch_funcs = []), 
                        1:TargetInfo(partitioner = HashPartitioner("key_b"),
                                    predicate = sqlglot.exp.TRUE,
                                    projection = ["key_b"],
                                    batch_funcs = [])})
count_executor = CountExecutor()
count = task_graph.new_blocking_node({0:joined},count_executor, placement_strategy= SingleChannelStrategy(),
    source_target_info={0:TargetInfo(partitioner = PassThroughPartitioner(),
                                    predicate = sqlglot.exp.TRUE,
                                    projection = None,
                                    batch_funcs = [])})

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(count.to_df())

a = json.read_json("a.json")
a = a.to_pandas()
a["key_a"] = a["key_a"].astype(str)
b = pd.read_csv("b.csv",names=["key_b","val1","val2"])
print(len(a.merge(b,left_on="key_a", right_on = "key_b", how="inner")))
