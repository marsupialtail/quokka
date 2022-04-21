import sys
sys.path.append("/home/ubuntu/quokka/pyquokka")
import datetime
import time
from quokka_runtime import TaskGraph
from dataset import InputHDF5Dataset, InputDiskHDF5Dataset
import pandas as pd
import ray
import os
import numpy as np
import redis
import mkl
r = redis.Redis(host="localhost", port=6800, db=0)
r.flushall()

class GramianExecutor:
    def __init__(self) -> None:
        self.state = None
        self.num_states = 1
    def initialize(datasets):
        pass
    def serialize(self):
        return {0: self.state}, "all"
    def deserialize(self, s):
        assert type(s) == list and len(s) == 1
        self.state = s[0][0]

    def execute(self,batches,stream_id, executor_id):
        batches = [batch for batch in batches if batch is not None]
        print(mkl.set_num_threads(8))

        print("start",time.time())
        for batch in batches:
            if self.state is None:
                self.state = np.transpose(batch).dot(batch)
            else:
                self.state += np.transpose(batch).dot(batch)
        print("end",time.time())

    def done(self,executor_id):
        print("done")
        return self.state 

def partition_key1(data, source_channel, target_channel):

    if source_channel // 8 == target_channel:
        return data
    else:
        return None

reader = InputHDF5Dataset("yugan","bigmatrix3.hdf5","data")
#reader = InputDiskHDF5Dataset("/data/bigmatrix3.hdf5","data")

task_graph = TaskGraph()

matrix = task_graph.new_input_reader_node(reader, {'localhost':8, '172.31.11.134':8,'172.31.15.208':8, '172.31.10.96':8})
#matrix = task_graph.new_input_reader_node(reader, {'localhost':4})

gramian = GramianExecutor()

#output = task_graph.new_blocking_node({0:matrix}, None, gramian, {'localhost':1}, {0:None})
output = task_graph.new_blocking_node({0:matrix}, None, gramian, {'localhost':1, '172.31.11.134':1,'172.31.15.208':1, '172.31.10.96':1}, {0:partition_key1})

task_graph.create()
start = time.time()
task_graph.run_with_fault_tolerance()
print("total time ", time.time() - start)
