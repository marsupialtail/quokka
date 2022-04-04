import sys
sys.path.append("/home/ubuntu/quokka/")
import datetime
import time
from quokka_runtime import TaskGraph
from dataset import InputHDF5Dataset
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
    def serialize(self, s):
        assert type(s) == list and len(s) == 1
        self.state = s[0][0]

    def execute(self,batches,stream_id, executor_id):
        print(mkl.set_num_threads(4))

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

reader = InputHDF5Dataset("yugan","bigmatrix2.hdf5","data")

task_graph = TaskGraph()

matrix = task_graph.new_input_reader_node(reader, {'localhost':8})

gramian = GramianExecutor()

output = task_graph.new_blocking_node({0:matrix}, None, gramian, {'localhost':1}, {0:None})

task_graph.create()
start = time.time()
task_graph.run_with_fault_tolerance()
print("total time ", time.time() - start)
