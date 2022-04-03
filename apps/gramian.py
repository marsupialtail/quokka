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
r = redis.Redis(host="localhost", port=6800, db=0)
r.flushall()

class GramianExecutor:
    def __init__(self) -> None:
        self.state = None
    def initialize(datasets):
        pass
    def serialize(self):
        pass
    def serialize(self, s):
        pass

    def execute(self,batches,stream_id, executor_id):
        
        batch = np.concat(batches)

        if self.state is None:
            self.state = np.dot(np.transpose(batch), batch)
        else:
            self.state += np.dot(np.transpose(batch), batch)

    def done(self,executor_id):
        print("done")
        return self.state 

reader = InputHDF5Dataset("yugan","bigmatrix2.hdf5","data")

task_graph = TaskGraph()

matrix = task_graph.new_input_reader_node(reader, {'localhost':4})

gramian = GramianExecutor()

output = task_graph.new_blocking_node({0:matrix}, None, gramian, {'localhost':1}, {0:None})

task_graph.create()
start = time.time()
task_graph.run_with_fault_tolerance()
print("total time ", time.time() - start)
