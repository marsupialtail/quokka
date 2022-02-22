import sys

import time
from quokka_runtime import TaskGraph
from sql import StatelessExecutor, StorageExecutor
import pandas as pd
import redis
import numpy as np
import pickle

class SpMVExecutor(StatelessExecutor):
    # this is basically an inner join, but we need to do some smart things to manage memory

    def __init__(self):

        
        self.my_matrix = None
        self.my_objects = None
        self.partial_sum = None
        
        # this is a Ray optimization. we should not instantiate the state when making the objects and passing them around
        # we should insantiate them once they are on their machines. So instantiate state lazily
        self.result = None 

    # this whole set up is not great
    def initialize(self, datasets, my_id):
        assert type(datasets) == list and len(datasets) == 0
        dataset_objects = self.datasets[0].get_objects.remote()
        self.my_objects = dataset_objects[my_id]
        

    def execute(self,batches, stream_id, executor_id):

        # cannot do it in initialize function because all the nodes in a task graph are initialized at once
        # we are not using shared memory solution, so this will blow up the memory usage if done in initialize! do this lazily once you need it
        if self.my_matrix is None:
            dfs = []
            for object in self.my_objects:
                ip, key, size = object
                r = redis.Redis(host=ip, port=6800, db=0)
                # with shared memory object store, picle.loads and r.get latency should be gone. the concat latency might still be there
                dfs.append(pickle.loads(r.get(key)))
            self.my_matrix = pd.concat(dfs) - 1 # matrix market is 0 indexed, so subtract 1 from all indices

        result = self.my_matrix.merge(pd.concat(batches), on = "y").groupby("x").agg({'val':'sum'})

        if self.partial_sum is None:
            self.partial_sum = result 
        else:
            self.partial_sum = self.partial_sum.add(result, fill_value = 0)
   
    def done(self,executor_id):
        vector = self.partial_sum.reset_index()
        vector.rename(columns = {"x":"y"},inplace= True)
        del self.my_matrix
        return vector


ROWS = 4847571
BLOCKS = 8

def partition_key(data, channel):
    assigned_portion_start = ROWS // BLOCKS * channel
    assigned_portion_end = ROWS//BLOCKS * (channel + 1)
    return data[(data.x >= assigned_portion_start) & (data.x < assigned_portion_end)]

def partition_key_vector(data, channel):
    assigned_portion_start = ROWS // BLOCKS * channel
    assigned_portion_end = ROWS//BLOCKS * (channel + 1)
    return data[(data.y >= assigned_portion_start) & (data.y < assigned_portion_end)]

storage_graph = TaskGraph()
graph_stream = storage_graph.new_input_csv("pagerank-graphs","livejournal.csv",["x","y"],{'localhost':8}, sep=" " )
storage_executor = StorageExecutor()
graph_dataset = storage_graph.new_blocking_node({0:graph_stream},None, storage_graph, {"localhost":8}, partition_key)
storage_graph.initialize()
storage_graph.execute()

execute_graph = TaskGraph()
spmv = SpMVExecutor()
vector_stream = execute_graph.new_input_csv("pagerank-graphs","vector.csv",["y","val"], {'localhost':8}, sep= " ")
for i in range(9):
    vector_stream = execute_graph.new_non_blocking_node({0:vector_stream}, [graph_dataset], spmv, {'localhost':8}, {0:None})
final_vector = execute_graph.new_blocking_node({0:vector_stream}, [graph_dataset], spmv, {'localhost':8}, {0:None})
print(final_vector.to_numpy.remote())
