import sys
sys.path.append("/home/ubuntu/quokka/pyquokka")

import time
from quokka_runtime import TaskGraph
from sql import StatelessExecutor, AggExecutor
import pandas as pd
import numpy as np
import ray
import os
import redis
r = redis.Redis(host="localhost", port=6800, db=0)
r.flushall()
task_graph = TaskGraph()

BX = 2
BY = 2
MATRIX_SIZE = 1000

def partition_key_0(data, source_channel, target_channel):
    channel_x = target_channel // BY 
    range_x_start = (MATRIX_SIZE // BX) * channel_x 

    return data[ (data.key >= range_x_start ) & (data.key < range_x_start + (MATRIX_SIZE // BX)) ]

def partition_key_1(data, source_channel, target_channel):
    channel_y = target_channel % BY 
    range_y_start = (MATRIX_SIZE // BY) * channel_y

    return data[ (data.key >= range_y_start) & (data.key < range_y_start + (MATRIX_SIZE//BY))]

class MatMulExecutor(StatelessExecutor):
    # this is basically an inner join, but we need to do some smart things to manage memory

    def __init__(self):

        
        self.state0 = []
        self.state1 = []
        
        # this is a Ray optimization. we should not instantiate the state when making the objects and passing them around
        # we should insantiate them once they are on their machines. So instantiate state lazily
        self.result = None 

    def execute(self,batches, stream_id, executor_id):

        if self.result is None:
            self.result = np.zeros((MATRIX_SIZE // BX, MATRIX_SIZE // BY))

        batch = pd.concat(batches).to_numpy()[:,1:]
        if stream_id == 0:
            
            row_start = sum([i.shape[0] for i in self.state0])
            row_end = row_start + batch.shape[0]
            col_start = 0
            for shard in self.state1:
                col_end = col_start + shard.shape[1]
                self.result[row_start:row_end, col_start : col_end] = np.dot(batch, shard)
                col_start = col_end
            self.state0.append(batch)
        
        if stream_id == 1:
            transposed = np.transpose(batch)
            col_start = sum([i.shape[1] for i in self.state1])
            col_end = col_start + transposed.shape[1]
            print(col_start, col_end)
            row_start = 0
            for shard in self.state0:
                row_end = row_start + shard.shape[0]
                self.result[row_start: row_end, col_start : col_end] = np.dot(shard, transposed)
                row_start = row_end
            self.state1.append(transposed)
   
    def done(self,executor_id):
        print(np.sum(self.result))
        return pd.DataFrame([np.sum(self.result)])
        print("done join ", executor_id) 


A = task_graph.new_input_csv("quokka-examples","matrix.csv",["key"] + ["avalue" + str(i) for i in range(MATRIX_SIZE)],{'localhost':4})
AT = task_graph.new_input_csv("quokka-examples","matrix.csv",["key"] + ["avalue" + str(i) for i in range(MATRIX_SIZE)],{'localhost':4})

mm_executor = MatMulExecutor()
#output_stream = task_graph.new_stateless_node({0:quotes,1:trades},join_executor,4,ip="172.31.48.233")
output_stream = task_graph.new_stateless_node({0:A,1:AT},mm_executor,{'localhost':4},{0:partition_key_0, 1:partition_key_1})
#agg_executor = AggExecutor()
#agged = task_graph.new_stateless_node({0:output_stream}, agg_executor, {'localhost':1},{0:None})
#counted = task_graph.new_stateless_node({0:output_stream}, count_executor, {'localhost':1}, {0:None})
#output_executor = OutputCSVExecutor(4,"quokka-examples","result")
#wrote = task_graph.new_stateless_node({0:output_stream},output_executor,4)

task_graph.initialize()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)
#import pdb;pdb.set_trace()
