import sys
sys.path.append("/home/ubuntu/quokka/")

import time
from quokka_runtime import TaskGraph
from sql import OOCJoinExecutor, JoinExecutor, OutputCSVExecutor, CountExecutor
import ray
import os
task_graph = TaskGraph()

#quotes = task_graph.new_input_csv("yugan","a-big.csv",["key"] + ["avalue" + str(i) for i in range(100)],2,ip="172.31.16.185")
quotes = task_graph.new_input_csv("yugan","a-big.csv",["key"] + ["avalue" + str(i) for i in range(100)],{'localhost':2})
#trades = task_graph.new_input_csv("yugan","b-big.csv",["key"] + ["bvalue" + str(i) for i in range(100)],2,ip="172.31.16.185")
#trades = task_graph.new_input_csv("yugan","b-big.csv",["key"] + ["bvalue" + str(i) for i in range(100)],{'172.31.16.185':2})
trades = task_graph.new_input_csv("yugan","b-big.csv",["key"] + ["bvalue" + str(i) for i in range(100)],{'localhost':2})
join_executor = OOCJoinExecutor(on="key")
#output_stream = task_graph.new_stateless_node({0:quotes,1:trades},join_executor,4,ip="172.31.48.233")
output = task_graph.new_blocking_node({0:quotes,1:trades},None, join_executor,{'localhost':4},{0:"key", 1:"key"})

task_graph.initialize()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)

del task_graph

task_graph2 = TaskGraph()
count_executor = CountExecutor()
joined_stream = task_graph2.new_input_redis(output,{'localhost':4})
final = task_graph2.new_blocking_node({0:joined_stream}, None, count_executor, {'localhost':4}, {0:'key'})

#print(final.to_pandas.remote())

#import pdb;pdb.set_trace()
