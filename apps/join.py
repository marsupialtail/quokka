import sys
sys.path.append("/home/ubuntu/quokka/")

import time
from quokka_runtime import TaskGraph
from sql import JoinExecutor, OutputCSVExecutor
import ray
import os
task_graph = TaskGraph()

#quotes = task_graph.new_input_csv("yugan","a-big.csv",["key"] + ["avalue" + str(i) for i in range(100)],2,ip="172.31.16.185")
quotes = task_graph.new_input_csv("yugan","a-big.csv",["key"] + ["avalue" + str(i) for i in range(100)],2)
#trades = task_graph.new_input_csv("yugan","b-big.csv",["key"] + ["bvalue" + str(i) for i in range(100)],2,ip="172.31.16.185")
trades = task_graph.new_input_csv("yugan","b-big.csv",["key"] + ["bvalue" + str(i) for i in range(100)],2, dependents=[quotes])
join_executor = JoinExecutor(on="key")
#output_stream = task_graph.new_stateless_node({0:quotes,1:trades},join_executor,4,ip="172.31.48.233")
output_stream = task_graph.new_stateless_node({0:quotes,1:trades},join_executor,4,{0:"key", 1:"key"})
#output_executor = OutputCSVExecutor(4,"yugan","result")
#wrote = task_graph.new_stateless_node({0:output_stream},output_executor,4)

task_graph.initialize()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)
#import pdb;pdb.set_trace()
