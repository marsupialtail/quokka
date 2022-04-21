import sys
sys.path.append("/home/ubuntu/quokka/pyquokka")

import time
from quokka_runtime import TaskGraph
from sql import PolarJoinExecutor, OutputCSVExecutor, CountExecutor
import ray
import os
import redis
r = redis.Redis(host="localhost", port=6800, db=0)
r.flushall()
task_graph = TaskGraph()

#quotes = task_graph.new_input_csv("yugan","a-big.csv",["key"] + ["avalue" + str(i) for i in range(100)],2,ip="172.31.16.185")
quotes = task_graph.new_input_csv("yugan","a-big.csv",["key"] + ["avalue" + str(i) for i in range(100)],{'localhost':1})
#trades = task_graph.new_input_csv("yugan","b-big.csv",["key"] + ["bvalue" + str(i) for i in range(100)],2,ip="172.31.16.185")
#trades = task_graph.new_input_csv("yugan","b-big.csv",["key"] + ["bvalue" + str(i) for i in range(100)],{'172.31.16.185':2})
trades = task_graph.new_input_csv("yugan","b-big.csv",["key"] + ["bvalue" + str(i) for i in range(100)],{'localhost':1})
join_executor = PolarJoinExecutor(on="key")
#output_stream = task_graph.new_stateless_node({0:quotes,1:trades},join_executor,4,ip="172.31.48.233")
output = task_graph.new_blocking_node({0:quotes,1:trades},None, join_executor,{'localhost':4},{0:"key", 1:"key"})

print("ids",quotes,trades,output)
task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)

del task_graph
#
task_graph2 = TaskGraph()
count_executor = CountExecutor()
joined_stream = task_graph2.new_input_redis(output,{'localhost':4})
final = task_graph2.new_blocking_node({0:joined_stream}, None, count_executor, {'localhost':1}, {0:None})
task_graph2.create()
task_graph2.run()
print(ray.get(final.to_pandas.remote()))

