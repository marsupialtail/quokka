import sys
#sys.path.append("/home/ubuntu/quokka-dev/")
import time
from quokka_runtime import TaskGraph
from sql import JoinExecutor, OutputCSVExecutor
import ray
import os
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ["PYTHONPATH"] = parent_dir + ":" + os.environ.get("PYTHONPATH", "")
ray.init("auto",ignore_reinit_error=True)
task_graph = TaskGraph()

quotes = task_graph.new_input_csv("yugan","a-big.csv",["key"] + ["avalue" + str(i) for i in range(100)],2)
trades = task_graph.new_input_csv("yugan","b-big.csv",["key"] + ["bvalue" + str(i) for i in range(100)],2)
join_executor = JoinExecutor("key")
output_stream = task_graph.new_stateless_node({0:quotes,1:trades},join_executor,4)
#output_executor = OutputCSVExecutor(4,"yugan","result")
#wrote = task_graph.new_stateless_node({0:output_stream},output_executor,4)

task_graph.initialize()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)
#import pdb;pdb.set_trace()
