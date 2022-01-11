import sys
sys.path.append("..")
import time
from quokka import TaskGraph
from sql import JoinExecutor, OutputCSVExecutor
task_graph = TaskGraph()

quotes = task_graph.new_input_csv("yugan","a.csv",["key","avalue1", "avalue2"],2)
trades = task_graph.new_input_csv("yugan","b.csv",["key","avalue1", "avalue2"],2)
join_executor = JoinExecutor("key")
output_stream = task_graph.new_stateless_node({0:quotes,1:trades},join_executor,2)
output_executor = OutputCSVExecutor(2,"yugan","result")
wrote = task_graph.new_stateless_node({0:output_stream},output_executor,2)


task_graph.initialize()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)
#import pdb;pdb.set_trace()