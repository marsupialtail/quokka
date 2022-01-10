import sys
sys.path.append("..")
import time
from quokka import TaskGraph
from sql import JoinExecutor
task_graph = TaskGraph()

quotes = task_graph.new_input_csv("yugan","a.csv",["key","avalue1", "avalue2"],1)
trades = task_graph.new_input_csv("yugan","b.csv",["key","avalue1", "avalue2"],1)
join_executor = JoinExecutor("key")
output_stream = task_graph.new_stateless_node({0:quotes,1:trades},join_executor,1)
task_graph.initialize()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)
#import pdb;pdb.set_trace()