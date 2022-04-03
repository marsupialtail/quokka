import sys
sys.path.append("/home/ubuntu/quokka/")
import datetime
import time
from quokka_runtime import TaskGraph
from sql import MergeSortedExecutor, OutputCSVExecutor
import pandas as pd
import ray
import os
import polars
import pyarrow as pa
import pyarrow.compute as compute
import redis
r = redis.Redis(host="localhost", port=6800, db=0)
r.flushall()

task_graph = TaskGraph()

lineitem_filter = lambda x: polars.from_arrow(x).sort('l_partkey')





executor = MergeSortedExecutor("l_partkey", record_batch_rows = 250000, length_limit = 1000000)
stream = task_graph.new_non_blocking_node({0:lineitem}, None, executor, {'localhost':4, '172.31.11.134':4}, {0: partition_key})
outputer = OutputCSVExecutor("quokka-sorted-lineitem","lineitem")
output = task_graph.new_blocking_node({0:stream}, None,outputer, {'localhost':4, '172.31.11.134':4}, {0: partition_key2} )

task_graph.create()
start = time.time()
task_graph.run_with_fault_tolerance()
print("total time ", time.time() - start)
