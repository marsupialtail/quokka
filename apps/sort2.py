import sys
sys.path.append("/home/ubuntu/quokka/")
import time
from quokka_runtime import TaskGraph
from sql import  OutputCSVExecutor
from dataset import SortPhase2Dataset

import polars

import redis
r = redis.Redis(host="localhost", port=6800, db=0)
r.flushall()

ips = ['localhost', '172.31.11.134', '172.31.15.208', '172.31.11.188']
workers = 4

# pass through filter.
def partition_key2(data, source_channel, target_channel):

    if source_channel == target_channel:
        return data
    else:
        return None

files = {0:["mergesort-0-15.arrow", "mergesort-0-19.arrow", "mergesort-0-23.arrow",
"mergesort-0-25.arrow"],1:["mergesort-1-16.arrow", "mergesort-1-17.arrow",
"mergesort-1-19.arrow", "mergesort-1-23.arrow", "mergesort-1-24.arrow",
"mergesort-1-25.arrow"],2:["mergesort-2-15.arrow", "mergesort-2-21.arrow",
"mergesort-2-23.arrow", "mergesort-2-25.arrow"],3:["mergesort-3-19.arrow",
"mergesort-3-20.arrow", "mergesort-3-21.arrow", "mergesort-3-23.arrow"], 4:["mergesort-4-17.arrow", "mergesort-4-18.arrow", "mergesort-4-19.arrow",
"mergesort-4-21.arrow", "mergesort-4-22.arrow"], 5:["mergesort-5-19.arrow",
"mergesort-5-21.arrow", "mergesort-5-22.arrow", "mergesort-5-23.arrow"], 6:["mergesort-6-17.arrow", "mergesort-6-21.arrow", "mergesort-6-22.arrow",
"mergesort-6-23.arrow", "mergesort-6-24.arrow", "mergesort-6-25.arrow"], 7:["mergesort-7-18.arrow", "mergesort-7-19.arrow", "mergesort-7-21.arrow",
"mergesort-7-23.arrow", "mergesort-7-24.arrow", "mergesort-7-25.arrow"]}

for key in files:
    files[key] = ["/data/" + i for i in files[key]]

reader = SortPhase2Dataset(files)
task_graph = TaskGraph()
stream = task_graph.new_input_reader_node(reader, {ip:4 for ip in ips[:workers]})
outputer = OutputCSVExecutor("quokka-sorted-lineitem","lineitem")
output = task_graph.new_blocking_node({0:stream}, None,outputer, {ip:4 for ip in ips[:workers]}, {0: partition_key2} )

task_graph.create()
start = time.time()
task_graph.run_with_fault_tolerance()
print("total time ", time.time() - start)