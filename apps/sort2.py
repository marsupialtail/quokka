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

#files = {0: ['/data/mergesort-0-7.arrow', '/data/mergesort-0-9.arrow', '/data/mergesort-0-11.arrow', '/data/mergesort-0-13.arrow', '/data/mergesort-0-14.arrow'], 1: ['/data/mergesort-1-5.arrow', '/data/mergesort-1-7.arrow', '/data/mergesort-1-11.arrow', '/data/mergesort-1-13.arrow', '/data/mergesort-1-14.arrow'], 2: ['/data/mergesort-2-7.arrow', '/data/mergesort-2-9.arrow', '/data/mergesort-2-11.arrow', '/data/mergesort-2-13.arrow', '/data/mergesort-2-14.arrow'], 3: ['/data/mergesort-3-7.arrow', '/data/mergesort-3-9.arrow', '/data/mergesort-3-11.arrow', '/data/mergesort-3-13.arrow', '/data/mergesort-3-14.arrow']}
#files = {0: ['/data/mergesort-0-0.arrow', '/data/mergesort-0-1.arrow', '/data/mergesort-0-2.arrow', '/data/mergesort-0-3.arrow', '/data/mergesort-0-4.arrow'], 1: ['/data/mergesort-1-0.arrow', '/data/mergesort-1-1.arrow', '/data/mergesort-1-2.arrow', '/data/mergesort-1-3.arrow', '/data/mergesort-1-4.arrow'], 2: ['/data/mergesort-2-0.arrow', '/data/mergesort-2-1.arrow', '/data/mergesort-2-2.arrow', '/data/mergesort-2-3.arrow', '/data/mergesort-2-4.arrow'], 3: ['/data/mergesort-3-0.arrow', '/data/mergesort-3-1.arrow', '/data/mergesort-3-2.arrow', '/data/mergesort-3-3.arrow', '/data/mergesort-3-4.arrow'], 4: ['/data/mergesort-4-0.arrow', '/data/mergesort-4-1.arrow', '/data/mergesort-4-2.arrow', '/data/mergesort-4-3.arrow', '/data/mergesort-4-4.arrow'], 5: ['/data/mergesort-5-0.arrow', '/data/mergesort-5-1.arrow', '/data/mergesort-5-2.arrow', '/data/mergesort-5-3.arrow', '/data/mergesort-5-4.arrow'], 6: ['/data/mergesort-6-0.arrow', '/data/mergesort-6-1.arrow', '/data/mergesort-6-2.arrow', '/data/mergesort-6-3.arrow', '/data/mergesort-6-4.arrow'], 7: ['/data/mergesort-7-0.arrow', '/data/mergesort-7-1.arrow', '/data/mergesort-7-2.arrow', '/data/mergesort-7-3.arrow', '/data/mergesort-7-4.arrow']}

files = {0: ['/data/mergesort-0-0.arrow', '/data/mergesort-0-1.arrow', '/data/mergesort-0-2.arrow'], 1: ['/data/mergesort-1-0.arrow', '/data/mergesort-1-1.arrow', '/data/mergesort-1-2.arrow'], 2: ['/data/mergesort-2-0.arrow', '/data/mergesort-2-1.arrow', '/data/mergesort-2-2.arrow'], 3: ['/data/mergesort-3-0.arrow', '/data/mergesort-3-1.arrow', '/data/mergesort-3-2.arrow'], 4: ['/data/mergesort-4-0.arrow', '/data/mergesort-4-1.arrow', '/data/mergesort-4-2.arrow'], 5: ['/data/mergesort-5-0.arrow', '/data/mergesort-5-1.arrow', '/data/mergesort-5-2.arrow'], 6: ['/data/mergesort-6-0.arrow', '/data/mergesort-6-1.arrow', '/data/mergesort-6-2.arrow'], 7: ['/data/mergesort-7-0.arrow', '/data/mergesort-7-1.arrow', '/data/mergesort-7-2.arrow'], 8: ['/data/mergesort-8-0.arrow', '/data/mergesort-8-1.arrow', '/data/mergesort-8-2.arrow'], 9: ['/data/mergesort-9-0.arrow', '/data/mergesort-9-1.arrow', '/data/mergesort-9-2.arrow'], 10: ['/data/mergesort-10-0.arrow', '/data/mergesort-10-1.arrow', '/data/mergesort-10-2.arrow'], 11: ['/data/mergesort-11-0.arrow', '/data/mergesort-11-1.arrow', '/data/mergesort-11-2.arrow'], 12: ['/data/mergesort-12-0.arrow', '/data/mergesort-12-1.arrow', '/data/mergesort-12-2.arrow'], 13: ['/data/mergesort-13-0.arrow', '/data/mergesort-13-1.arrow', '/data/mergesort-13-2.arrow'], 14: ['/data/mergesort-14-0.arrow', '/data/mergesort-14-1.arrow', '/data/mergesort-14-2.arrow'], 15: ['/data/mergesort-15-0.arrow', '/data/mergesort-15-1.arrow', '/data/mergesort-15-2.arrow']}

reader = SortPhase2Dataset(files)
task_graph = TaskGraph()
stream = task_graph.new_input_reader_node(reader, {ip:4 for ip in ips[:workers]})
outputer = OutputCSVExecutor("quokka-sorted-lineitem","lineitem")
output = task_graph.new_blocking_node({0:stream}, None,outputer, {ip:4 for ip in ips[:workers]}, {0: partition_key2} )

task_graph.create()
start = time.time()
task_graph.run_with_fault_tolerance()
print("total time ", time.time() - start)
