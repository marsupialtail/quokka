import sys
sys.path.append("/home/ubuntu/quokka/")
import datetime
import time
from quokka_runtime import TaskGraph
from sql import MergeSortedExecutor, OutputCSVExecutor
from dataset import InputCSVDataset
import pandas as pd
import ray
import os
import polars
import pyarrow as pa
import pyarrow.compute as compute
import redis
r = redis.Redis(host="localhost", port=6800, db=0)
r.flushall()

ips = ['localhost', '172.31.11.134', '172.31.15.208', '172.31.11.188']
workers = 4
task_graph = TaskGraph()

lineitem_filter = lambda x: polars.from_arrow(x).sort('l_partkey')

def partition_key(data, source_channel,  target_channel):
    
    interval = (200000000 // 16)
    #interval = (200000 // 4)
    range_start = interval * target_channel

    return data[ (data.l_partkey > range_start ) & (data.l_partkey <= range_start + interval) ]

# pass through filter.
def partition_key2(data, source_channel, target_channel):

    if source_channel == target_channel:
        return data
    else:
        return None

lineitem_scheme = ["l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice", 
"l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct",
"l_shipmode","l_comment", "null"]

drop_null = lambda x: polars.from_arrow(x).drop("null")

if sys.argv[1] == "csv":

    lineitem_csv_reader = InputCSVDataset("tpc-h-csv", "lineitem/lineitem.tbl.1", lineitem_scheme , sep="|")
    lineitem_csv_reader.get_csv_attributes(8 * workers)
    lineitem = task_graph.new_input_reader_node(lineitem_csv_reader, {ip:8 for ip in ips[:workers]}, batch_func = drop_null)

elif sys.argv[1] == "parquet":
    raise Exception("not implemented")

executor = MergeSortedExecutor("l_partkey", record_batch_rows = 250000, length_limit = 1000000)
stream = task_graph.new_non_blocking_node({0:lineitem}, None, executor, {ip:4 for ip in ips[:workers]}, {0: partition_key})
outputer = OutputCSVExecutor("quokka-sorted-lineitem","lineitem")
output = task_graph.new_blocking_node({0:stream}, None,outputer, {ip:4 for ip in ips[:workers]}, {0: partition_key2} )

task_graph.create()
start = time.time()
task_graph.run_with_fault_tolerance()
print("total time ", time.time() - start)
