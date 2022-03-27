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

def partition_key(data, source_channel,  target_channel):
    
    interval = (200000000 // 4)
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

# list of 16 ips
ips = []

if sys.argv[2] == "csv":
    #lineitems = [task_graph.new_input_csv("tpc-h-csv","lineitem/lineitem.tbl." + str(i),lineitem_scheme,{ips[i-1]:16}, batch_func=lineitem_filter, sep="|") for i in range(1, 17)]
    lineitem = task_graph.new_input_csv("tpc-h-csv","lineitem/lineitem.tbl.1",lineitem_scheme,{'localhost':4, '172.31.11.134':4}, sep="|")
    #lineitem = task_graph.new_input_csv("tpc-h-small","lineitem.tbl",lineitem_scheme,{'localhost':4, '172.31.11.134':4}, sep="|")
elif sys.argv[2] == "parquet":
    if sys.argv[1] == "small":
        raise Exception("not implemented")
    else:
       lineitem = task_graph.new_input_multiparquet("tpc-h-parquet","lineitem.parquet", {'localhost':4,'172.31.11.134':4},columns=['l_shipdate','l_commitdate','l_shipmode','l_receiptdate','l_orderkey'], filters= [('l_shipmode', 'in', ['SHIP','MAIL']),('l_receiptdate','<',compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s")), ('l_receiptdate','>=',compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s"))], batch_func=lineitem_filter_parquet)

executor = MergeSortedExecutor("l_partkey", record_batch_rows = 250000, length_limit = 500000, output_line_limit = 1000000)
stream = task_graph.new_non_blocking_node({0:lineitem}, None, executor, {'localhost':2, '172.31.11.134':2}, {0: partition_key})
outputer = OutputCSVExecutor("quokka-sorted-lineitem","lineitem")
output = task_graph.new_blocking_node({0:stream}, None,outputer, {'localhost':2, '172.31.11.134':2}, {0: partition_key2} )

task_graph.create()
start = time.time()
task_graph.run_with_fault_tolerance()
print("total time ", time.time() - start)
