import pandas as pd
import sys
sys.path.append("/home/ubuntu/quokka/")
import datetime
import time
from quokka_runtime import TaskGraph
from sql import AggExecutor, PolarJoinExecutor, StorageExecutor
from dataset import InputCSVDataset, InputMultiParquetDataset
import pyarrow.compute as compute
import os
import polars
import redis
import ray
r = redis.Redis(host="localhost", port=6800, db=0)
r.flushall()


# aggregation push down might be interesting to think about
# this is not very good because we don't know the thing is actually sorted in l_order on lineitem

ips = [ 'localhost','172.31.11.134','172.31.15.208','172.31.11.188']
workers = 4

def batch_func2(df):
    df["product"] = df["l_extendedprice"] * (1 - df["l_discount"])
    return df.groupby(["o_orderkey", "o_orderdate", "o_shippriority"]).agg(revenue = ('product','sum'))

def final_func(state):
    return state.sort_values(['revenue','o_orderdate'],ascending = [False,True])[:10]

lineitem_scheme = ["l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice", 
"l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct",
"l_shipmode","l_comment", "null"]
order_scheme = ["o_orderkey", "o_custkey","o_orderstatus","o_totalprice","o_orderdate","o_orderpriority","o_clerk",
"o_shippriority","o_comment", "null"]
customer_scheme = ["c_custkey","c_name","c_address","c_nationkey","c_phone","c_acctbal","c_mktsegment","c_comment", "null"]

orders_filter = lambda x: polars.from_arrow(x.filter(compute.less(x['o_orderdate'] , compute.strptime("1995-03-03",format="%Y-%m-%d",unit="s"))).select(["o_orderkey","o_custkey","o_shippriority", "o_orderdate"]))
lineitem_filter = lambda x: polars.from_arrow(x.filter(compute.greater(x['l_shipdate'] , compute.strptime("1995-03-15",format="%Y-%m-%d",unit="s"))).select(["l_orderkey","l_extendedprice","l_discount"]))
customer_filter = lambda x: polars.from_arrow(x.filter(compute.equal(x["c_mktsegment"] , 'BUILDING')).select(["c_custkey"]))


lineitem_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","lineitem.parquet", columns = ["l_orderkey","l_extendedprice","l_discount"], filters= [('l_shipdate','>',compute.strptime("1995-03-15",format="%Y-%m-%d",unit="s"))])
orders_parquet_reader =InputMultiParquetDataset("tpc-h-parquet","orders.parquet", columns = ["o_orderkey","o_custkey","o_shippriority", "o_orderdate"], filters= [('o_orderdate','<',compute.strptime("1995-03-03",format="%Y-%m-%d",unit="s"))])
customer_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","customer.parquet", columns = ["c_custkey"], filters= [("c_mktsegment","==","BUILDING")])

def partition_key1(data, source_channel, target_channel):

    if source_channel // 4 == target_channel:
        return data
    else:
        return None

def partition_key2(data, source_channel, target_channel):

    if source_channel  == target_channel:
        return data
    else:
        return None

storage = StorageExecutor()
task_graph = TaskGraph()
lineitem = task_graph.new_input_reader_node(lineitem_parquet_reader, {ips[i]: 8 for i in range(workers)})
orders = task_graph.new_input_reader_node(orders_parquet_reader,{ips[i]: 8 for i in range(workers)})
customer = task_graph.new_input_reader_node(customer_parquet_reader, {ips[i]: 8 for i in range(workers)})
    
cached_lineitem = task_graph.new_blocking_node({0:lineitem},None, storage, {ip:8 for ip in ips[:workers]}, {0:partition_key2})
cached_orders = task_graph.new_blocking_node({0:orders},None, storage, {ip:8 for ip in ips[:workers]}, {0:partition_key2})
cached_customer = task_graph.new_blocking_node({0:customer},None, storage, {ip:8 for ip in ips[:workers]}, {0:partition_key2})

task_graph.create()
start = time.time()
task_graph.run_with_fault_tolerance()
load_time = time.time() - start

task_graph2 = TaskGraph()
orders = task_graph2.new_input_redis(cached_orders, {ip:4 for ip in ips[:workers]})
customer = task_graph2.new_input_redis(cached_customer, {ip:4 for ip in ips[:workers]})
# join order picked by hand, might not be  the best one!
join_executor1 = PolarJoinExecutor(left_on = "c_custkey", right_on = "o_custkey",columns=["o_orderkey", "o_orderdate", "o_shippriority"])
cached_temp = task_graph2.new_blocking_node({0:customer,1:orders},None, join_executor1,{ips[i]: 4 for i in range(workers)}, {0:"c_custkey", 1:"o_custkey"})
task_graph2.create()
start = time.time()
task_graph2.run_with_fault_tolerance()
compute_time = time.time() - start

task_graph3 = TaskGraph()
join_executor2 = PolarJoinExecutor(left_on="o_orderkey",right_on="l_orderkey",batch_func=batch_func2)
lineitem = task_graph3.new_input_redis(cached_lineitem, {ip:4 for ip in ips[:workers]})
temp = task_graph3.new_input_redis(cached_temp, {ip:4 for ip in ips[:workers]})
cached_joined = task_graph3.new_blocking_node({0:temp, 1: lineitem},None, join_executor2, {ips[i]: 4 for i in range(workers)}, {0: "o_orderkey", 1:"l_orderkey"})
task_graph3.create()
start = time.time()
task_graph3.run_with_fault_tolerance()
compute_time += time.time() - start


task_graph4 = TaskGraph()
agg_executor = AggExecutor(fill_value = 0)
joined = task_graph4.new_input_redis(cached_joined, {ip:4 for ip in ips[:workers]})
intermediate = task_graph4.new_non_blocking_node({0:joined}, None, agg_executor, {ips[i]: 1 for i in range(workers)}, {0:partition_key1})
agg_executor1 = AggExecutor(final_func=final_func)
agged = task_graph4.new_blocking_node({0:intermediate}, None, agg_executor1, {'localhost':1}, {0:None})

task_graph4.create()
start = time.time()
task_graph4.run()
compute_time += time.time() - start

print("load time ", load_time)
print("compute time ", compute_time)
print(ray.get(agged.to_pandas.remote()))
