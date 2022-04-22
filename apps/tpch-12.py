import sys
sys.path.append("/home/ubuntu/quokka/pyquokka")
import datetime
import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.sql import AggExecutor, PolarJoinExecutor
from pyquokka.dataset import InputCSVDataset, InputMultiParquetDataset
import pandas as pd
import ray
import os
import polars
import pyarrow as pa
import pyarrow.compute as compute
import redis
from schema import * 
r = redis.Redis(host="localhost", port=6800, db=0)
r.flushall()

ips = ['localhost', '172.31.11.134', '172.31.15.208', '172.31.11.188']
workers = 4

task_graph = TaskGraph()

def batch_func(df):
    df["high"] = ((df["o_orderpriority"] == "1-URGENT") | (df["o_orderpriority"] == "2-HIGH")).astype(int)
    df["low"] = ((df["o_orderpriority"] != "1-URGENT") & (df["o_orderpriority"] != "2-HIGH")).astype(int)
    result = df.groupby("l_shipmode").agg({'high':['sum'],'low':['sum']})
    return result

orders_filter = lambda x: polars.from_arrow(x.select(["o_orderkey","o_orderpriority"]))
lineitem_filter = lambda x: polars.from_arrow(x.filter(compute.and_(compute.and_(compute.and_(compute.is_in(x["l_shipmode"],value_set = pa.array(["SHIP","MAIL"])), compute.less(x["l_commitdate"], x["l_receiptdate"])), compute.and_(compute.less(x["l_shipdate"], x["l_commitdate"]), compute.greater_equal(x["l_receiptdate"], compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s")))), compute.less(x["l_receiptdate"], compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s")))).select(["l_orderkey","l_shipmode"]))
orders_filter_parquet = lambda x: polars.from_arrow(x)
lineitem_filter_parquet = lambda x: polars.from_arrow(x.filter(compute.and_(compute.less(x["l_commitdate"], x["l_receiptdate"]), compute.less(x["l_shipdate"], x["l_commitdate"]))).select(["l_orderkey","l_shipmode"]))


if sys.argv[2] == "csv":
    if sys.argv[1] == "small":
        lineitem_csv_reader = InputCSVDataset("tpc-h-small", "lineitem.tbl", lineitem_scheme , sep="|")
        orders_csv_reader = InputCSVDataset("tpc-h-small", "orders.tbl", order_scheme , sep="|")

        lineitem = task_graph.new_input_reader_node(lineitem_csv_reader, {'localhost':8}, batch_func = lineitem_filter)
        orders = task_graph.new_input_reader_node(orders_csv_reader, {'localhost':8}, batch_func = orders_filter)

    else:
        lineitem_csv_reader = InputCSVDataset("tpc-h-csv", "lineitem/lineitem.tbl.1", lineitem_scheme , sep="|", stride = 128 * 1024 * 1024)
        orders_csv_reader = InputCSVDataset("tpc-h-csv", "orders/orders.tbl.1", order_scheme , sep="|", stride = 128 * 1024 * 1024)
        lineitem_csv_reader.get_csv_attributes(16 * workers)
        orders_csv_reader.get_csv_attributes(8 * workers)
        lineitem = task_graph.new_input_reader_node(lineitem_csv_reader, {ip:16 for ip in ips[:workers]}, batch_func = lineitem_filter)
        orders = task_graph.new_input_reader_node(orders_csv_reader, {ip:8 for ip in ips[:workers]}, batch_func = orders_filter)

elif sys.argv[2] == "parquet":
    if sys.argv[1] == "small":
        raise Exception("not implemented")
    else:

        lineitem_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","lineitem.parquet",columns=['l_shipdate','l_commitdate','l_shipmode','l_receiptdate','l_orderkey'], filters= [('l_shipmode', 'in', ['SHIP','MAIL']),('l_receiptdate','<',compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s")), ('l_receiptdate','>=',compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s"))])
        orders_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","orders.parquet",columns = ['o_orderkey','o_orderpriority'])

        lineitem = task_graph.new_input_reader_node(lineitem_parquet_reader, {ip:8 for ip in ips[:workers]}, batch_func = lineitem_filter_parquet)
        orders = task_graph.new_input_reader_node(orders_parquet_reader, {ip:8 for ip in ips[:workers]}, batch_func = orders_filter_parquet)
      

join_executor = PolarJoinExecutor(left_on="o_orderkey",right_on="l_orderkey", batch_func=batch_func)
output_stream = task_graph.new_non_blocking_node({0:orders,1:lineitem},None,join_executor, {ip:4 for ip in ips[:workers]}, {0:"o_orderkey", 1:"l_orderkey"})
agg_executor = AggExecutor()
agged = task_graph.new_blocking_node({0:output_stream}, None, agg_executor, {'localhost':1}, {0:None})



task_graph.create()
start = time.time()
task_graph.run_with_fault_tolerance()
print("total time ", time.time() - start)

print(ray.get(agged.to_pandas.remote()))
