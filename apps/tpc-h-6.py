import ray
import sys
import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.dataset import InputCSVDataset, InputMultiParquetDataset
from pyquokka.sql import AggExecutor

import pandas as pd
import pyarrow.compute as compute
import redis
r = redis.Redis(host="localhost", port=6800, db=0)
r.flushall()
task_graph = TaskGraph()

ips = ['localhost', '172.31.11.134', '172.31.15.208', '172.31.11.188']
workers = 1

lineitem_scheme = ["l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice", 
"l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct",
"l_shipmode","l_comment", "null"]


def lineitem_filter(df):
    
    filtered_df = df.filter(compute.and_(compute.and_(compute.greater(df["l_shipdate"], compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s")), compute.greater_equal(df["l_discount"], 0.05)), compute.and_(compute.less_equal(df["l_discount"],0.07), compute.less(df["l_quantity"], 24)))).to_pandas()
    return pd.DataFrame([ (filtered_df.l_extendedprice * filtered_df.l_discount).sum()],columns=['sum'])

def lineitem_filter_parquet(df):
    
    filtered_df = df.to_pandas()
    return pd.DataFrame([ (filtered_df.l_extendedprice * filtered_df.l_discount).sum()],columns=['sum'])

if sys.argv[2] == "csv":
    if sys.argv[1] == "small":
        lineitem_csv_reader = InputCSVDataset("tpc-h-small", "lineitem.tbl", lineitem_scheme , sep="|")
        lineitem = task_graph.new_input_reader_node(lineitem_csv_reader, {'localhost':16}, batch_func = lineitem_filter)
    else:
        lineitem_csv_reader = InputCSVDataset("tpc-h-csv", "lineitem/lineitem.tbl.1", lineitem_scheme , sep="|")
        lineitem = task_graph.new_input_reader_node(lineitem_csv_reader, {ip:16 for ip in ips[:workers]}, batch_func=lineitem_filter)

elif sys.argv[2] == "parquet":
    if sys.argv[1] == "small":
        raise Exception("not implemented")
    else:
        lineitem_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","lineitem.parquet",columns=["l_discount","l_extendedprice"], filters = [('l_shipdate','>',compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s")), ('l_discount','>=',0.05), ('l_discount','<=',0.07),('l_quantity','<',24)])
        lineitem = task_graph.new_input_reader_node(lineitem_parquet_reader,{ip:8 for ip in ips[:workers]}, batch_func=lineitem_filter_parquet)

agg_executor = AggExecutor()
agged = task_graph.new_blocking_node({0:lineitem},None, agg_executor, {'localhost':1}, {0:None})
task_graph.create()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)
print(ray.get(agged.to_pandas.remote()))
