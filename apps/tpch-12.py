import sys
sys.path.append("/home/ubuntu/quokka/")
import datetime
import time
from quokka_runtime import TaskGraph
from sql import AggExecutor, PolarJoinExecutor
import pandas as pd
import ray
import os
import polars
import pyarrow as pa
import pyarrow.compute as compute

task_graph = TaskGraph()

def batch_func(df):
    df["high"] = ((df["o_orderpriority"] == "1-URGENT") | (df["o_orderpriority"] == "2-HIGH")).astype(int)
    df["low"] = ((df["o_orderpriority"] != "1-URGENT") & (df["o_orderpriority"] != "2-HIGH")).astype(int)
    result = df.groupby("l_shipmode").agg({'high':['sum'],'low':['sum']})
    return result

lineitem_scheme = ["l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice", 
"l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct",
"l_shipmode","l_comment", "null"]
order_scheme = ["o_orderkey", "o_custkey","o_orderstatus","o_totalprice","o_orderdate","o_orderpriority","o_clerk",
"o_shippriority","o_comment", "null"]

orders_filter = lambda x: polars.from_arrow(x.select(["o_orderkey","o_orderpriority"]))
lineitem_filter = lambda x: polars.from_arrow(x.filter(compute.and_(compute.and_(compute.and_(compute.is_in(x["l_shipmode"],value_set = pa.array(["SHIP","MAIL"])), compute.less(x["l_commitdate"], x["l_receiptdate"])), compute.and_(compute.less(x["l_shipdate"], x["l_commitdate"]), compute.greater_equal(x["l_receiptdate"], compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s")))), compute.less(x["l_receiptdate"], compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s")))).select(["l_orderkey","l_shipmode"]))
orders_filter_parquet = lambda x: polars.from_arrow(x)
lineitem_filter_parquet = lambda x: polars.from_arrow(x.filter(compute.and_(compute.less(x["l_commitdate"], x["l_receiptdate"]), compute.less(x["l_shipdate"], x["l_commitdate"]))).select(["l_orderkey","l_shipmode"]))
if sys.argv[2] == "csv":
    if sys.argv[1] == "small":
        print("DOING  SMALL")
        orders = task_graph.new_input_csv("tpc-h-small","orders.tbl",order_scheme,{'localhost':8, '172.31.16.185':8},batch_func=orders_filter, sep="|")
        lineitem = task_graph.new_input_csv("tpc-h-small","lineitem.tbl",lineitem_scheme,{'localhost':8, '172.31.16.185':8},batch_func=lineitem_filter, sep="|")
    else:
        #orders = task_graph.new_input_csv("tpc-h-csv","orders/orders.tbl.1",order_scheme,{'localhost':8, '172.31.16.185':8},batch_func=orders_filter, sep="|")
        #lineitem = task_graph.new_input_csv("tpc-h-csv","lineitem/lineitem.tbl.1",lineitem_scheme,{'localhost':8, '172.31.16.185':8},batch_func=lineitem_filter, sep="|")
        orders = task_graph.new_input_csv("tpc-h-csv","orders/orders.tbl.1",order_scheme,{'localhost':8},batch_func=orders_filter, sep="|")
        lineitem = task_graph.new_input_csv("tpc-h-csv","lineitem/lineitem.tbl.1",lineitem_scheme,{'localhost':16},batch_func=lineitem_filter, sep="|")
elif sys.argv[2] == "parquet":
    if sys.argv[1] == "small":
        raise Exception("not implemented")
    else:
#        lineitem = task_graph.new_input_multiparquet("tpc-h-parquet","lineitem.parquet", {'localhost':8},columns=['l_shipdate','l_commitdate','l_shipmode','l_receiptdate','l_orderkey'], filters= [('l_shipmode', 'in', ['SHIP','MAIL']),('l_receiptdate','<',compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s")), ('l_receiptdate','>=',compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s"))], batch_func=lineitem_filter_parquet)
#        orders = task_graph.new_input_multiparquet("tpc-h-parquet","orders.parquet",{'localhost':4},columns = ['o_orderkey','o_orderpriority'], batch_func = orders_filter_parquet)
        #lineitem = task_graph.new_input_multiparquet("tpc-h-parquet","lineitem.parquet", {'localhost':8,'172.31.11.134':8,'172.31.15.208':8,'172.31.10.96':8},columns=['l_shipdate','l_commitdate','l_shipmode','l_receiptdate','l_orderkey'], filters= [('l_shipmode', 'in', ['SHIP','MAIL']),('l_receiptdate','<',compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s")), ('l_receiptdate','>=',compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s"))], batch_func=lineitem_filter_parquet)
        #orders = task_graph.new_input_multiparquet("tpc-h-parquet","orders.parquet",{'localhost':4,'172.31.11.134':4,'172.31.15.208':4,'172.31.10.96':4},columns = ['o_orderkey','o_orderpriority'], batch_func = orders_filter_parquet)
        lineitem = task_graph.new_input_multiparquet("tpc-h-parquet","lineitem.parquet", {'localhost':8,'172.31.11.134':8},columns=['l_shipdate','l_commitdate','l_shipmode','l_receiptdate','l_orderkey'], filters= [('l_shipmode', 'in', ['SHIP','MAIL']),('l_receiptdate','<',compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s")), ('l_receiptdate','>=',compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s"))], batch_func=lineitem_filter_parquet)
        orders = task_graph.new_input_multiparquet("tpc-h-parquet","orders.parquet",{'localhost':4,'172.31.11.134':4},columns = ['o_orderkey','o_orderpriority'], batch_func = orders_filter_parquet)

join_executor = PolarJoinExecutor(left_on="o_orderkey",right_on="l_orderkey", batch_func=batch_func)
#output_stream = task_graph.new_non_blocking_node({0:orders,1:lineitem},None,join_executor,{'localhost':4, '172.31.11.134':4,'172.31.15.208':4,'172.31.10.96':4}, {0:"o_orderkey", 1:"l_orderkey"})
output_stream = task_graph.new_non_blocking_node({0:orders,1:lineitem},None,join_executor,{'localhost':4, '172.31.11.134':4}, {0:"o_orderkey", 1:"l_orderkey"})
#output_stream = task_graph.new_non_blocking_node({0:orders,1:lineitem},None,join_executor,{'localhost':4,'172.31.16.185':4}, {0:"o_orderkey", 1:"l_orderkey"})
agg_executor = AggExecutor()
agged = task_graph.new_blocking_node({0:output_stream}, None, agg_executor, {'localhost':1}, {0:None})


task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(ray.get(agged.to_pandas.remote()))

#import pdb;pdb.set_trace()
