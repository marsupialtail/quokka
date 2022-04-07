import pandas as pd
import sys
sys.path.append("/home/ubuntu/quokka/")
import datetime
import time
from quokka_runtime import TaskGraph
from sql import AggExecutor, PolarJoinExecutor
from dataset import InputCSVDataset, InputMultiParquetDataset
import pyarrow.compute as compute
import os
import polars
import redis
r = redis.Redis(host="localhost", port=6800, db=0)
r.flushall()
task_graph = TaskGraph()

# aggregation push down might be interesting to think about
# this is not very good because we don't know the thing is actually sorted in l_order on lineitem

def batch_func1(results):
    return [i[["o_orderkey", "o_orderdate", "o_shippriority"]] for i in results]

def batch_func2(results):
    aggs = []
    for df in results:
        df["product"] = df["l_extendedprice"] * (1 - df["l_discount"])
        aggs.append(df.groupby(["l_orderkey", "o_orderdate", "o_shippriority"]).agg(revenue = ('product','sum')))
    for i in range(1,len(aggs)):
        aggs[0] = aggs[0].add(aggs[i],fill_value=0)
    
    return [aggs[0]]

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

if sys.argv[1] == "csv":
    
    lineitem_csv_reader = InputCSVDataset("tpc-h-csv", "lineitem/lineitem.tbl.1", lineitem_scheme , sep="|")
    orders_csv_reader = InputCSVDataset("tpc-h-csv", "orders/orders.tbl.1", lineitem_scheme , sep="|")
    customer_csv_reader = InputCSVDataset("tpc-h-csv", "customer/customer.tbl.1", lineitem_scheme , sep="|")

    lineitem = task_graph.new_input_reader_node(lineitem_csv_reader, {'localhost':8}, batch_func = lineitem_filter)
    orders = task_graph.new_input_reader_node(orders_csv_reader, {'localhost':8}, batch_func = orders_filter)
    customer = task_graph.new_input_reader_node(customer_csv_reader, {'localhost':8}, batch_func = customer_filter)

else:

    lineitem_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","lineitem.parquet", columns = ["l_orderkey","l_extendedprice","l_discount"], filters= [('l_shipdate','>',compute.strptime("1995-03-15",format="%Y-%m-%d",unit="s"))])
    orders_parquet_reader =InputMultiParquetDataset("tpc-h-parquet","orders.parquet", columns = ["o_orderkey","o_custkey","o_shippriority", "o_orderdate"], filters= [('o_orderdate','<',compute.strptime("1995-03-03",format="%Y-%m-%d",unit="s"))])
    customer_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","customer.parquet", columns = ["c_custkey"], filters= [("c_cmktsegment","==","BUILDING")])

    lineitem = task_graph.new_input_reader_node(lineitem_parquet_reader, {'localhost':8}, batch_func = lineitem_filter)
    orders = task_graph.new_input_reader_node(orders_parquet_reader, {'localhost':8}, batch_func = orders_filter)
    customer = task_graph.new_input_reader_node(customer_parquet_reader, {'localhost':8}, batch_func = customer_filter)
    

# join order picked by hand, might not be  the best one!
join_executor1 = PolarJoinExecutor(left_on = "c_custkey", right_on = "o_custkey",batch_func=batch_func1)
join_executor2 = PolarJoinExecutor(left_on="o_orderkey",right_on="l_orderkey",batch_func=batch_func2)
temp = task_graph.new_non_blocking_node({0:customer,1:orders},None, join_executor1,{'localhost':2,'172.31.16.185':2}, {0:"c_custkey", 1:"o_custkey"})
joined = task_graph.new_non_blocking_node({0:temp, 1: lineitem},None, join_executor2, {'localhost':2, '172.31.16.185':2}, {0: "o_orderkey", 1:"l_orderkey"})

agg_executor = AggExecutor(final_func=final_func)
agged = task_graph.new_blocking_node({0:joined}, None, agg_executor, {'localhost':1}, {0:None})

task_graph.initialize()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(ray.get(agged.to_pandas.remote()))
