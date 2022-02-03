import sys
sys.path.append("/home/ubuntu/quokka/")
import datetime
import time
from quokka_runtime import TaskGraph
from sql import AggExecutor, JoinExecutor
import ray
import os
task_graph = TaskGraph()

def batch_func(results):
    aggs = []
    for df in results:
        df["high"] = ((df["o_orderpriority"] == "1-URGENT") | (df["o_orderpriority"] == "2-HIGH")).astype(int)
        df["low"] = ((df["o_orderpriority"] != "1-URGENT") & (df["o_orderpriority"] != "2-HIGH")).astype(int)
        aggs.append(df.groupby("l_shipmode").agg({'high':['sum'],'low':['sum']}))
    for i in range(1,len(aggs)):
        aggs[0] = aggs[0].add(aggs[i],fill_value=0)
    return [aggs[0]]

lineitem_scheme = ["l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice", 
"l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct",
"l_shipmode","l_comment", "null"]
order_scheme = ["o_orderkey", "o_custkey","o_orderstatus","o_totalprice","o_orderdate","o_orderpriority","o_clerk",
"o_shippriority","o_comment", "null"]

orders_filter = lambda x: x[["o_orderkey","o_orderpriority"]]
lineitem_filter = lambda x: x[((x.l_shipmode == "MAIL") | (x.l_shipmode == "SHIP")) & (x.l_commitdate < x.l_receiptdate) 
& (x.l_shipdate < x.l_commitdate) & (x.l_receiptdate >= datetime.date(1994,1,1)) & (x.l_receiptdate < datetime.date(1995,1,1))][["l_orderkey","l_shipmode"]]

#orders = task_graph.new_input_csv("tpc-h-csv","orders/orders.tbl.1",order_scheme,8,batch_func=orders_filter, sep="|")
#lineitem = task_graph.new_input_csv("tpc-h-csv","lineitem/lineitem.tbl.1",lineitem_scheme,8,batch_func=lineitem_filter, sep="|")
orders = task_graph.new_input_csv("tpc-h-small","orders.tbl",order_scheme,8,batch_func=orders_filter, sep="|")
lineitem = task_graph.new_input_csv("tpc-h-small","lineitem.tbl",lineitem_scheme,8,batch_func=lineitem_filter, sep="|")
join_executor = JoinExecutor(left_on="o_orderkey",right_on="l_orderkey", batch_func=batch_func, left_primary = True)
output_stream = task_graph.new_stateless_node({0:orders,1:lineitem},join_executor,4, {0:"o_orderkey", 1:"l_orderkey"})
agg_executor = AggExecutor()
agged = task_graph.new_stateless_node({0:output_stream}, agg_executor, 1, {0:None})

task_graph.initialize()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)
#import pdb;pdb.set_trace()
