import sys
sys.path.append("/home/ubuntu/quokka/")
import datetime
import time
from quokka_runtime import TaskGraph
from sql import AggExecutor
import ray
import pandas as pd
task_graph = TaskGraph()


lineitem_scheme = ["l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice", 
"l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct",
"l_shipmode","l_comment", "null"]

def lineitem_filter(df):
    filtered_df = df[(df.l_shipdate > "1994-01-01") & (df.l_discount >= 0.05) & (df.l_discount <= 0.07) & (df.l_quantity < 24)]
    filtered_df["product"] = filtered_df.l_extendedprice * filtered_df.l_discount
    return pd.DataFrame([filtered_df["product"].sum()],columns=['sum'])

lineitem = task_graph.new_input_csv("tpc-h-small","lineitem.tbl",lineitem_scheme,8,batch_func=lineitem_filter, sep="|")
agg_executor = AggExecutor()
agged = task_graph.new_stateless_node({0:lineitem}, agg_executor, 1, {0:None})

task_graph.initialize()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)
#import pdb;pdb.set_trace()
