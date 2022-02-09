
import sys
sys.path.append("/home/ubuntu/quokka/")
import datetime
import time
from quokka_runtime import TaskGraph
from sql import AggExecutor
import ray
import pandas as pd
task_graph = TaskGraph()

# unless I was dreaming, and my dreams somehow manifested themselves in the EC2 instance execution logs, sometimes the results are wrong. Though it is extremely infrequent.

lineitem_scheme = ["l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice", 
"l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct",
"l_shipmode","l_comment", "null"]

def lineitem_filter(df):
    filtered_df = df[(df.l_shipdate > pd.to_datetime(datetime.date(1994,1,1))) & (df.l_discount >= 0.05) & (df.l_discount <= 0.07) & (df.l_quantity < 24)]
    #filtered_df = df[  (df.l_discount >= 0.05) & (df.l_discount <= 0.07) & (df.l_quantity < 24)]
    return pd.DataFrame([ (filtered_df.l_extendedprice * filtered_df.l_discount).sum()],columns=['sum'])

if sys.argv[1] == "small":
    lineitem = task_graph.new_input_csv("tpc-h-small","lineitem.tbl",lineitem_scheme,{'localhost':8,'172.31.16.185':8},batch_func=lineitem_filter, sep="|")
    #lineitem = task_graph.new_input_csv("tpc-h-small","lineitem.tbl",lineitem_scheme,{'localhost':8},batch_func=lineitem_filter, sep="|")
else:
    lineitem = task_graph.new_input_csv("tpc-h-csv","lineitem/lineitem.tbl.1",lineitem_scheme,{'localhost':8, '172.31.16.185':8},batch_func=lineitem_filter, sep="|")

agg_executor = AggExecutor()
agged = task_graph.new_stateless_node({0:lineitem}, agg_executor, {'localhost':1}, {0:None})

task_graph.initialize()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)
#import pdb;pdb.set_trace()
