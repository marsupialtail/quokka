import sys
sys.path.append("/home/ubuntu/quokka/")
import datetime
import time
from quokka_runtime import TaskGraph
from sql import AggExecutor
import ray
import os
task_graph = TaskGraph()

class CustomJoinExecutor:
    def __init__(self, on = None, left_on = None, right_on = None):
        self.state0 = []
        self.state1 = []
        self.temp_results = []
        if on is not None:
            assert left_on is None and right_on is None
            self.left_on = on
            self.right_on = on
        else:
            assert left_on is not None and right_on is not None
            self.left_on = left_on
            self.right_on = right_on
        self.agg_result = None

    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batch, stream_id, executor_id):
        results = []
        if stream_id == 0:
            if len(self.state1) > 0:
                results = [batch.merge(i,left_on = self.left_on, right_on = self.right_on ,how='inner',suffixes=('_a','_b')) for i in self.state1]
            self.state0.append(batch)
             
        elif stream_id == 1:
            if len(self.state0) > 0:
                results = [i.merge(batch,left_on = self.left_on, right_on = self.right_on ,how='inner',suffixes=('_a','_b')) for i in self.state0]
            self.state1.append(batch)
        
        if len(results) > 0:
            aggs = []
            for df in results:
                df["high"] = ((df["o_orderpriority"] == "1-URGENT") | (df["o_orderpriority"] == "2-HIGH")).astype(int)
                df["low"] = ((df["o_orderpriority"] != "1-URGENT") & (df["o_orderpriority"] != "2-HIGH")).astype(int)
                aggs.append(df.groupby("l_shipmode").agg({'high':['sum'],'low':['sum']}))
            for i in range(1,len(aggs)):
                aggs[0] = aggs[0].add(aggs[i],fill_value=0)
            if self.agg_result is None:
                self.agg_result = aggs[0]
            else:
                self.agg_result = self.agg_result.add(aggs[0],fill_value = 0)
            return None
    
    def done(self,executor_id):
        print(self.agg_result)
        print("done " + str(executor_id))
        return self.agg_result


lineitem_scheme = ["l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice", 
"l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct",
"l_shipmode","l_comment", "null"]
order_scheme = ["o_orderkey", "o_custkey","o_orderstatus","o_totalprice","o_orderdate","o_orderpriority","o_clerk",
"o_shippriority","o_comment", "null"]

orders_filter = lambda x: x[["o_orderkey","o_orderpriority"]]
lineitem_filter = lambda x: x[((x.l_shipmode == "MAIL") | (x.l_shipmode == "SHIP")) & (x.l_commitdate < x.l_receiptdate) 
& (x.l_shipdate < x.l_commitdate) & (x.l_receiptdate >= datetime.date(1994,1,1)) & (x.l_receiptdate < datetime.date(1995,1,1))][["l_orderkey","l_shipmode"]]

orders = task_graph.new_input_csv("tpc-h-small","orders.tbl",order_scheme,8,batch_func=orders_filter, sep="|")
lineitem = task_graph.new_input_csv("tpc-h-small","lineitem.tbl",lineitem_scheme,8,batch_func=lineitem_filter, sep="|")
join_executor = CustomJoinExecutor(left_on="o_orderkey",right_on="l_orderkey")
output_stream = task_graph.new_stateless_node({0:orders,1:lineitem},join_executor,4, {0:"o_orderkey", 1:"l_orderkey"})
agg_executor = AggExecutor()
agged = task_graph.new_stateless_node({0:output_stream}, agg_executor, 1, {0:None})
#output_executor = OutputCSVExecutor(4,"yugan","result")
#wrote = task_graph.new_stateless_node({0:output_stream},output_executor,4)

task_graph.initialize()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)
#import pdb;pdb.set_trace()
