import sys
import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.executors import AggExecutor, PolarJoinExecutor, StorageExecutor
from pyquokka.dataset import InputMultiParquetDataset
import ray
import polars
import pyarrow as pa
import pyarrow.compute as compute
from pyquokka.utils import LocalCluster, QuokkaClusterManager
from schema import * 

manager = QuokkaClusterManager()
cluster = manager.get_cluster_from_json("config.json")
#cluster = LocalCluster()

task_graph = TaskGraph(cluster)

def batch_func(df):
    df["high"] = ((df["o_orderpriority"] == "1-URGENT") | (df["o_orderpriority"] == "2-HIGH")).astype(int)
    df["low"] = ((df["o_orderpriority"] != "1-URGENT") & (df["o_orderpriority"] != "2-HIGH")).astype(int)
    result = df.groupby("l_shipmode").agg({'high':['sum'],'low':['sum']})
    return result

orders_filter = lambda x: polars.from_arrow(x.select(["o_orderkey","o_orderpriority"]))
lineitem_filter = lambda x: polars.from_arrow(x.filter(compute.and_(compute.and_(compute.and_(compute.is_in(x["l_shipmode"],value_set = pa.array(["SHIP","MAIL"])), compute.less(x["l_commitdate"], x["l_receiptdate"])), compute.and_(compute.less(x["l_shipdate"], x["l_commitdate"]), compute.greater_equal(x["l_receiptdate"], compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s")))), compute.less(x["l_receiptdate"], compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s")))).select(["l_orderkey","l_shipmode"]))
orders_filter_parquet = lambda x: polars.from_arrow(x)
lineitem_filter_parquet = lambda x: polars.from_arrow(x.filter(compute.and_(compute.less(x["l_commitdate"], x["l_receiptdate"]), compute.less(x["l_shipdate"], x["l_commitdate"]))).select(["l_orderkey","l_shipmode"]))

lineitem_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","lineitem.parquet",columns=['l_shipdate','l_commitdate','l_shipmode','l_receiptdate','l_orderkey'], filters= [('l_shipmode', 'in', ['SHIP','MAIL']),('l_receiptdate','<',compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s")), ('l_receiptdate','>=',compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s"))])
orders_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","orders.parquet",columns = ['o_orderkey','o_orderpriority'])

lineitem = task_graph.new_input_reader_node(lineitem_parquet_reader, batch_func = lineitem_filter_parquet)
orders = task_graph.new_input_reader_node(orders_parquet_reader, batch_func = orders_filter_parquet)

storage = StorageExecutor()
cached_lineitem = task_graph.new_blocking_node({0:lineitem},storage)
cached_orders = task_graph.new_blocking_node({0:orders},storage)


task_graph.create()
start = time.time()
task_graph.run()
load_time = time.time() - start

task_graph2 = TaskGraph(cluster)
lineitem = task_graph2.new_input_redis(cached_lineitem)
orders = task_graph2.new_input_redis(cached_orders)

join_executor = PolarJoinExecutor(left_on="o_orderkey",right_on="l_orderkey", batch_func=batch_func)
cached_joined = task_graph2.new_blocking_node({0:orders,1:lineitem},join_executor, partition_key_supplied={0:"o_orderkey", 1:"l_orderkey"})

task_graph2.create()
start = time.time()
task_graph2.run()
compute_time = time.time() - start

task_graph3 = TaskGraph(cluster)
joined = task_graph3.new_input_redis(cached_joined)
agg_executor = AggExecutor()
agged = task_graph3.new_blocking_node({0:joined}, agg_executor, ip_to_num_channel={cluster.leader_private_ip: 1}, partition_key_supplied={0:None})

task_graph3.create()
start = time.time()
task_graph3.run()
compute_time += time.time() - start

print("load time ", load_time)
print("compute time ", compute_time)

print(agged.to_pandas())
