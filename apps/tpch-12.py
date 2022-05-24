import sys
import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.sql import AggExecutor, PolarJoinExecutor
from pyquokka.dataset import InputS3CSVDataset, InputMultiParquetDataset
import ray
import polars
import pyarrow as pa
import pyarrow.compute as compute
from schema import * 
from pyquokka.utils import LocalCluster, QuokkaClusterManager

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


if sys.argv[1] == "csv":
    lineitem_csv_reader = InputS3CSVDataset("tpc-h-csv", lineitem_scheme , key = "lineitem/lineitem.tbl.1", sep="|", stride = 128 * 1024 * 1024)
    orders_csv_reader = InputS3CSVDataset("tpc-h-csv",  order_scheme , key ="orders/orders.tbl.1",sep="|", stride = 128 * 1024 * 1024)
    lineitem = task_graph.new_input_reader_node(lineitem_csv_reader, batch_func = lineitem_filter)
    orders = task_graph.new_input_reader_node(orders_csv_reader, batch_func = orders_filter)

elif sys.argv[1] == "parquet":
    lineitem_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","lineitem.parquet",columns=['l_shipdate','l_commitdate','l_shipmode','l_receiptdate','l_orderkey'], filters= [('l_shipmode', 'in', ['SHIP','MAIL']),('l_receiptdate','<',compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s")), ('l_receiptdate','>=',compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s"))])
    orders_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","orders.parquet",columns = ['o_orderkey','o_orderpriority'])
    lineitem = task_graph.new_input_reader_node(lineitem_parquet_reader, batch_func = lineitem_filter_parquet)
    orders = task_graph.new_input_reader_node(orders_parquet_reader, batch_func = orders_filter_parquet)
      

join_executor = PolarJoinExecutor(left_on="o_orderkey",right_on="l_orderkey", batch_func=batch_func)
output_stream = task_graph.new_non_blocking_node({0:orders,1:lineitem},join_executor, ip_to_num_channel = {ip: 4 for ip in list(cluster.private_ips.values())}, partition_key_supplied={0:"o_orderkey", 1:"l_orderkey"})
agg_executor = AggExecutor()
agged = task_graph.new_blocking_node({0:output_stream},  agg_executor, ip_to_num_channel={cluster.leader_private_ip: 1}, partition_key_supplied={0:None})

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(ray.get(agged.to_pandas.remote()))
