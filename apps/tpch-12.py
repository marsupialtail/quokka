import sys
import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.executors import AggExecutor, PolarJoinExecutor
from pyquokka.dataset import InputS3CSVDataset, InputMultiParquetDataset
import ray
import polars
import pyarrow as pa
import pyarrow.compute as compute
from pyquokka.target_info import BroadcastPartitioner, HashPartitioner, TargetInfo
from schema import * 
from pyquokka.utils import LocalCluster, QuokkaClusterManager

import sqlglot

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
    lineitem = task_graph.new_input_reader_node(lineitem_csv_reader)
    orders = task_graph.new_input_reader_node(orders_csv_reader)

elif sys.argv[1] == "parquet":
    lineitem_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","lineitem.parquet",columns=['l_shipdate','l_commitdate','l_shipmode','l_receiptdate','l_orderkey'], filters= [('l_shipmode', 'in', ['SHIP','MAIL']),('l_receiptdate','<',compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s")), ('l_receiptdate','>=',compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s"))])
    orders_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","orders.parquet",columns = ['o_orderkey','o_orderpriority'])
    lineitem = task_graph.new_input_reader_node(lineitem_parquet_reader, batch_func = lineitem_filter_parquet)
    orders = task_graph.new_input_reader_node(orders_parquet_reader, batch_func = orders_filter_parquet)
      

join_executor = PolarJoinExecutor(left_on="o_orderkey",right_on="l_orderkey")

if sys.argv[1] == "csv":
    output_stream = task_graph.new_non_blocking_node({0:orders,1:lineitem},join_executor,
        source_target_info={0:TargetInfo(partitioner = HashPartitioner("o_orderkey"), 
                                        predicate = None,
                                        projection = ["o_orderkey", "o_orderpriority"],
                                        batch_funcs = []), 
                            1:TargetInfo(partitioner = HashPartitioner("l_orderkey"),
                                        predicate = sqlglot.parse_one("l_shipmode IN ('MAIL','SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and \
            l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1995-01-01'"),
                                        projection = ["l_orderkey","l_shipmode"],
                                        batch_funcs = [])})
else:
    output_stream = task_graph.new_non_blocking_node({0:orders,1:lineitem},join_executor,
        source_target_info={0:TargetInfo(partitioner = HashPartitioner("o_orderkey"), 
                                        predicate = None,
                                        projection = None,
                                        batch_funcs = []), 
                            1:TargetInfo(partitioner = HashPartitioner("l_orderkey"),
                                        predicate = sqlglot.parse_one("l_commitdate < l_receiptdate and l_shipdate < l_commitdate "),
                                        projection = ["l_orderkey","l_shipmode"],
                                        batch_funcs = [])})
agg_executor = AggExecutor()
agged = task_graph.new_blocking_node({0:output_stream},  agg_executor, ip_to_num_channel={cluster.leader_private_ip: 1}, 
    source_target_info={0:TargetInfo(
        partitioner = BroadcastPartitioner(),
        predicate = None,
        projection = None,
        batch_funcs = [batch_func]
    )})

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(agged.to_pandas())
