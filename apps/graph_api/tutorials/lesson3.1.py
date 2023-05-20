"""
This is a Quokka TaskGraph API implementation of the TPC-H 4 query. 
"""

import sys
import time
from pyquokka import QuokkaContext
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.executors import SQLAggExecutor, BuildProbeJoinExecutor
from pyquokka.dataset import InputDiskCSVDataset, InputS3CSVDataset, InputParquetDataset

import pyarrow.compute as compute
from pyquokka.target_info import BroadcastPartitioner, HashPartitioner, TargetInfo
from pyquokka.placement_strategy import * 
from pyquokka.utils import LocalCluster, QuokkaClusterManager
import polars
import ray
import sqlglot

# manager = QuokkaClusterManager()
# cluster = manager.get_cluster_from_json("config.json")
cluster = LocalCluster()
qc = QuokkaContext(cluster)
qc.set_config("memory_limit", 0.01)

task_graph = TaskGraph(qc)

if sys.argv[1] == "csv":

    lineitem_csv_reader = InputDiskCSVDataset("/Users/EA/Desktop/Quokka_Research/Test_Datasets/tpc-h-public/lineitem.tbl", header = True, sep = "|", stride=16 * 1024 * 1024)
    orders_csv_reader = InputDiskCSVDataset("/Users/EA/Desktop/Quokka_Research/Test_Datasets/tpc-h-public/orders.tbl", header = True, sep = "|", stride=16 * 1024 * 1024)
    # lineitem_csv_reader = InputS3CSVDataset("tpc-h-csv", lineitem_scheme , key = "lineitem/lineitem.tbl.1", sep="|", stride = 128 * 1024 * 1024)
    # orders_csv_reader = InputS3CSVDataset("tpc-h-csv",  order_scheme , key ="orders/orders.tbl.1",sep="|", stride = 128 * 1024 * 1024)
    lineitem = task_graph.new_input_reader_node(lineitem_csv_reader, stage = -1)
    orders = task_graph.new_input_reader_node(orders_csv_reader)

elif sys.argv[1] == "parquet":
    lineitem_parquet_reader = InputParquetDataset("tpc-h-parquet","lineitem.parquet",columns=['l_shipdate','l_commitdate','l_shipmode','l_receiptdate','l_orderkey'], filters= [('l_shipmode', 'in', ['SHIP','MAIL']),('l_receiptdate','<',compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s")), ('l_receiptdate','>=',compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s"))])
    orders_parquet_reader = InputParquetDataset("tpc-h-parquet","orders.parquet",columns = ['o_orderkey','o_orderpriority'])
    lineitem = task_graph.new_input_reader_node(lineitem_parquet_reader)
    orders = task_graph.new_input_reader_node(orders_parquet_reader)
      

join_executor = BuildProbeJoinExecutor(left_on="o_orderkey",right_on="l_orderkey", how="semi")

if sys.argv[1] == "csv":
    output_stream = task_graph.new_non_blocking_node({0:orders,1:lineitem},join_executor,
        source_target_info={0:TargetInfo(partitioner = HashPartitioner("o_orderkey"), 
                                        predicate = sqlglot.parse_one("o_orderdate between date '1993-07-01' and date '1993-09-30'"),
                                        projection = ["o_orderkey", "o_orderpriority"],
                                        batch_funcs = []), 
                            1:TargetInfo(partitioner = HashPartitioner("l_orderkey"),
                                        predicate = sqlglot.parse_one("l_commitdate < l_receiptdate"),
                                        projection = ["l_orderkey"],
                                        batch_funcs = [])})
else:
    output_stream = task_graph.new_non_blocking_node({0:orders,1:lineitem},join_executor,
            source_target_info={0:TargetInfo(partitioner = HashPartitioner("o_orderkey"), 
                                        predicate = sqlglot.parse_one("o_orderdate between date '1993-07-01' and date '1993-09-30'"),

                                        projection = ["o_orderkey", "o_orderpriority"],
                                        batch_funcs = []), 
                            1:TargetInfo(partitioner = HashPartitioner("l_orderkey"),
                                        predicate = sqlglot.parse_one("l_commitdate < l_receiptdate"),
                                        projection = ["l_orderkey"],
                                        batch_funcs = [])})

agg_executor = SQLAggExecutor(["o_orderpriority"], [("o_orderpriority", "asc")], "count(*) as order_count")

agged = task_graph.new_blocking_node({0:output_stream}, agg_executor, placement_strategy = SingleChannelStrategy(), 
    source_target_info={0:TargetInfo(
        partitioner = BroadcastPartitioner(),
        predicate = None,
        projection = None,
        batch_funcs = []
    )})

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(ray.get(qc.dataset_manager.to_df.remote(agged)))