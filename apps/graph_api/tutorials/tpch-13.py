"""
This is a Quokka TaskGraph API implementation of the TPC-H 13 query.
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

    orders_csv_reader = InputDiskCSVDataset("/Users/EA/Desktop/Quokka_Research/Test_Datasets/tpc-h-public/orders.tbl", header = True, sep = "|", stride=16 * 1024 * 1024)
    customer_csv_reader = InputDiskCSVDataset("/Users/EA/Desktop/Quokka_Research/Test_Datasets/tpc-h-public/customer.tbl", header = True, sep = "|", stride=16 * 1024 * 1024)
    # lineitem_csv_reader = InputS3CSVDataset("tpc-h-csv", lineitem_scheme , key = "lineitem/lineitem.tbl.1", sep="|", stride = 128 * 1024 * 1024)
    # orders_csv_reader = InputS3CSVDataset("tpc-h-csv",  order_scheme , key ="orders/orders.tbl.1",sep="|", stride = 128 * 1024 * 1024)
    orders = task_graph.new_input_reader_node(orders_csv_reader, stage = -1)
    customer = task_graph.new_input_reader_node(customer_csv_reader, stage = 0)
else:
    raise Exception("Please specify input data type (e.g. csv).")
      
join_executor1 = BuildProbeJoinExecutor(left_on="c_custkey",right_on="o_custkey", how="left")

if sys.argv[1] == "csv":
    first_joined = task_graph.new_non_blocking_node({0:customer,1:orders},join_executor1,
        source_target_info={0:TargetInfo(partitioner = HashPartitioner("c_custkey"), 
                                        predicate = None,
                                        projection = ["c_custkey"],
                                        batch_funcs = []), 
                            1:TargetInfo(partitioner = HashPartitioner("o_custkey"),
                                        predicate = sqlglot.parse_one("not o_comment::text like '%special%requests%'"),
                                        projection = ["o_custkey", "o_orderkey"],
                                        batch_funcs = [])})

agg_executor1 = SQLAggExecutor(["c_custkey"], None, "count(o_orderkey) as c_count")

agged1 = task_graph.new_non_blocking_node({0:first_joined}, agg_executor1, placement_strategy = SingleChannelStrategy(), 
    source_target_info={0:TargetInfo(
        partitioner = BroadcastPartitioner(),
        predicate = None,
        projection = None,
        batch_funcs = []
    )})

agg_executor2 = SQLAggExecutor(["c_count"], [("custdist", "desc"), ("c_count", "desc")], "count(*) as custdist")

agged2 = task_graph.new_blocking_node({0:agged1}, agg_executor2, placement_strategy = SingleChannelStrategy(), 
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

print(ray.get(qc.dataset_manager.to_df.remote(agged2)))