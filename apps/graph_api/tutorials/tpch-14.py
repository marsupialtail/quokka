"""
This is a Quokka TaskGraph API implementation of the TPC-H 14 query.
Note: This query is still a work in progress. 
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

def batch_func(df):
    df = df.with_columns(polars.when(df["p_type"].str.starts_with("PROMO")).then(df['l_extendedprice'] * (1 - df['l_discount'])).otherwise(0).alias("promo_revenue"))
    df = df.with_columns((df["l_extendedprice"] * (1 - df["l_discount"])).alias("revenue"))
    return df


if sys.argv[1] == "csv":

    lineitem_csv_reader = InputDiskCSVDataset("/Users/EA/Desktop/Quokka_Research/Test_Datasets/tpc-h-public/lineitem.tbl", header = True, sep = "|", stride=16 * 1024 * 1024)
    part_csv_reader = InputDiskCSVDataset("/Users/EA/Desktop/Quokka_Research/Test_Datasets/tpc-h-public/part.tbl", header = True, sep = "|", stride=16 * 1024 * 1024)
    # lineitem_csv_reader = InputS3CSVDataset("tpc-h-csv", lineitem_scheme , key = "lineitem/lineitem.tbl.1", sep="|", stride = 128 * 1024 * 1024)
    # orders_csv_reader = InputS3CSVDataset("tpc-h-csv",  order_scheme , key ="orders/orders.tbl.1",sep="|", stride = 128 * 1024 * 1024)
    lineitem = task_graph.new_input_reader_node(lineitem_csv_reader)
    part = task_graph.new_input_reader_node(part_csv_reader, stage=-1)

else:
    raise Exception("Please specify input data type (e.g. csv).")

join_executor = BuildProbeJoinExecutor(left_on="l_partkey",right_on="p_partkey")

if sys.argv[1] == "csv":
    output_stream = task_graph.new_non_blocking_node({0:lineitem,1:part},join_executor,
        source_target_info={0:TargetInfo(partitioner = HashPartitioner("l_partkey"), 
                                        predicate = sqlglot.parse_one("l_shipdate between date '1995-09-01' and date '1995-09-30'"),
                                        projection = ["l_partkey", "l_extendedprice", "l_discount"],
                                        batch_funcs = []), 
                            1:TargetInfo(partitioner = HashPartitioner("p_partkey"),
                                        predicate = None,
                                        projection = ["p_partkey", "p_type"],
                                        batch_funcs = [])})

agg_executor = SQLAggExecutor([], None, "100 * sum(revenue) / sum(promo_revenue)")

agged = task_graph.new_blocking_node({0:output_stream},  agg_executor, placement_strategy = SingleChannelStrategy(), 
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

print(ray.get(qc.dataset_manager.to_df.remote(agged)))
