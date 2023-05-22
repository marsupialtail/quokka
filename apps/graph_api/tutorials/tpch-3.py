"""
This is a Quokka TaskGraph API implementation of the TPC-H 3 query.
Compared to Quokka's DataStream API, it is somewhat tricky to implement
the SQL "LIMIT" clause directly in Quokka's TaskGraph API, the results
of the query disregard the limit of 10 set in the original TPC-H 3 query.
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
    df = df.with_columns((df["l_extendedprice"] * (1 - df["l_discount"])).alias("actual_price"))
    result = df.to_arrow().group_by(["l_orderkey", "o_orderdate", "o_shippriority"]).aggregate([("actual_price","sum")])
    return polars.from_arrow(result)


if sys.argv[1] == "csv":

    lineitem_csv_reader = InputDiskCSVDataset("/Users/EA/Desktop/Quokka_Research/Test_Datasets/tpc-h-public/lineitem.tbl", header = True, sep = "|", stride=16 * 1024 * 1024)
    orders_csv_reader = InputDiskCSVDataset("/Users/EA/Desktop/Quokka_Research/Test_Datasets/tpc-h-public/orders.tbl", header = True, sep = "|", stride=16 * 1024 * 1024)
    customer_csv_reader = InputDiskCSVDataset("/Users/EA/Desktop/Quokka_Research/Test_Datasets/tpc-h-public/customer.tbl", header = True, sep = "|", stride=16 * 1024 * 1024)
    # lineitem_csv_reader = InputS3CSVDataset("tpc-h-csv", lineitem_scheme , key = "lineitem/lineitem.tbl.1", sep="|", stride = 128 * 1024 * 1024)
    # orders_csv_reader = InputS3CSVDataset("tpc-h-csv",  order_scheme , key ="orders/orders.tbl.1",sep="|", stride = 128 * 1024 * 1024)
    lineitem = task_graph.new_input_reader_node(lineitem_csv_reader, stage=-1)
    orders = task_graph.new_input_reader_node(orders_csv_reader, stage = 0)
    customer = task_graph.new_input_reader_node(customer_csv_reader, stage = -1)
else:
    raise Exception("Please specify input data type (e.g. csv).")
      
join_executor1 = BuildProbeJoinExecutor(left_on="o_custkey",right_on="c_custkey")

if sys.argv[1] == "csv":
    first_joined = task_graph.new_non_blocking_node({0:orders,1:customer},join_executor1,
        source_target_info={0:TargetInfo(partitioner = HashPartitioner("o_custkey"), 
                                        predicate = sqlglot.parse_one("o_orderdate <= date '1995-03-14'"),
                                        projection = ["o_orderkey", "o_orderdate", "o_shippriority", "o_custkey"],
                                        batch_funcs = []), 
                            1:TargetInfo(partitioner = HashPartitioner("c_custkey"),
                                        predicate = sqlglot.parse_one("c_mktsegment = 'BUILDING'"),
                                        projection = ["c_custkey"],
                                        batch_funcs = [])})

join_executor2 = BuildProbeJoinExecutor(left_on="o_orderkey",right_on="l_orderkey", key_to_keep="right")

if sys.argv[1] == "csv":
    output_stream = task_graph.new_non_blocking_node({0:first_joined,1:lineitem},join_executor2,
        source_target_info={0:TargetInfo(partitioner = HashPartitioner("o_orderkey"), 
                                        predicate = None,
                                        projection = None,
                                        batch_funcs = []), 
                            1:TargetInfo(partitioner = HashPartitioner("l_orderkey"),
                                        predicate = sqlglot.parse_one("l_shipdate >= date '1995-03-16'"),
                                        projection = ["l_orderkey", "l_extendedprice", "l_discount"],
                                        batch_funcs = [])})


agg_executor = SQLAggExecutor(["l_orderkey", "o_orderdate", "o_shippriority"], [("revenue", "desc"), ("o_orderdate", "asc")], "sum(actual_price_sum) as revenue")

agged = task_graph.new_blocking_node({0:output_stream}, agg_executor, placement_strategy = SingleChannelStrategy(), 
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