"""
This is a Quokka TaskGraph API implementation of the TPC-H 19 query.
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
    df = df.with_columns((df["l_extendedprice"] * (1 - df["l_discount"])).alias("actual_price"))
    return df

part_first_condition = "p_size between 1 and 5 and p_brand = 'Brand#12' and p_container in ('SM BOX', 'SM CASE', 'SM PACK', 'SM PKG')"
part_second_condition = "p_size between 1 and 10 and p_brand = 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PACK', 'MED PKG')"
part_third_condition = "p_size between 1 and 15 and p_brand ='Brand#34' and p_container in ('LG BOX', 'LG CASE', 'LG PACK', 'LG PKG')"

lineitem_first_condition = "l_quantity between 1 and 30"
lineitem_second_condition = "l_shipinstruct = 'DELIVER IN PERSON'"
lineitem_third_condition = "l_shipmode in ('AIR', 'AIR REG')"

part_condition = part_first_condition + " or " + part_second_condition + " or " + part_third_condition
lineitem_condition = lineitem_first_condition + " and " + lineitem_second_condition + " and " + lineitem_third_condition

joined_predicate = part_first_condition + " and l_quantity between 1 and 11 or " + part_second_condition + " and l_quantity between 10 and 20 or " + part_third_condition + " and l_quantity between 20 and 30"
print(joined_predicate)

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
                                        predicate = sqlglot.parse_one(lineitem_condition),
                                        projection = None,
                                        batch_funcs = []), 
                            1:TargetInfo(partitioner = HashPartitioner("p_partkey"),
                                        predicate = sqlglot.parse_one(part_condition),
                                        projection = None,
                                        batch_funcs = [])})

agg_executor = SQLAggExecutor([], None, "sum(actual_price) as revenue")

agged = task_graph.new_blocking_node({0:output_stream}, agg_executor, placement_strategy = SingleChannelStrategy(), 
    source_target_info={0:TargetInfo(
        partitioner = BroadcastPartitioner(),
        predicate = sqlglot.parse_one(joined_predicate),
        projection = None,
        batch_funcs = [batch_func]
    )})

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(ray.get(qc.dataset_manager.to_df.remote(agged)))
