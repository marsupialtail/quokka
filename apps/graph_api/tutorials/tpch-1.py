"""
This is a Quokka TaskGraph API implementation of the TPC-H 1 query.
Since the TaskGraph deals directly with batches of data, it is somewhat
tricky to implement averages directly in this interface. Therefore,
any averages computed for the end result of this query represent
the averages of the averages of the different batches of data processed.
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
    df = df.with_columns((df["l_extendedprice"] * (1 - df["l_discount"]) * (1 + df["l_tax"])).alias("tax_price"))
    result = df.to_arrow().group_by(["l_returnflag", "l_linestatus"]).aggregate([("actual_price","sum"), ("tax_price", "sum"), ("l_quantity", "sum"), ("l_extendedprice", "sum"), ("l_quantity", "hash_mean"), ("l_extendedprice", "hash_mean"), ("l_discount", "hash_mean"), ("l_extendedprice", "hash_count")])
    return polars.from_arrow(result)


if sys.argv[1] == "csv":

    lineitem_csv_reader = InputDiskCSVDataset("/Users/EA/Desktop/Quokka_Research/Test_Datasets/tpc-h-public/lineitem.tbl", header = True, sep = "|", stride=16 * 1024 * 1024)
    lineitem = task_graph.new_input_reader_node(lineitem_csv_reader, stage=-1)
else:
    raise Exception("Please specify input data type (e.g. csv).")

agg_executor = SQLAggExecutor(["l_returnflag", "l_linestatus"], [("l_returnflag", "asc"), ("l_linestatus", "asc")], 
                                "sum(l_quantity_sum) as sum_qty, sum(l_extendedprice_sum) as sum_base_price, sum(actual_price_sum) as sum_disc_price, sum(tax_price_sum) as sum_charge, avg(l_quantity_mean) as avg_qty, avg(l_extendedprice_mean) as avg_price, avg(l_discount_mean) as avg_disc, sum(l_extendedprice_count) as count_order")

agged = task_graph.new_blocking_node({0:lineitem}, agg_executor, placement_strategy = SingleChannelStrategy(), 
    source_target_info={0:TargetInfo(
        partitioner = BroadcastPartitioner(),
        predicate = sqlglot.parse_one("l_shipdate <= date '1998-09-02'"),
        projection = None,
        batch_funcs = [batch_func]
    )})

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(ray.get(qc.dataset_manager.to_df.remote(agged)))