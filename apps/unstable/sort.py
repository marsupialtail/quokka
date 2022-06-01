import sys
import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.sql import MergeSortedExecutor, OutputCSVExecutor
from pyquokka.dataset import InputCSVDataset, SortPhase2Dataset
import ray
import polars
from pyquokka.utils import LocalCluster, QuokkaClusterManager
from schema import * 
manager = QuokkaClusterManager()
cluster = manager.get_cluster_from_json("config.json")

task_graph = TaskGraph(cluster)

lineitem_filter = lambda x: polars.from_arrow(x).sort('l_partkey')

def partition_key(data, source_channel,  target_channel):
    
    interval = (200000000 // 16)
    #interval = (200000 // 4)
    range_start = interval * target_channel

    return data[ (data.l_partkey > range_start ) & (data.l_partkey <= range_start + interval) ]

# pass through filter.
def partition_key2(data, source_channel, target_channel):

    if source_channel == target_channel:
        return data
    else:
        return None

drop_null = lambda x: polars.from_arrow(x).drop("null").sort("l_partkey")

if sys.argv[1] == "csv":

    lineitem_csv_reader = InputCSVDataset("tpc-h-csv", "lineitem/lineitem.tbl.1", lineitem_scheme , sep="|")
    lineitem = task_graph.new_input_reader_node(lineitem_csv_reader, {ip:8 for ip in ips[:workers]}, batch_func = drop_null)

elif sys.argv[1] == "parquet":
    raise Exception("not implemented")

executor = MergeSortedExecutor("l_partkey", record_batch_rows = 2500000, length_limit = 10000000)
stream = task_graph.new_blocking_node({0:lineitem}, None, executor, {ip:4 for ip in ips[:workers]}, {0: partition_key})

task_graph.create()
start = time.time()
task_graph.run_with_fault_tolerance()
print("total time ", time.time() - start)
files = stream.to_dict()
print(files)

del task_graph

reader = SortPhase2Dataset(files,"l_partkey",2500000)
task_graph = TaskGraph()
stream = task_graph.new_input_reader_node(reader, {ip:4 for ip in ips[:workers]})
outputer = OutputCSVExecutor("quokka-sorted-lineitem","lineitem")
output = task_graph.new_blocking_node({0:stream}, None,outputer, {ip:4 for ip in ips[:workers]}, {0: partition_key2} )

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)
