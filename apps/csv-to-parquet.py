import sys
import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.executors import OutputS3ParquetFastExecutor
from pyquokka.dataset import InputS3CSVDataset
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


lineitem_csv_reader = InputS3CSVDataset("tpc-h-csv", lineitem_scheme , key = "lineitem/lineitem.tbl.1", sep="|", stride = 128 * 1024 * 1024)
lineitem = task_graph.new_input_reader_node(lineitem_csv_reader)

output_executor = OutputS3ParquetFastExecutor("quokka-sorted-lineitem","lineitem")
output_stream = task_graph.new_non_blocking_node({0:lineitem},output_executor,ip_to_num_channel={i:1 for i in list(cluster.private_ips.values())})

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)
