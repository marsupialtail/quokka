import sys
import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.sql import AggExecutor, PolarJoinExecutor
from pyquokka.dataset import InputDiskCSVDataset
import ray

from pyquokka.utils import LocalCluster, QuokkaClusterManager

manager = QuokkaClusterManager()
cluster = LocalCluster()

task_graph = TaskGraph(cluster)

a_reader = InputDiskCSVDataset("a.csv", ["key","val1","val2"] , stride =  1024)
b_reader = InputDiskCSVDataset("b.csv",  ["key","val1","val2"] ,  stride =  1024)
a = task_graph.new_input_reader_node(a_reader)
b = task_graph.new_input_reader_node(b_reader)

join_executor = PolarJoinExecutor(on="key")
output_stream = task_graph.new_non_blocking_node({0:a,1:b},join_executor,partition_key_supplied={0:"key", 1:"key"})
#agg_executor = AggExecutor()
#agged = task_graph.new_blocking_node({0:output_stream},  agg_executor, ip_to_num_channel={cluster.leader_private_ip: 1}, partition_key_supplied={0:None})

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)

#print(ray.get(agged.to_pandas.remote()))
