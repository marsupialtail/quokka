import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.executors import AggExecutor, PolarJoinExecutor, CountExecutor
from pyquokka.dataset import InputDiskCSVDataset
import ray
import pandas as pd

from pyquokka.utils import LocalCluster, QuokkaClusterManager

manager = QuokkaClusterManager()
cluster = LocalCluster()

task_graph = TaskGraph(cluster)

a_reader = InputDiskCSVDataset("a.csv", ["key","val1","val2"] , stride =  1024)
b_reader = InputDiskCSVDataset("b.csv",  ["key","val1","val2"] ,  stride =  1024)
a = task_graph.new_input_reader_node(a_reader)
b = task_graph.new_input_reader_node(b_reader)

join_executor = PolarJoinExecutor(on="key")
joined = task_graph.new_non_blocking_node({0:a,1:b},join_executor,partition_key_supplied={0:"key", 1:"key"})
count_executor = CountExecutor()
count = task_graph.new_blocking_node({0:joined},count_executor)

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(count.to_list())

a = pd.read_csv("a.csv",names=["key","val1","val2"])
b = pd.read_csv("b.csv",names=["key","val1","val2"])
print(len(a.merge(b,on="key",how="inner")))
