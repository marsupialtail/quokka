import time
from pyquokka import QuokkaContext
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.executors import JoinExecutor, CountExecutor
from pyquokka.target_info import TargetInfo, PassThroughPartitioner, HashPartitioner
from pyquokka.placement_strategy import SingleChannelStrategy
from pyquokka.dataset import InputDiskCSVDataset
import sqlglot
import pandas as pd
import ray

from pyquokka.utils import LocalCluster, QuokkaClusterManager

cluster = LocalCluster()
qc = QuokkaContext(cluster)
task_graph = TaskGraph(qc)

a_reader = InputDiskCSVDataset("a.csv", header = True, stride =  1024)
b_reader = InputDiskCSVDataset("b.csv", header = True ,  stride =  1024)
a = task_graph.new_input_reader_node(a_reader)
b = task_graph.new_input_reader_node(b_reader)

join_executor = JoinExecutor(left_on="key_a", right_on = "key_b")
joined = task_graph.new_non_blocking_node({0:a,1:b},join_executor,
    source_target_info={0:TargetInfo(partitioner = HashPartitioner("key_a"), 
                                    predicate = sqlglot.exp.TRUE,
                                    projection = ["key_a"],
                                    batch_funcs = []), 
                        1:TargetInfo(partitioner = HashPartitioner("key_b"),
                                    predicate = sqlglot.exp.TRUE,
                                    projection = ["key_b"],
                                    batch_funcs = [])})
count_executor = CountExecutor()
count = task_graph.new_blocking_node({0:joined},count_executor, placement_strategy= SingleChannelStrategy(),
    source_target_info={0:TargetInfo(partitioner = PassThroughPartitioner(),
                                    predicate = sqlglot.exp.TRUE,
                                    projection = None,
                                    batch_funcs = [])})

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(ray.get(count.to_df.remote()))

a = pd.read_csv("a.csv",names=["key","val1","val2"])
b = pd.read_csv("b.csv",names=["key","val1","val2"])
print(len(a.merge(b,on="key",how="inner")))
