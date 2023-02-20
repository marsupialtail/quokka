import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.executors import JoinExecutor, CountExecutor
from pyquokka.target_info import TargetInfo, PassThroughPartitioner, HashPartitioner
from pyquokka.placement_strategy import SingleChannelStrategy
from pyquokka.dataset import InputDiskJSONDataset
import sqlglot
import pandas as pd
import pyarrow as pa
import ray

from pyquokka.utils import LocalCluster, QuokkaClusterManager

manager = QuokkaClusterManager()
cluster = LocalCluster()

task_graph = TaskGraph(cluster)


schema_a = pa.schema([
    pa.field('key_a', pa.int64()),
    pa.field('val1_a', pa.float64()),
    pa.field('val2_a', pa.float64())
])

schema_b = pa.schema([
    pa.field('key_b', pa.int64()),
    pa.field('val1_b', pa.float64()),
    pa.field('val2_b', pa.float64())
])


a_reader = InputDiskJSONDataset("a.json", schema=schema_a)
b_reader = InputDiskJSONDataset("b.json", schema=schema_b)

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

import json
a = json.read_json("a.json")
b = json.read_json("b.json")
a = a.to_pandas()
b = b.to_pandas()
print(len(a.merge(b,left_on="key_a", right_on = "key_b", how="inner")))
