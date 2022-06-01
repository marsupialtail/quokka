import time
import sys
sys.path.append("/home/ubuntu/quokka/pyquokka")
import pyarrow.compute as compute
import pyarrow as pa
import pandas as pd
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.dataset import InputS3CSVDataset
from pyquokka.executors import UDFExecutor, AggExecutor
import ray
from pyquokka.utils import LocalCluster, QuokkaClusterManager

manager = QuokkaClusterManager()
cluster = manager.get_cluster_from_json("config.json")

def udf2(x):
    da = compute.list_flatten(compute.ascii_split_whitespace(x["text"]))
    c = da.value_counts().flatten()
    return pa.Table.from_arrays([c[0], c[1]], names=["word","count"]).to_pandas().set_index("word")

task_graph = TaskGraph(cluster)

reader = InputS3CSVDataset("wordcount-input",["text"],  sep="|", stride = 128 * 1024 * 1024)
words = task_graph.new_input_reader_node(reader, batch_func = udf2)

agg = AggExecutor(fill_value=0)
intermediate = task_graph.new_non_blocking_node({0:words},agg)
result = task_graph.new_blocking_node({0:intermediate}, agg, ip_to_num_channel={cluster.leader_private_ip: 1}, partition_key_supplied={0:None})
task_graph.create()
start = time.time()
task_graph.run()
print(time.time() - start)
print(result.to_pandas())
