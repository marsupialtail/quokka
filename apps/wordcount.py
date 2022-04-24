import time
import sys
sys.path.append("/home/ubuntu/quokka/pyquokka")
import pyarrow.compute as compute
import pyarrow as pa
import pandas as pd
from quokka_runtime import TaskGraph
from dataset import InputMultiCSVDataset

from sql import UDFExecutor, AggExecutor
import ray
import redis
r = redis.Redis(host="localhost", port=6800, db=0)
r.flushall()

ips = ['localhost', '172.31.11.134', '172.31.15.208', '172.31.11.188']
workers = 2

def udf2(x):
    da = compute.list_flatten(compute.ascii_split_whitespace(x["text"]))
    c = da.value_counts().flatten()
    return pa.Table.from_arrays([c[0], c[1]], names=["word","count"]).to_pandas().set_index("word")

def partition_key1(data, source_channel, target_channel):

    if source_channel // 8 == target_channel:
        return data
    else:
        return None

task_graph = TaskGraph()

reader = InputMultiCSVDataset("wordcount-input", None, ["text"],  sep="|", stride = 128 * 1024 * 1024)
words = task_graph.new_input_reader_node(reader, {ip:8 for ip in ips[:workers]}, batch_func = udf2)

agg = AggExecutor(fill_value=0)
intermediate = task_graph.new_non_blocking_node({0:words}, None, agg, {ip:1 for ip in ips[:workers]}, {0:partition_key1})
result = task_graph.new_blocking_node({0:intermediate}, None, agg, {"localhost":1}, {0:None})
task_graph.create()
start = time.time()
task_graph.run_with_fault_tolerance()
print(time.time() - start)
print(ray.get(result.to_pandas.remote()))
