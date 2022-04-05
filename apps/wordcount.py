import time
import sys
sys.path.append("/home/ubuntu/quokka/")
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

def udf(x):
    da = compute.list_flatten(compute.ascii_split_whitespace(x.to_arrow()["text"]))
    c = da.value_counts().flatten()
    return pa.Table.from_arrays([c[0], c[1]], names=["word","count"]).to_pandas().set_index("word")

def udf2(x):
    da = compute.list_flatten(compute.ascii_split_whitespace(x["text"]))
    c = da.value_counts().flatten()
    return pa.Table.from_arrays([c[0], c[1]], names=["word","count"]).to_pandas().set_index("word")

def partition_key1(data, source_channel, target_channel):

    if source_channel // 16 == target_channel:
        return data
    else:
        return None
def partition_key2(data, source_channel, target_channel):

    if source_channel == target_channel:
        return data
    else:
        return None

task_graph = TaskGraph()

reader = InputMultiCSVDataset("wordcount-input", None, ["text"],  sep="|")

words = task_graph.new_input_reader_node(reader, {'localhost':16, '172.31.11.134':16,'172.31.15.208':16, '172.31.10.96':16})#, batch_func = udf2)

udf_exe = UDFExecutor(udf)
output = task_graph.new_non_blocking_node({0:words},None,udf_exe,{'localhost':16, '172.31.11.134':16,'172.31.15.208':16, '172.31.10.96':16},{0:partition_key2})
agg = AggExecutor(fill_value=0)
intermediate = task_graph.new_non_blocking_node({0:output}, None, agg, {'localhost':1, '172.31.11.134':1,'172.31.15.208':1, '172.31.10.96':1}, {0:partition_key1})
result = task_graph.new_blocking_node({0:intermediate}, None, agg, {"localhost":1}, {0:None})
task_graph.create()
start = time.time()
task_graph.run_with_fault_tolerance()
print(time.time() - start)
print(ray.get(result.to_pandas.remote()))
