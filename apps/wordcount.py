import pyarrow.compute as compute
import pyarrow as pa
import pandas as pd
from quokka_runtime import TaskGraph
from sql import UDFExecutor, AggExecutor
import ray

def udf(x):
    da = compute.list_flatten(compute.ascii_split_whitespace(x["text"]))
    c = da.value_counts().flatten()
    return pa.Table.from_arrays([c[0], c[1]], names=["word","count"]).to_pandas().set_index("word")

def partition_key2(data, source_channel, target_channel):

    if source_channel == target_channel:
        return data
    else:
        return None

task_graph = TaskGraph()
words = task_graph.new_input_csv("wordcount-input","1.txt",["text"],{'localhost':8}, sep="|")
udf_exe = UDFExecutor(udf)
output = task_graph.new_non_blocking_node({0:words},None,udf_exe,{"localhost":8},{0:partition_key2})
agg = AggExecutor(fill_value=0)
result = task_graph.new_blocking_node({0:output}, None, agg, {"localhost":1}, {0:None})

print(ray.get(result.to_pandas.remote()))
