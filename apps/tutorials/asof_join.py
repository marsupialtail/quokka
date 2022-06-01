from sys import prefix
import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.executors import GroupAsOfJoinExecutor, CountExecutor
from pyquokka.dataset import InputDiskCSVDataset, InputS3CSVDataset
#from schema import * 
from pyquokka.utils import LocalCluster, QuokkaClusterManager
import polars

# manager = QuokkaClusterManager()
# cluster = manager.get_cluster_from_json("config.json")
cluster = LocalCluster()

task_graph = TaskGraph(cluster)

# quote_reader = InputS3CSVDataset("quokka-asofjoin", ["time","symbol","seq","bid","ask","bsize","asize","is_nbbo"] , prefix="quotes", stride = 128 * 1024 * 1024, header = True)
# trades_reader = InputS3CSVDataset("quokka-asofjoin", ["time","symbol","size","price"] , prefix="trades", stride = 128 * 1024 * 1024, header = True)

quote_reader = InputDiskCSVDataset("test_quote2.csv", ["time","symbol","seq","bid","ask","bsize","asize","is_nbbo"] , stride =  1024 * 5, header = True)
trades_reader = InputDiskCSVDataset("test_trade2.csv",  ["time","symbol","size","price"] ,  stride =  1024 * 5, header = True)

quotes = task_graph.new_input_reader_node(quote_reader)
trades = task_graph.new_input_reader_node(trades_reader)

join_executor = GroupAsOfJoinExecutor(group_on="symbol", on="time")
output = task_graph.new_blocking_node({0:trades,1:quotes},join_executor, partition_key_supplied={0:"symbol", 1:"symbol"})

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)
result = output.to_pandas()
result.to_csv("result.csv")

quotes = polars.read_csv("test_quote2.csv")
trades = polars.read_csv("test_trade2.csv")
ref = trades.join_asof(quotes,by="symbol",on="time").sort("symbol").drop_nulls()
print(ref.sort("symbol"))

print((result["size"] - ref["size"]).sum())
