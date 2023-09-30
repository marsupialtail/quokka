from pyquokka.quokka_runtime import TaskGraph
from pyquokka.dataset import InputRestPostAPIDataset, InputRestGetAPIDataset
from pyquokka.utils import LocalCluster
from pyquokka.executors import Executor
from pyquokka.target_info import TargetInfo, PassThroughPartitioner
from pyquokka.placement_strategy import SingleChannelStrategy
import sqlglot
import time
import polars
import os
import ray
cluster = LocalCluster()

#Replace with your Alchemy API key:
apiKey = os.environ["ALCHEMY_KEY"]


class AddExecutor(Executor):
    def __init__(self) -> None:
        self.state = None
    def execute(self,batches,stream_id, channel):
        for batch in batches:
            self.state = polars.from_arrow(batch) if self.state is None else self.state.vstack(polars.from_arrow(batch))

    def done(self,channel):
        return self.state


task_graph = TaskGraph(cluster)
# arguments = [{'fromBlock': hex(1000000), 'toBlock': hex(1000100)},
#              {'fromBlock': hex(1000100), 'toBlock': hex(1000200)},
#              {'fromBlock': hex(1000200), 'toBlock': hex(1000300)},
#              {'fromBlock': hex(1000300), 'toBlock': hex(1000400)}]
# arguments = [{'jsonrpc': '2.0', 'method': 'eth_getLogs', 'params': [i], 'id': 0} for i in arguments]
# reader = RestPostAPIDataset("https://eth-mainnet.alchemyapi.io/v2/"+apiKey, arguments, 
#                             {'Content-Type': 'application/json'}, 
#                             ["address","blockHash","blockNumber","data","logIndex","removed","topics","transactionHash","transactionIndex"]
#                             , batch_size = 1)
# numbers = task_graph.new_input_reader_node(reader)

arguments = [
    "{}.json".format(i) for i in range(30000000,30100000)
]

reader = InputRestGetAPIDataset("https://hacker-news.firebaseio.com/v0/item/", arguments, ["by","id","title","url","score","time"])
numbers = task_graph.new_input_reader_node(reader)

executor = AddExecutor()
sum = task_graph.new_blocking_node({0:numbers},executor, placement_strategy = SingleChannelStrategy(), source_target_info={0:TargetInfo(partitioner = PassThroughPartitioner(), 
                                    predicate = sqlglot.exp.TRUE,
                                    projection = None,
                                    batch_funcs = [])})

task_graph.create()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)

result = ray.get(sum.to_df.remote())
print(result)
# result.write_csv("output.csv")