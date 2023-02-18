from pyquokka.quokka_runtime import TaskGraph
from pyquokka.utils import LocalCluster
from pyquokka.executors import Executor
from pyquokka.target_info import TargetInfo, PassThroughPartitioner
from pyquokka.placement_strategy import SingleChannelStrategy
import sqlglot
import time
import polars
import ray
cluster = LocalCluster()

from web3 import Web3, HTTPProvider

#Replace with your Alchemy API key:
apiKey = "ukYQfHBOSIeUwipjO76QoCwst48yE528"


# this dataset will generate a sequence of numbers, from 0 to limit. 
class SimpleDataset:
    def __init__(self, arguments) -> None:
        
        self.arguments = arguments
        self.web3 = None

    def get_own_state(self, num_channels):
        channel_info = {}
        for channel in range(num_channels):
            channel_info[channel] = [self.arguments[i] for i in range(channel, len(self.arguments), num_channels)]
        return channel_info

    def execute(self, channel, state = None):

        if self.web3 is None:
           self.web3 = Web3(Web3.HTTPProvider('https://eth-mainnet.g.alchemy.com/v2/'+apiKey))
        
        logs = state(self.web3)
        df = polars.from_records([dict(k) for k in logs])
        print(len(df))
        return None, df
    
class AddExecutor(Executor):
    def __init__(self) -> None:
        self.state = None
    def execute(self,batches,stream_id, channel):
        for batch in batches:
            self.state = polars.from_arrow(batch) if self.state is None else self.state.vstack(polars.from_arrow(batch))

    def done(self,channel):
        return self.state


web3 = Web3(Web3.HTTPProvider('https://eth-mainnet.g.alchemy.com/v2/'+apiKey))
task_graph = TaskGraph(cluster)
arguments = [{'fromBlock': 1000000, 'toBlock': 1000100},
             {'fromBlock': 1000100, 'toBlock': 1000200},
             {'fromBlock': 1000200, 'toBlock': 1000300},
             {'fromBlock': 1000300, 'toBlock': 1000400}]
# you must capture the local i in the lambda.
arguments = [lambda x, i = i: x.eth.get_logs(i) for i in arguments]
reader = SimpleDataset(arguments)
numbers = task_graph.new_input_reader_node(reader)

executor = AddExecutor()
sum = task_graph.new_blocking_node({0:numbers},executor, placement_strategy = SingleChannelStrategy(), source_target_info={0:TargetInfo(partitioner = PassThroughPartitioner(), 
                                    predicate = sqlglot.exp.TRUE,
                                    projection = None,
                                    batch_funcs = [])},assume_sorted={0:True})

task_graph.create()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(ray.get(sum.to_df.remote()))
