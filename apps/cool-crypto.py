from pyquokka.quokka_runtime import TaskGraph
from pyquokka.utils import LocalCluster
from pyquokka.executors import Executor
import time
import ray
from web3 import Web3, HTTPProvider

cluster = LocalCluster()

# this dataset will generate a sequence of numbers, from 0 to limit. Channel 
class SimpleDataset:
    def __init__(self, lower_limit, upper_limit,step_size = 1) -> None:
        
        self.lower_limit = lower_limit
        self.upper_limit = upper_limit
        self.step_size = step_size
        self.num_channels = None

    def set_num_channels(self, num_channels):
        self.num_channels = num_channels

    def get_next_batch(self, mapper_id, pos=None):
        # let's ignore the keyword pos = None, which is only relevant for fault tolerance capabilities.
        assert self.num_channels is not None
        curr_number = self.lower_limit + mapper_id * self.step_size
        while curr_number < self.upper_limit:
            yield curr_number, curr_number
            curr_number += self.num_channels * self.step_size

class Web3Executor(Executor):
    def __init__(self) -> None:
        pass
    def execute(self,batches,stream_id, executor_id):
        results = []
        for batch in batches:
            assert type(batch) == int
            
            w3 = Web3(HTTPProvider('https://eth-mainnet.alchemyapi.io/v2/ENDPOINT'))
            abi = '[{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"}]'
            myContract = w3.eth.contract(address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", abi=abi)
            response = myContract.functions.balanceOf("0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640").call(block_identifier=batch)
            results.append(response)
            print(response)
        return results
            
    def done(self,executor_id):
        print("I am executor ", executor_id, " my sum is ", self.sum)
        return self.sum

task_graph = TaskGraph(cluster)
reader = SimpleDataset(14840000, 14860000, 1000)
numbers = task_graph.new_input_reader_node(reader)

executor = Web3Executor()
sum = task_graph.new_blocking_node({0:numbers},executor)

task_graph.create()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(sum.to_list())
