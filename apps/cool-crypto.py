from pyquokka.quokka_runtime import TaskGraph
from pyquokka.utils import LocalCluster
from pyquokka.executors import Executor
import time
import ray
from web3 import Web3, HTTPProvider
import pandas as pd
import redis
import pickle
cluster = LocalCluster()

# sync reserve0 reserve1
sync_topic = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"

swap_topic = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"

class Web3HTTPSource:
    def __init__(self, start_block, refresh_period=0.1) -> None:
        self.latest_block = start_block
        self.refresh_period = refresh_period
    
    def set_num_channels(self, num_channels):
        assert num_channels == 1
        self.num_channels = 1
    
    def get_next_batch(self, mapper_id, pos=None):
        assert self.num_channels == 1
        w3 = Web3(HTTPProvider('https://eth-mainnet.alchemyapi.io/v2/pY5kxua2ah1CGVFU5p4LIlF9OB5ftbtC'))
        r = redis.Redis('localhost',6800)
        while True:
            time.sleep(self.refresh_period)
            events = pd.DataFrame(w3.eth.getLogs({'fromBlock': self.latest_block + 1, 'toBlock': "latest", 'topics':[sync_topic]}))
            events["reserve0"] = events.apply(lambda x: int( x['data'][-128:-64], 16), axis=1)
            events["reserve1"] = events.apply(lambda x: int( x['data'][-64:], 16), axis=1)
            events.apply(lambda x: r.set(x['address'], pickle.dumps((x["reserve0"], x["reserve1"]))), axis=1)
            yield None, None


class Web3Executor(Executor):
    def __init__(self) -> None:
        pass
    def execute(self,batches,stream_id, executor_id):
        results = []
        for batch in batches:
            assert type(batch) == int
            
            w3 = Web3(HTTPProvider('https://eth-mainnet.alchemyapi.io/v2/pY5kxua2ah1CGVFU5p4LIlF9OB5ftbtC'))
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
reader = Web3HTTPSource()
numbers = task_graph.new_input_reader_node(reader)

executor = Web3Executor()
sum = task_graph.new_blocking_node({0:numbers},executor)

task_graph.create()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(sum.to_list())
