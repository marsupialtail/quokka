import time
import ray
from web3 import Web3, HTTPProvider
import polars
from tqdm import tqdm
import concurrent.futures
from pyquokka.executors.sql_executors import OutputExecutor
from pyquokka import QuokkaContext
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.executors import SQLAggExecutor, BuildProbeJoinExecutor
from pyquokka.dataset import InputDiskCSVDataset, InputS3CSVDataset, InputParquetDataset

import pyarrow.compute as compute
from pyquokka.target_info import BroadcastPartitioner, HashPartitioner, TargetInfo, PassThroughPartitioner
from pyquokka.placement_strategy import * 
from pyquokka.utils import LocalCluster, QuokkaClusterManager
import pyarrow as pa

def decode_int24(a):
    value = int(a.hex(),16)
    l = len(a.hex()) - 3
    return -(value & (0x8 * (16 ** l) )) | (value & (0x8 * (16 ** l) -1 ))

class LiquidityBuilder:

    def __init__(self, mint_topic, burn_topic):
        self.liquidity_state = {}
        assert type(mint_topic) == str and type(burn_topic) == str, "topics must be strings"
        assert mint_topic[:-2] == "0x" and burn_topic[:-2] == "0x", "topics must be hex strings, start with 0x"
        self.mint_topic = mint_topic
        self.burn_topic = burn_topic

    def execute(self,batches, stream_id, executor_id):
        def do_row(address, amount, low_tick, high_tick, is_mint = True):
            prev_low = self.liquidity_state[address, low_tick]
            prev_hi = self.liquidity_state[address, high_tick]
            prev_low = 0 if prev_low is None else int(prev_low)
            prev_hi = 0 if prev_hi is None else int(prev_hi)
            if is_mint:
                new_low = prev_low + amount
                new_hi = prev_hi - amount
            else:
                new_low = prev_low - amount
                new_hi = prev_hi + amount
            if new_low == 0:
                del self.liquidity_state[address, low_tick]
            else:
                self.liquidity_state[address, low_tick] = new_low
            if new_hi == 0:
                del self.liquidity_state[address, high_tick]
            else:
                self.liquidity_state[address, high_tick] = new_hi
        
        batch = polars.from_arrow(pa.concat_tables(batches))

        mint_events = batch.filter(polars.col("topics").list.contains(polars.lit(int(self.mint_topic, 16).to_bytes(32, "big"))))
        burn_events = batch.filter(polars.col("topics").list.contains(polars.lit(int(self.burn_topic, 16).to_bytes(32, "big"))))

        burn_events['low_tick'] = burn_events.apply(lambda x: decode_int24(x['topics'][2]),axis=1)
        burn_events['high_tick'] = burn_events.apply(lambda x: decode_int24(x['topics'][3]),axis=1)
        burn_events['amount'] = burn_events.apply(lambda x: int(x['data'][-192:-128], 16), axis=1)
        burn_events.apply(lambda x: do_row(x["address"], x["amount"], x["low_tick"], x["high_tick"], is_mint = False), axis = 1)

        mint_events['low_tick'] = mint_events.apply(lambda x: decode_int24(x['topics'][2]),axis=1)
        mint_events['high_tick'] = mint_events.apply(lambda x: decode_int24(x['topics'][3]),axis=1)
        mint_events['amount'] = mint_events.apply(lambda x: int(x['data'][-192:-128], 16), axis=1)
        mint_events.apply(lambda x: do_row(x["address"], x["amount"], x["low_tick"], x["high_tick"], is_mint = True), axis = 1)

        print("synced up to", batch['blockNumber'].max())
        
    
    def done(self,executor_id):
        return
        

class Web3LogDataset:

    def __init__(self, endpoint, start = None, end = None, topics = None) -> None:
        self.w3 = Web3(HTTPProvider(endpoint))
        self.start = start
        self.end = end
        self.topics = topics
        self.executor = None

    # we are going to range partition stuff here instead of stride partition
    def get_own_state(self, num_channels):
        self.num_channels = num_channels
        # get each channel's resposnbile range
        channel_range = (self.end - self.start) // num_channels
        channel_infos = {}
        for channel in range(num_channels):
            channel_infos[channel] = []
            my_start = self.start + channel * channel_range
            my_end = my_start + channel_range if channel != num_channels - 1 else self.end
            for pos in range(my_start, my_end, 2000):
                channel_infos[channel].append((pos, pos + 2000))
        return channel_infos


    def execute(self, mapper_id, block_range):

        # you should now calculate the wait time to make sure you only send at most four requests per second
        start, end = block_range
        try:
            a = self.w3.eth.getLogs({'fromBlock': start, 'toBlock': end, 'topics':self.topics})
        except:
            time.sleep(1)
            a = self.w3.eth.getLogs({'fromBlock': start, 'toBlock': end, 'topics':self.topics})
        return None, polars.from_records([dict(i) for i in a])

class Web3BlocksDataset:

    def __init__(self, endpoint, start = None, end = None) -> None:
        self.w3 = Web3(HTTPProvider(endpoint))
        self.start = start
        self.end = end

    # we are going to range partition stuff here instead of stride partition
    def get_own_state(self, num_channels):
        self.num_channels = num_channels
        # get each channel's resposnbile range
        channel_range = (self.end - self.start) // num_channels
        channel_infos = {}
        for channel in range(num_channels):
            channel_infos[channel] = []
            my_start = self.start + channel * channel_range
            my_end = my_start + channel_range if channel != num_channels - 1 else self.end
            for pos in range(my_start, my_end, 2000):
                channel_infos[channel].append((pos, pos + 2000))
        return channel_infos


    def execute(self, mapper_id, block_range):

        # you should now calculate the wait time to make sure you only send at most four requests per second

        start, end = block_range
        records = []
        for block in range(start, end):
            try:
                a = self.w3.eth.getBlock(block)
            except:
                time.sleep(1)
                a = self.w3.eth.getBlock(block)
            records.append(dict(a))
        return None, polars.from_records(records)

cluster = LocalCluster()
qc = QuokkaContext(cluster, 2, 1)
task_graph = TaskGraph(qc)

v3_mint_topic = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
v3_burn_topic = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"
v3_swap_topic = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
# arb_log_reader = Web3LogDataset('https://arb-mainnet.g.alchemy.com/v2/OJggh8NWc0Zb5lVIZQnwBeOe6GJ_i5w-', start = 0, end = 97575993, topics = [[v3_mint_topic, v3_burn_topic, v3_swap_topic]])
arb_blocks_reader = Web3BlocksDataset('https://arb-mainnet.g.alchemy.com/v2/OJggh8NWc0Zb5lVIZQnwBeOe6GJ_i5w-', start = 0, end = 97575993)
logs = task_graph.new_input_reader_node(arb_blocks_reader, stage = 0)
output_executor = OutputExecutor("arb-blocks", "parquet", row_group_size=100000)
agged = task_graph.new_blocking_node({0:logs}, output_executor,  
    source_target_info={0:TargetInfo(
        partitioner = PassThroughPartitioner(),
        predicate = None,
        projection = None,
        batch_funcs = []
    )})

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(ray.get(qc.dataset_manager.to_df.remote(agged)))