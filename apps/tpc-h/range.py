from pyquokka.df import * 
from pyquokka.utils import LocalCluster, QuokkaClusterManager
from pyquokka.dataset import InputSortedEC2ParquetDataset, InputEC2CoPartitionedSortedParquetDataset
import pyarrow.compute as compute
from pyquokka.target_info import BroadcastPartitioner, HashPartitioner, TargetInfo
import pyarrow as pa
import pyarrow.compute as compute
import numpy as np
from pyquokka.executors import Executor
import polars

class SortedAsofExecutor(Executor):
    def __init__(self, time_col_trades = 'time', time_col_quotes = 'time', symbol_col_trades = 'symbol', symbol_col_quotes = 'symbol') -> None:
        self.trade_state = None
        self.quote_state = None
        self.join_state = None
        self.time_col_trades = time_col_trades
        self.time_col_quotes = time_col_quotes
        self.symbol_col_trades = symbol_col_trades
        self.symbol_col_quotes = symbol_col_quotes

    def execute(self,batches,stream_id, executor_id):    
        batch = polars.concat(batches)
        if stream_id == 0:
            if self.trade_state is None:
                self.trade_state = batch
            else:
                self.trade_state.vstack(batch, in_place = True)
        else:
            if self.quote_state is None:
                self.quote_state = batch
            else:
                self.quote_state.vstack(batch, in_place = True)

        if self.trade_state is None or self.quote_state is None or len(self.trade_state) == 0 or len(self.quote_state) == 0:
            return

        joinable_trades = self.trade_state.filter(self.trade_state["time"] < self.quote_state["time"][-1])
        if len(joinable_trades) == 0:
            return
        self.trade_state =  self.trade_state.filter(self.trade_state["time"] >= self.quote_state["time"][-1])
        joinable_quotes = self.quote_state.filter(self.quote_state["time"] <= joinable_trades["time"][-1])
        if len(joinable_quotes) == 0:
            return
        self.quote_state = self.quote_state.filter(self.quote_state["time"] >= joinable_quotes["time"][-1])

        result = joinable_trades.join_asof(joinable_quotes, on = "time", by = "symbol")
        print(len(result))
        # return result
        
    
    def done(self, executor_id):
        return self.trade_state.join_asof(self.quote_state, on = "time", by = "symbol")

executor = SortedAsofExecutor()

manager = QuokkaClusterManager()
cluster = manager.get_cluster_from_json("config.json")
#cluster = LocalCluster()

task_graph = TaskGraph(cluster, io_per_node=2, exec_per_node=2)

trades_parquet_reader = InputSortedEC2ParquetDataset("quokka-asof-parquet","trades","time",columns=['time','symbol'])
quotes_parquet_reader = InputEC2CoPartitionedSortedParquetDataset("quokka-asof-parquet","quotes","time", columns = ['time','symbol','bsize'])
channel_bounds = trades_parquet_reader.get_bounds(8)
quotes_parquet_reader.get_bounds(8, channel_bounds)

trades = task_graph.new_input_reader_node(trades_parquet_reader)
quotes = task_graph.new_input_reader_node(quotes_parquet_reader)

output_stream = task_graph.new_blocking_node({0:trades,1:quotes}, executor,
    source_target_info={0:TargetInfo(partitioner = PassThroughPartitioner(), 
                                    predicate = sqlglot.exp.TRUE,
                                    projection = ['time','symbol'],
                                    batch_funcs = []), 
                        1:TargetInfo(partitioner = PassThroughPartitioner(),
                                    predicate = sqlglot.exp.TRUE,
                                    projection = ['time','symbol','bsize'],
                                    batch_funcs = [])})

# agg_executor = AggExecutor(["l_shipmode"], {"high_sum": "sum", "low_sum":"sum"}, False)

# agged = task_graph.new_blocking_node({0:output_stream},  agg_executor, ip_to_num_channel={cluster.leader_private_ip: 1}, 
#     source_target_info={0:TargetInfo(
#         partitioner = BroadcastPartitioner(),
#         predicate = None,
#         projection = None,
#         batch_funcs = [batch_func]
#     )})

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)

# print(agged.to_pandas())