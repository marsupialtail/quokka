from pyquokka.df import * 
from pyquokka.utils import LocalCluster, QuokkaClusterManager

import pyarrow as pa
import pyarrow.compute as compute
import numpy as np
from pyquokka.executors import Executor
import polars


manager = QuokkaClusterManager()
cluster = manager.get_cluster_from_json("config.json")

qc = QuokkaContext(cluster,4, 2)

quotes = qc.read_parquet("s3://quokka-asof-parquet/quotes/*")
trades = qc.read_parquet("s3://quokka-asof-parquet/trades/*")

class SortedAsofExecutor(Executor):
    def __init__(self, time_col_trades = 'time', time_col_quotes = 'time', symbol_col_trades = 'symbol', symbol_col_quotes = 'symbol') -> None:
        self.trade_dict = {}
        self.quote_dict = {}
        self.join_dict = {}
        self.time_col_trades = time_col_trades
        self.time_col_quotes = time_col_quotes
        self.symbol_col_trades = symbol_col_trades
        self.symbol_col_quotes = symbol_col_quotes

    def execute(self,batches,stream_id, executor_id):    # print("indicator", indicator)
        batch = polars.concat(batches, rechunk = True)
        splits = batch.partition_by(groups="symbol") #splits is a list of dataframes
        for split in splits:
            symbol = split["symbol"][0]
            if stream_id == 0:
                if symbol not in self.trade_dict:
                    self.trade_dict[symbol] = split
                else:
                    self.trade_dict[symbol].vstack(split, in_place= True)
                
            elif stream_id == 1:
                if symbol not in self.quote_dict:
                    self.quote_dict[symbol] = split
                else:
                    self.quote_dict[symbol].vstack(split, in_place= True)
            else:
                raise Exception

            if symbol not in self.trade_dict or symbol not in self.quote_dict:
                continue

            current_trade_state = self.trade_dict[symbol]
            current_quote_state = self.quote_dict[symbol]

            if len(current_trade_state) == 0 or len(current_quote_state) == 0:
                continue

            joinable_trades = current_trade_state.filter(current_trade_state["time"] < current_quote_state["time"][-1])
            if len(joinable_trades) == 0:
                continue
            remaining_trades = current_trade_state.filter(current_trade_state["time"] >= current_quote_state["time"][-1])
            joinable_quotes = current_quote_state.filter(current_quote_state["time"] <= joinable_trades["time"][-1])
            remaining_quotes = current_quote_state.filter(current_quote_state["time"] > joinable_trades["time"][-1])

            result = joinable_trades.join_asof(joinable_quotes, on = "time")
            if symbol not in self.join_dict:
                self.join_dict[symbol] = result
            else:
                self.join_dict[symbol].vstack(result, in_place= True)
            
            self.trade_dict[symbol] = remaining_trades
            self.quote_dict[symbol] = remaining_quotes
    
    def done(self, executor_id):
        unmatched_trades = self.trade_dict[list(self.trade_dict.keys())[0]][:0]
        for symbol in self.trade_dict:

            if symbol not in self.quote_dict or len(self.quote_dict[symbol]) == 0:
                unmatched_trades.vstack(self.trade_dict[symbol], in_place = True)
            else:
                result = self.trade_dict[symbol].join_asof(self.quote_dict[symbol], on = "time")
                if symbol not in self.join_dict:
                    self.join_dict[symbol] = result
                else:
                    self.join_dict[symbol].vstack(result, in_place= True)
        

class AsofBlockingExecutor(Executor):
    def __init__(self, time_col_trades = 'time', time_col_quotes = 'time', symbol_col_trades = 'symbol', symbol_col_quotes = 'symbol') -> None:
        self.trades = None
        self.quotes = None
        self.time_col_trades = time_col_trades
        self.time_col_quotes = time_col_quotes
        self.symbol_col_trades = symbol_col_trades
        self.symbol_col_quotes = symbol_col_quotes

    def execute(self,batches,stream_id, executor_id):
        # trades
        if stream_id == 0:
            for batch in batches:
                if self.trades is None:
                    self.trades = batch
                else:
                    self.trades.vstack(batch, in_place = True)
        # quotes
        elif stream_id == 1:
            for batch in batches:
                if self.quotes is None:
                    self.quotes = batch
                else:
                    self.quotes.vstack(batch, in_place = True)
        else:
            raise Exception

    def done(self,executor_id):
        return self.trades.lazy().sort('time').join_asof(self.quotes.lazy().sort('time'), 
            left_on = self.time_col_trades, right_on = self.time_col_quotes, by_left = self.symbol_col_trades, by_right = self.symbol_col_quotes).collect()

# executor = AsofBlockingExecutor()
executor = SortedAsofExecutor()
grouped_quotes = quotes.groupby("symbol") #.agg({"*":"count"}).collect()
#print(grouped_quotes)
grouped_trades = trades.groupby("symbol")
grouped_trades.cogroup(grouped_quotes, executor, ["time","symbol","bid","price"]).count()