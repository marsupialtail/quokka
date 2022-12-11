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
                    self.trades = batch.select(["time","symbol","price"])
                else:
                    self.trades.vstack(batch.select(["time","symbol","price"]), in_place = True)
        # quotes
        elif stream_id == 1:
            for batch in batches:
                if self.quotes is None:
                    self.quotes = batch.select(["time","symbol","bid"])
                else:
                    self.quotes.vstack(batch.select(["time","symbol","bid"]), in_place = True)
        else:
            raise Exception

    def done(self,executor_id):
        return self.trades.lazy().sort('time').join_asof(self.quotes.lazy().sort('time'), 
            left_on = self.time_col_trades, right_on = self.time_col_quotes, by_left = self.symbol_col_trades, by_right = self.symbol_col_quotes).collect()

executor = AsofBlockingExecutor()
grouped_quotes = quotes.groupby("symbol").agg({"*":"count"}).collect()
print(grouped_quotes)
# grouped_trades = trades.groupby("symbol")
# grouped_trades.cogroup(grouped_quotes, executor, ["time","symbol","bid","price"]).count()