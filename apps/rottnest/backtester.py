from pyquokka import QuokkaContext
from pyquokka.utils import LocalCluster, QuokkaClusterManager
from pyarrow.fs import S3FileSystem
from pyquokka.executors import Executor
import pyarrow.parquet as pq
import pyarrow as pa
import polars
import json
import numpy as np
from matplotlib import pyplot as plt

# def generate_random_alpha(start = 1548220019000, end = 1577006819000, num = 1000000):
def generate_random_alpha(start = 1548220019000, end = 1670006819000, num = 1000000):

    universe = json.load(open("universe.json","r"))
    symbols = universe["symbols"]
    timestamps = np.sort(np.random.randint(start, end, num))
    symbols = np.random.choice(symbols, num)
    direction = np.random.choice([-1,1], num)
    alphas = polars.from_dict({"timestamp": timestamps, "symbol": symbols, "direction": direction}).with_columns(polars.col("timestamp").cast(polars.UInt64()))
    exits = alphas.with_columns(polars.col("timestamp") + 3600 * 1000 * 5)
    alphas = alphas.vstack(exits).sort("timestamp")
    return alphas

manager = QuokkaClusterManager(key_name = "zihengw", key_location = "/home/ziheng/Downloads/zihengw.pem")
manager.start_cluster("config_2.json")
cluster = manager.get_cluster_from_json("config_2.json")
qc = QuokkaContext(cluster, 2, 2)

class Backtester(Executor):

    def __init__(self,alphas):
        self.state = {}
        self.current_biggest_timestamp = 0
        self.positions = {"cash": 0}
        self.alphas = alphas
        assert self.alphas.columns == ["timestamp", "symbol", "direction"]

    def execute(self,batches, stream_id, executor_id):
        batches = [polars.from_arrow(batch) for batch in batches]

        # each batch corresponds to one day. you can concat but then you lose the ability in the future
        # to do stuff between market close and open.
        dates = []
        daily_equity = []
        for batch in batches:

            assert set(["timestamp", "symbol", "price"]).issubset(set(batch.columns))
            timestamp = batch["timestamp"][-1]
            # assert batch["timestamp"].is_sorted(), batch["timestamp"]
            assert timestamp >= self.current_biggest_timestamp
            self.current_biggest_timestamp = timestamp

            # figure out the trades you can make
            # print(timestamp, self.alphas["timestamp"])
            viable_alphas = self.alphas.filter(polars.col("timestamp") <= timestamp)
            # these are alphas that you definitely didn't act on
            remaining_alphas = self.alphas.filter(polars.col("timestamp") > timestamp)
            # figure out the trades you actually made, there might be some symbols where there are no trades following alphas
            attempted_trades = viable_alphas.join_asof(batch, on = "timestamp", by = "symbol", strategy = "forward")
            # figure out what these symbols are 
            executed_trades = attempted_trades.filter(polars.col("price").is_not_null())
            unexecuted_alphas = attempted_trades.filter(polars.col("price").is_null()).select(["timestamp", "symbol", "direction"]).sort("timestamp")
            # prepend them to the unexecuted alphas, hopefully they will be executed next batch
            self.alphas = unexecuted_alphas.vstack(remaining_alphas)

            if len(executed_trades) != 0:
                for trade in executed_trades.iter_rows(named = True):
                    symbol = trade["symbol"]
                    direction = trade["direction"]
                    if symbol not in self.positions:
                        self.positions[symbol] = 0
                    self.positions[symbol] += direction
                    self.positions["cash"] -= direction * trade["price"]
            
            # now find the last trade tick in the day for each of the symbols in your positions
            # note you also need to make sure that the last trade tick in the day is actually after the trade
            # however this should be the case since we are using forward asof join

            symbols = list(self.positions.keys())
            symbols.remove("cash")
            directions = [self.positions[symbol] for symbol in symbols]
            positions = polars.from_dict({"symbol": symbols, "direction": directions})
            # print(positions)
            if len(positions) == 0:
                stock_equity = 0
            else:
                last_trades = batch.filter(polars.col("symbol").is_in(symbols)).groupby("symbol", maintain_order = True).last()
                last_trades = last_trades.join(positions, on = "symbol")
                stock_equity = (last_trades["direction"] * last_trades["price"]).sum()

            dates.append(batch["date"][0])
            daily_equity.append(stock_equity + self.positions["cash"])
        
        return polars.from_dict({"date": dates, "equity": daily_equity}, schema = {"date": polars.Utf8(), "equity": polars.Float64()})

    def done(self,executor_id):
        return
        
alpha = generate_random_alpha()
symbols = set(alpha["symbol"].to_list())
sql_filter = "symbol in ({})".format(",".join(["'{}'".format(symbol) for symbol in symbols]))
df = qc.read_sorted_parquet("s3://nasdaq-tick-data/yearly-trades-parquet-all/*", 
                            sorted_by = "timestamp", nthreads = 2, sort_order = "stride", name_col="date")
# first filter for trades that happened only on NASDAQ
df = df.filter_sql("exchange in ('T', 'Q') and condition not in ('16','20')")
df = df.with_columns({"timestamp_hr": lambda x: polars.from_epoch(x["timestamp"] - 5 * 3600 * 1000, time_unit = "ms")})
df = df.with_columns({"hour": lambda x: x["timestamp_hr"].dt.hour(), "minute": lambda x: x["timestamp_hr"].dt.minute()})
df = df.with_columns({"minutes_in_day": lambda x: x["hour"] * 60 + x["minute"]})
df = df.filter_sql("minutes_in_day >= 570 and minutes_in_day <= 960")
print(df)
df = df.filter_sql(sql_filter)
print(df)
executor = Backtester(alpha)
df = df.stateful_transform(executor, ["date", "equity"], {"date", "timestamp", "symbol", "price"},  by = "symbol")
print(df)
df.explain()
results = df.collect()
results.write_parquet("results.parquet")
pnl = results.groupby("date").agg(polars.col("equity").sum()).sort("date")["equity"].to_list()
plt.plot(pnl)
plt.show()