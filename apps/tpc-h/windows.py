from pyquokka.df import * 
from pyquokka.windowtypes import *
from pyquokka.utils import LocalCluster, QuokkaClusterManager
import numpy as np
import polars

manager = QuokkaClusterManager()
cluster = manager.get_cluster_from_json("config.json")
# cluster = LocalCluster()
qc = QuokkaContext(cluster,4, 2)

quotes = qc.read_sorted_parquet("s3://quokka-asof-parquet/quotes/*", "time")
# trades = qc.read_sorted_parquet("s3://quokka-asof-parquet/trades/*", "time")

# quotes = qc.read_sorted_csv("/home/ziheng/tpc-h/quotes.csv", "time", has_header = True)
# trades = qc.read_sorted_csv("/home/ziheng/tpc-h/trades.csv", "time", has_header = True)

window = SlidingWindow(size_before=1000)

quotes = quotes.filter("ask > 1")

windowed_quotes = quotes._windowed_aggregate("time", "symbol", window, ["time","symbol","mean_bid"], {"bid"}, [])

print(windowed_quotes.count())

# quotes.groupby(["symbol"]).agg({"bid": "mean","time":"max"}).collect()