from pyquokka.df import * 
from pyquokka.windowtypes import *
from pyquokka.utils import LocalCluster, QuokkaClusterManager
import numpy as np
import polars

manager = QuokkaClusterManager()
# cluster = manager.get_cluster_from_json("config.json")
cluster = LocalCluster()
qc = QuokkaContext(cluster,4, 2)

# quotes = qc.read_sorted_parquet("s3://quokka-asof-parquet/quotes/*", "time")
# trades = qc.read_sorted_parquet("s3://quokka-asof-parquet/trades/*", "time")

quotes = qc.read_sorted_csv("/home/ziheng/tpc-h/quotes.csv", "time", has_header = True)
# trades = qc.read_sorted_csv("/home/ziheng/tpc-h/trades.csv", "time", has_header = True)

window = SlidingWindow("time", "symbol", size_before=100000, aggregation_dict={"avg_bid":"AVG(bid)"})
trigger = OnEventTrigger()

quotes = quotes.filter("ask > 1")
windowed_quotes = quotes.windowed_transform(window, trigger)
result = windowed_quotes.collect()
ref = polars.read_csv("/home/ziheng/tpc-h/quotes.csv").filter(polars.col("ask") > 1).\
    groupby_rolling("time", period ="100000i", by = "symbol").agg(polars.col("bid").mean())

# we should sort by the actual value too because there could be multiple rows with the same symbol and time.
assert len(ref) == len(result)
assert (ref.sort(["symbol","time","bid"])["bid"] - result.sort(["symbol","time","avg_bid"])["avg_bid"]).sum() < 0.1
