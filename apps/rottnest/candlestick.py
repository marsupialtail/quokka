
from pyquokka import QuokkaContext
from pyquokka.utils import LocalCluster, QuokkaClusterManager
from pyarrow.fs import S3FileSystem
import pyarrow.parquet as pq
import polars

manager = QuokkaClusterManager(key_name = "zihengw", key_location = "/home/ziheng/Downloads/zihengw.pem")
manager.start_cluster("config.json")
cluster = manager.get_cluster_from_json("config.json")
qc = QuokkaContext(cluster, 2, 2)

def batch_func(df):
    s3fs = S3FileSystem()
    print(df.columns)
    assert df["date"].describe()["value"][2] == 1, "date is not unique"
    date = df["date"][0]
    # get timestamp from epoch time, normalize for time zone to EST
    df = df.with_columns(polars.from_epoch(df["timestamp"] - 5 * 3600000, time_unit = "ms").alias("timestamp_dt"))
    # process symbol name
    df = df.with_columns(polars.col("symbol").str.rstrip(".csv.gz"))
    # filter for regular trading hours
    df = df.filter((polars.col("timestamp_dt").dt.time() >= polars.time(hour = 9, minute = 30))
           & (polars.col("timestamp_dt").dt.time() <= polars.time(hour = 16)))
    df = df.with_columns((polars.col("timestamp") // 5000).alias("candle"))
    df = df.with_columns(polars.col("candle") - df["candle"].min())
    f = df.groupby(["symbol","candle"], maintain_order = True).agg([
        polars.head("price", 1).alias("open"),
        polars.tail("price", 1).alias("close"),
        polars.max("price").alias("high"),
        polars.min("price").alias("low"),
        polars.sum("volume")])
    f = f.with_columns([polars.col("open").list.first(), polars.col("close").list.first()])
    
    # write the result to S3
    pq.write_table(f.to_arrow(), "options-research/finnhub-trade-ticks/ohlcv-5s/2019/{}.parquet".format(date), filesystem = s3fs)
    return polars.from_dict({"written": [date]})


df = qc.read_parquet("s3://options-research/finnhub-trade-ticks/2019/*", nthreads=1)
df = df.transform(batch_func, ["written"], required_columns=df.schema, foldable = False)
print(df.collect())