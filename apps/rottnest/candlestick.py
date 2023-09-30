
from pyquokka import QuokkaContext
from pyquokka.utils import LocalCluster, QuokkaClusterManager
from pyarrow.fs import S3FileSystem
import pyarrow.parquet as pq
import polars

manager = QuokkaClusterManager(key_name = "zihengw", key_location = "/home/ziheng/Downloads/zihengw.pem")
# manager.start_cluster("config_2.json")
cluster = manager.get_cluster_from_json("config_2.json")
qc = QuokkaContext(cluster, 4, 2)

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
    df = df.with_columns((polars.col("timestamp") // 60000).alias("candle"))
    df = df.with_columns(polars.col("candle") - df["candle"].min())
    f = df.groupby(["symbol","candle"], maintain_order = True).agg([
        polars.head("price", 1).alias("open"),
        polars.tail("price", 1).alias("close"),
        polars.max("price").alias("high"),
        polars.min("price").alias("low"),
        polars.sum("volume")])
    f = f.with_columns([polars.col("open").list.first(), polars.col("close").list.first()])
    
    # write the result to S3
    pq.write_table(f.to_arrow(), "nasdaq-tick-data/ohlcv-1min-by-date/{}.parquet".format(date), filesystem = s3fs)
    return polars.from_dict({"written": [date]})


df = qc.read_parquet("s3://nasdaq-tick-data/yearly-trades-parquet-all/*", nthreads=1, name_column = "date")
df = df.filter_sql("exchange in ('T', 'Q') and condition not in ('16','20')")
df = df.transform(batch_func, ["written"], required_columns=df.schema, foldable = False)
print(df.collect())