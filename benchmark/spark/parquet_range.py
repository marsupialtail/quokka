from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
from pyspark.sql.types import StructType, StructField, FloatType, LongType, DecimalType, IntegerType, StringType, DateType, BooleanType
import pyarrow
import duckdb
import polars
import pandas as pd

df_trades = spark.read.parquet("s3://quokka-asof-parquet/trades/")
df_quotes = spark.read.parquet("s3://quokka-asof-parquet/quotes/")

df_trades.createOrReplaceTempView("trades")
df_quotes.createOrReplaceTempView("quotes")



# def udf2(l, r):
#     x = pyarrow.Table.from_pandas(l)
#     y = pyarrow.Table.from_pandas(r)
#     con = duckdb.connect()
#     result = con.execute("""
#         select 
#             x.time as time,
#             max(y.bid) as max_bid
#         from 
#             x,
#             y
#         where 
#             x.time > y.time 
#             and x.time < y.time + 1000000
#         group by 
#             x.time
#     """).arrow()
#     return result.to_pandas()


def udf2(l, r):
    return pd.merge_asof(l.sort_values('time'), r.sort_values('time'), on = "time")[["time", "bid"]]

def udf2(l, r):
    x = polars.from_pandas(l)
    y = polars.from_pandas(r)
    return x.join_asof(y, on = "time").select(["time", "bid"]).to_pandas()

import time

start = time.time()
df_trades_grouped = df_trades.groupby("symbol")
df_quotes_grouped = df_quotes.groupby("symbol")
cg = df_trades_grouped.cogroup(df_quotes_grouped)
result = cg.applyInPandas(udf2,  StructType().add("time",LongType(),True).add("bid",FloatType(),True))
result.count()
print(time.time() - start)
print("done")

