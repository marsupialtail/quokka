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

test = """
WITH k as (SELECT symbol, time, avg(bid) OVER (PARTITION BY symbol ORDER BY time RANGE 1000 preceding) AS rank FROM quotes WHERE ask > 0.5)
select max(rank) from k
"""


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

query = """

-- this takes more than 10 minnutes
select trades.time as trade_time, 
       quotes.time as quote_time
from trades,
     quotes
where trades.symbol == quotes.symbol
    and trades.time > quotes.time
    and trades.time < quotes.time + 1000

"""

query = """

-- this takes 13 seconds.
select trades.time as trade_time, 
       quotes.time as quote_time
from trades,
     quotes
where trades.symbol == "RPTP"
    and quotes.symbol == "RPTP"
    and trades.time == quotes.time

"""

query = """
with binned_trades (bin, time, symbol) as (
    select cast(time / 1000 as int),
        time,
        symbol
    from trades
    -- where symbol == "RPTP"
), 
binned_quotes (bin, time, symbol) as (
    select cast(time / 1000 as int),
        time,
        symbol
    from quotes
    -- where symbol == "RPTP"
),
prev_join (symbol, trade_time, quote_time) as (
    select  binned_trades.symbol,
            binned_trades.time,
            binned_quotes.time
    from binned_trades,
        binned_quotes
    where binned_trades.bin == binned_quotes.bin + 1
        and binned_trades.symbol == binned_quotes.symbol
),
curr_join (symbol, trade_time, quote_time) as (
    select  binned_trades.symbol,
            binned_trades.time,
            binned_quotes.time
    from binned_trades,
        binned_quotes
    where binned_trades.bin == binned_quotes.bin
        and binned_trades.symbol == binned_quotes.symbol
),
unioned (symbol, trade_time, quote_time) as (
    select symbol, trade_time, quote_time from prev_join
    union all
    select symbol, trade_time, quote_time from curr_join
)
select symbol,
        trade_time,
        count(*) as num_quotes
from unioned
where trade_time < quote_time + 1000
group by symbol, trade_time
"""

start = time.time(); result = spark.sql(query).collect(); print("QUERY TOOK", time.time() - start)