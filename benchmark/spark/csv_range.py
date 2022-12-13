from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
from pyspark.sql.types import StructType, StructField, FloatType, LongType, DecimalType, IntegerType, StringType, DateType, BooleanType

schema_quotes = StructType()\
        .add("time", LongType(), True)\
        .add("symbol", StringType(), True)\
        .add("seq", FloatType(), True)\
        .add("bid", FloatType(), True)\
        .add("ask", FloatType(), True)\
        .add("bsize", FloatType(), True)\
        .add("asize", FloatType(), True)\
        .add("is_nbbo", BooleanType(), True)

schema_trades = StructType()\
        .add("time", LongType(), True)\
        .add("symbol", StringType(), True)\
        .add("size", FloatType(), True)\
        .add("price", FloatType(), True)


df_trades = spark.read.option("header", "true")\
            .schema(schema_trades)\
            .csv("s3://quokka-asofjoin/trades/*")
df_quotes = spark.read.option("header", "true")\
            .schema(schema_quotes)\
            .csv("s3://quokka-asofjoin/quotes/*")


df_trades = spark.read.parquet("s3://quokka-asof-parquet/trades/")
df_quotes = spark.read.parquet("s3://quokka-asof-parquet/quotes/")

df_trades.createOrReplaceTempView("trades")
df_quotes.createOrReplaceTempView("quotes")

import pyarrow
import duckdb

def udf2(l, r):
    x = pyarrow.Table.from_pandas(l)
    y = pyarrow.Table.from_pandas(r)
    con = duckdb.connect()
    
    

df_trades_grouped = df_trades.groupby("symbol")
df_quotes_grouped = df_quotes.groupby("symbol")
cg = df_trades_grouped.cogroup(df_quotes_grouped)
cg.applyInPandas


query = """

select 
    trade.time,

from 
    trades,
    quotes
where 
    trade.time > quote.time 
    and trade.time < quote.time + 1000
    and 
group by 
    trade.time

"""