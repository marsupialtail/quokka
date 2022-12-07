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

df_trades.createOrReplaceTempView("trades")
df_quotes.createOrReplaceTempView("quotes")
