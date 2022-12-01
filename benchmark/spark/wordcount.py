import pandas as pd

from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
from pyspark.sql.types import StructType, StructField, FloatType, LongType, DecimalType, IntegerType, StringType, DateType, BooleanType

import pyarrow.compute as compute
import pyarrow

schema_lineitem = StructType()\
    .add("l_orderkey",LongType(),True)\
    .add("l_partkey",LongType(),True)\
    .add("l_suppkey",LongType(),True)\
    .add("l_linenumber",IntegerType(),True)\
    .add("l_quantity",DecimalType(10,2),True)\
    .add("l_extendedprice",DecimalType(10,2),True)\
    .add("l_discount",DecimalType(10,2),True)\
    .add("l_tax",DecimalType(10,2),True)\
    .add("l_returnflag",StringType(),True)\
    .add("l_linestatus",StringType(),True)\
    .add("l_shipdate",DateType(),True)\
    .add("l_commitdate",DateType(),True)\
    .add("l_receiptdate",DateType(),True)\
    .add("l_shipinstruct",StringType(),True)\
    .add("l_shipmode",StringType(),True)\
    .add("l_comment",StringType(),True)\
    .add("l_extra",StringType(),True)

df_lineitem = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_lineitem)\
        .csv("s3://tpc-h-csv/lineitem/lineitem.tbl.1")


def udf2(batches):
    for x in batches:
        x = pyarrow.Table.from_pandas(x)
        da = compute.list_flatten(compute.ascii_split_whitespace(x["l_comment"]))
        c = da.value_counts().flatten()
        yield pyarrow.Table.from_arrays([c[0], c[1]], names=["word","count"]).to_pandas()

# words = qc.read_csv(disk_path + "random-words.txt",["text"],sep="|")
counted = df_lineitem.select(["l_comment"]).mapInPandas(udf2, StructType().add("word",StringType(),True).add("count",LongType(),True))
import time
start = time.time(); f = counted.groupBy("word").sum("count").collect(); print(time.time() - start)


print(f)