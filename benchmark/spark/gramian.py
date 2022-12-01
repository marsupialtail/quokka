import pandas as pd

from pyspark.sql.types import LongType
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
from pyspark.sql.types import StructType, StructField, FloatType, LongType, DecimalType, IntegerType, StringType, DateType, BooleanType
import numpy as np

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
    result = np.zeros((4,4))
    for x in batches:
        x = x.to_numpy().astype(np.float32)
        result += np.dot(x.transpose(), x)
    yield pd.DataFrame(result, columns = ["a","b","c","d"])

counted = df_lineitem.select(["l_quantity", "l_extendedprice", "l_discount", "l_tax"]).mapInPandas(udf2, StructType().add("a",FloatType(),True).add("b",FloatType(),True).add("c",FloatType(),True).add("d",FloatType(),True))

import time
start = time.time()
results = counted.toPandas()
result = np.zeros((4,4))
for x in range(0, 376 * 4, 4):
    z = results.iloc[x:x+4].to_numpy()
    result += z
print(time.time() - start)
print(result)