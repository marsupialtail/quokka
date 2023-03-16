from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
from pyspark.sql.types import StructType, StructField, FloatType, LongType, DecimalType, IntegerType, StringType, DateType, BooleanType
# from tpch import * 

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

schema_orders = StructType()\
    .add("o_orderkey",LongType(),True)\
    .add("o_custkey",LongType(),True)\
    .add("o_orderstatus",StringType(),True)\
    .add("o_totalprice",DecimalType(10,2),True)\
    .add("o_orderdate",DateType(),True)\
    .add("o_orderpriority",StringType(),True)\
    .add("o_clerk",StringType(),True)\
    .add("o_shippriority",IntegerType(),True)\
    .add("o_comment",StringType(),True)\
    .add("o_extra",StringType(),True)

schema_customers = StructType()\
    .add("c_custkey", LongType(), True)\
    .add("c_name", StringType(), True)\
    .add("c_address", StringType(), True)\
    .add("c_nationkey", LongType(), True)\
    .add("c_phone", StringType(), True)\
    .add("c_acctbal", DecimalType(10,2),True)\
    .add("c_mktsegment", StringType(), True)\
    .add("c_comment", StringType(), True)

schema_part = StructType()\
        .add("p_partkey", LongType(), True)\
        .add("p_name", StringType(), True)\
        .add("p_mfgr", StringType(), True)\
        .add("p_brand", StringType(), True)\
        .add("p_type", StringType(), True)\
        .add("p_size", IntegerType(), True)\
        .add("p_container", StringType(), True)\
        .add("p_retailprice", DecimalType(10,2), True)\
        .add("p_comment", StringType(), True)

schema_supplier = StructType([StructField("s_suppkey",LongType(),False),StructField("s_name",StringType(),True),StructField("s_address",StringType(),True),StructField("s_nationkey",LongType(),False),StructField("s_phone",StringType(),True),StructField("s_acctbal",DecimalType(10,2),True),StructField("s_comment",StringType(),True)])

schema_partsupp = StructType([StructField("ps_partkey",LongType(),False),StructField("ps_suppkey",LongType(),False),StructField("ps_availqty",IntegerType(),True),StructField("ps_supplycost",DecimalType(10,2),True),StructField("ps_comment",StringType(),True)])

schema_nation = StructType([StructField("n_nationkey",LongType(),False),StructField("n_name",StringType(),False),StructField("n_regionkey",LongType(),False),StructField("n_comment",StringType(),True)])

schema_region = StructType([StructField("r_regionkey",LongType(),False),StructField("r_name",StringType(),True),StructField("r_comment",StringType(),True)])



df_lineitem = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_lineitem)\
            .csv("s3://tpc-h-csv-100/lineitem.tbl")
df_orders = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_orders)\
        .csv("s3://tpc-h-csv-100/orders.tbl")
df_customers = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_customers)\
        .csv("s3://tpc-h-csv-100/customer.tbl")
df_partsupp = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_partsupp)\
            .csv("s3://tpc-h-csv-100/partsupp.tbl")
df_part = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_part)\
        .csv("s3://tpc-h-csv-100/part.tbl")
df_supplier = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_supplier)\
        .csv("s3://tpc-h-csv-100/supplier.tbl")
df_region = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_region)\
            .csv("s3://tpc-h-csv-100/region.tbl")
df_nation = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_nation)\
        .csv("s3://tpc-h-csv-100/nation.tbl")


# df_lineitem = spark.read.table("demo.tpch1tb.lineitem")
# df_orders = spark.read.table("demo.tpch1tb.orders")
# df_customers = spark.read.table("demo.tpch1tb.customer")
# df_partsupp = spark.read.table("demo.tpch1tb.partsupp")
# df_part = spark.read.table("demo.tpch1tb.part")
# df_region = spark.read.table("demo.tpch1tb.region")
# df_nation = spark.read.table("demo.tpch1tb.nation")
# df_supplier = spark.read.table("demo.tpch1tb.supplier")

df_lineitem.createOrReplaceTempView("lineitem")
df_orders.createOrReplaceTempView("orders")
df_customers.createOrReplaceTempView("customer")
df_partsupp.createOrReplaceTempView("partsupp")
df_part.createOrReplaceTempView("part")
df_region.createOrReplaceTempView("region")
df_nation.createOrReplaceTempView("nation")
df_supplier.createOrReplaceTempView("supplier")

import time, sys

queries = [query1, query2,query3,query4,query5,query6,query7,query8,query9,query12]
start = time.time(); result = spark.sql(query5).collect(); print("QUERY TOOK", time.time() - start)

start = time.time(); result = spark.sql(queries[int(sys.argv[1])]).collect(); print("QUERY TOOK", time.time() - start)