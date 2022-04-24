from pyspark.sql.types import StructType, StructField, FloatType, LongType, DecimalType, IntegerType, StringType, DateType

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

schema_supplier = StructType([StructField("s_suppkey",LongType(),False),StructField("s_name",StringType(),True),StructField("s_address",StringType(),True),StructField("s_nationkey",LongType(),False),StructField("s_phone",StringType(),True),StructField("s_acctbal",DecimalType(10,2),True),StructField("s_comment",StringType(),True)])

schema_partsupp = StructType([StructField("ps_partkey",LongType(),False),StructField("ps_suppkey",LongType(),False),StructField("ps_availqty",IntegerType(),True),StructField("ps_supplycost",DecimalType(10,2),True),StructField("ps_comment",StringType(),True)])

schema_nation = StructType([StructField("n_nationkey",LongType(),False),StructField("n_name",StringType(),False),StructField("n_regionkey",LongType(),False),StructField("n_comment",StringType(),True)])

schema_region = StructType([StructField("r_regionkey",LongType(),False),StructField("r_name",StringType(),True),StructField("r_comment",StringType(),True)])

'''
df_lineitem = spark.read.parquet("s3://tpc-h-parquet/lineitem.parquet")
df_orders = spark.read.parquet("s3://tpc-h-parquet/orders.parquet")
df_customer = spark.read.parquet("s3://tpc-h-parquet/customer.parquet")
df_part = spark.read.parquet("s3://tpc-h-parquet/part.parquet")
df_partsupp = spark.read.parquet("s3://tpc-h-parquet/partsupp.parquet")
df_nation = spark.read.parquet("s3://tpc-h-parquet/nation.parquet")
df_region = spark.read.parquet("s3://tpc-h-parquet/region.parquet")
df_supplier = spark.read.parquet("s3://tpc-h-parquet/supplier.parquet")
df_lineitem.createOrReplaceTempView("lineitem")
df_orders.createOrReplaceTempView("orders")
df_customer.createOrReplaceTempView("customer")
df_part.createOrReplaceTempView("part")
df_partsupp.createOrReplaceTempView("partsupp")
df_nation.createOrReplaceTempView("nation")
df_region.createOrReplaceTempView("region")
df_supplier.createOrReplaceTempView("supplier")
'''