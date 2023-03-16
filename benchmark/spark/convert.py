from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
from pyspark.sql.types import StructType, StructField, FloatType, LongType, DecimalType, IntegerType, StringType, DateType, BooleanType

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
    .add("l_comment",StringType(),True)

schema_orders = StructType()\
    .add("o_orderkey",LongType(),True)\
    .add("o_custkey",LongType(),True)\
    .add("o_orderstatus",StringType(),True)\
    .add("o_totalprice",DecimalType(10,2),True)\
    .add("o_orderdate",DateType(),True)\
    .add("o_orderpriority",StringType(),True)\
    .add("o_clerk",StringType(),True)\
    .add("o_shippriority",IntegerType(),True)\
    .add("o_comment",StringType(),True)

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

source_bucket = "tpc-h-csv-1tb"

df_lineitem = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_lineitem)\
            .csv("s3://" +  source_bucket + "/lineitem/*")
df_orders = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_orders)\
        .csv("s3://" +  source_bucket + "/orders/*")
df_customers = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_customers)\
        .csv("s3://" +  source_bucket + "/customer/*")
df_partsupp = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_partsupp)\
            .csv("s3://" +  source_bucket + "/partsupp/*")
df_part = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_part)\
        .csv("s3://" +  source_bucket + "/part/*")
df_supplier = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_supplier)\
        .csv("s3://" +  source_bucket + "/supplier/*")
df_region = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_region)\
            .csv("s3://" +  source_bucket + "/region/*")
df_nation = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_nation)\
        .csv("s3://" +  source_bucket + "/nation/*")

df_lineitem1 = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_lineitem)\
            .csv("s3://tpc-h-csv/lineitem/lineitem.tbl.2")
df_orders1 = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_orders)\
        .csv("s3://tpc-h-csv/orders/orders.tbl.2")
df_customers1 = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_customers)\
        .csv("s3://tpc-h-csv/customer/customer.tbl.2")
df_partsupp1 = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_partsupp)\
            .csv("s3://tpc-h-csv/partsupp/partsupp.tbl.2")
df_part1 = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_part)\
        .csv("s3://tpc-h-csv/part/part.tbl.2")
df_supplier1 = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_supplier)\
        .csv("s3://tpc-h-csv/supplier/supplier.tbl.2")

df_lineitem.union(df_lineitem1).repartition(128).write.parquet("s3://tpc-h-large-parquet/lineitem.parquet/")
df_orders.union(df_orders1).repartition(128).write.parquet("s3://tpc-h-large-parquet/orders.parquet/")
df_customers.union(df_customers1).repartition(128).write.parquet("s3://tpc-h-large-parquet/customer.parquet/")
df_partsupp.union(df_partsupp1).repartition(128).write.parquet("s3://tpc-h-large-parquet/partsupp.parquet/")
df_part.union(df_part1).repartition(128).write.parquet("s3://tpc-h-large-parquet/part.parquet/")
df_supplier.union(df_supplier1).repartition(128).write.parquet("s3://tpc-h-large-parquet/supplier.parquet/")
df_region.repartition(128).write.parquet("s3://tpc-h-large-parquet/region.parquet/")
df_nation.repartition(128).write.parquet("s3://tpc-h-large-parquet/nation.parquet/")