import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, LongType, DecimalType, IntegerType, StringType, DateType

def run_convert_files():
    
    with SparkSession.builder.appName("convert_files").getOrCreate() as spark:
       
        def convert_files(src, dst, schema):
            df = spark.read.option("header", "false").option("delimiter","|")\
                .schema(schema)\
                .csv(src)
            df.write.parquet(dst)
            return

        path_src = "s3://tpc-h-small/"
        path_dst = "s3://tpc-h-small/parquet/"

        src_lineitem = path_src + "lineitem.tbl"
        dst_lineitem = path_dst + "lineitem"
        schema_lineitem = StructType()\
            .add("l_orderkey",LongType(),False)\
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
        convert_files(src_lineitem, dst_lineitem, schema_lineitem)

        src_orders = path_src + "orders.tbl"
        dst_orders = path_dst + "orders"
        schema_orders = StructType()\
            .add("o_orderkey",LongType(),False)\
            .add("o_custkey",LongType(),False)\
            .add("o_orderstatus",StringType(),True)\
            .add("o_totalprice",DecimalType(10,2),True)\
            .add("o_orderdate",DateType(),True)\
            .add("o_orderpriority",StringType(),True)\
            .add("o_clerk",StringType(),True)\
            .add("o_shippriority",IntegerType(),True)\
            .add("o_comment",StringType(),True)
        convert_files(src_orders, dst_orders, schema_orders)

        # src_customer = path_src + "customer/*"
        # dst_customer = path_dst + "customer"
        # schema_customer = StructType()\
        #     .add("c_custkey",LongType(), False)\
        #     .add("c_name",StringType(), True)\
        #     .add("c_address",StringType(), True)\
        #     .add("c_nationkey",LongType(), False)\
        #     .add("c_phone",StringType(), True)\
        #     .add("c_acctbal",DecimalType(10,2), True)\
        #     .add("c_mktsegment",StringType(), True)\
        #     .add("c_comment",StringType(), True)
        # convert_files(src_customer, dst_customer, schema_customer)

        # src_part = path_src + "part/*"
        # dst_part = path_dst + "part"
        # schema_part = StructType()\
        #     .add("p_partkey", LongType(), False)\
        #     .add("p_name",StringType(),True)\
        #     .add("p_mfgr",StringType(),True)\
        #     .add("p_brand",StringType(),True)\
        #     .add("p_type",StringType(),True)\
        #     .add("p_size",IntegerType(),True)\
        #     .add("p_container",StringType(),True)\
        #     .add("p_retailprice",DecimalType(10,2),True)\
        #     .add("p_comment",StringType(),True)
        # convert_files(src_part, dst_part, schema_part)

        # src_supplier = path_src + "supplier/*"
        # dst_supplier = path_dst + "supplier"
        # schema_supplier = StructType()\
        #     .add("s_suppkey",LongType(),False)\
        #     .add("s_name",StringType(),True)\
        #     .add("s_address",StringType(),True)\
        #     .add("s_nationkey",LongType(),False)\
        #     .add("s_phone",StringType(),True)\
        #     .add("s_acctbal",DecimalType(10,2),True)\
        #     .add("s_comment",StringType(),True)
        # convert_files(src_supplier, dst_supplier, schema_supplier)

        # src_partsupp = path_src + "partsupp/*"
        # dst_partsupp = path_dst + "partsupp"
        # schema_partsupp = StructType()\
        #     .add("ps_partkey",LongType(),False)\
        #     .add("ps_suppkey",LongType(),False)\
        #     .add("ps_availqty",IntegerType(),True)\
        #     .add("ps_supplycost",DecimalType(10,2),True)\
        #     .add("ps_comment",StringType(),True)
        # convert_files(src_partsupp, dst_partsupp, schema_partsupp)

        # src_nation = path_src + "nation/*"
        # dst_nation = path_dst + "nation"
        # schema_nation = StructType()\
        #     .add("n_nationkey",LongType(),False)\
        #     .add("n_name",StringType(),False)\
        #     .add("n_regionkey",LongType(),False)\
        #     .add("n_comment",StringType(),True)
        # convert_files(src_nation, dst_nation, schema_nation)

        # src_region = path_src + "region/*"
        # dst_region = path_dst + "region"
        # schema_region = StructType()\
        #     .add("r_regionkey",LongType(),False)\
        #     .add("r_name",StringType(),True)\
        #     .add("r_comment",StringType(),True)
        # convert_files(src_region, dst_region, schema_region)


if __name__ == "__main__":
    run_convert_files()
