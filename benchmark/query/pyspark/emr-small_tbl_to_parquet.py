import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, LongType, DecimalType, IntegerType, StringType, DateType

def run_convert_files():
    
    with SparkSession.builder.appName("convert_files").getOrCreate() as spark:

        sc = spark.sparkContext
        block_size = 1024 * 1024 * 1024    # 1GB
        sc._jsc.hadoopConfiguration().setInt("dfs.blocksize", block_size)
        sc._jsc.hadoopConfiguration().setInt("parquet.block.size", blockSize)
       
        def convert_files(src, dst, schema):
            df = sc.read.option("header", "false").option("delimiter","|")\
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

        # src_orders = path_src + "orders.tbl"
        # dst_orders = path_dst + "orders"
        # schema_orders = StructType()\
        #     .add("o_orderkey",LongType(),False)\
        #     .add("o_custkey",LongType(),False)\
        #     .add("o_orderstatus",StringType(),True)\
        #     .add("o_totalprice",DecimalType(10,2),True)\
        #     .add("o_orderdate",DateType(),True)\
        #     .add("o_orderpriority",StringType(),True)\
        #     .add("o_clerk",StringType(),True)\
        #     .add("o_shippriority",IntegerType(),True)\
        #     .add("o_comment",StringType(),True)
        # convert_files(src_orders, dst_orders, schema_orders)


if __name__ == "__main__":
    run_convert_files()
