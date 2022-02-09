import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, LongType, DecimalType, IntegerType, StringType, DateType

def run_tpch_q06(source_lineitem, output_uri):
    
    with SparkSession.builder.appName("TPC-H_q06").getOrCreate() as spark:
       
        timing = []
        start = time.time()

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
            .csv(source_lineitem)
        df_lineitem.createOrReplaceTempView("lineitem")

        query_output = spark.sql("""
            SELECT SUM(l_extendedprice*l_discount) AS revenue
              FROM lineitem 
             WHERE l_shipdate >= date '1994-01-01'
               AND l_shipdate < date '1995-01-01'
               AND l_discount BETWEEN 0.05 AND 0.07
               AND l_quantity < 24
            """)
        query_output.collect()

        end = time.time()
        timing.append(end - start)

        # query_output.write.option("header", "true").mode("overwrite").csv(output_uri)
        timing_df = spark.createDataFrame(timing, FloatType())
        timing_df.coalesce(1).write.option("header", "false").mode("overwrite").csv(output_uri)
        timing_df.repartition(1).write.option("header", "false").mode("overwrite").csv(output_uri)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--source_lineitem')
    parser.add_argument('--output_uri')
    args = parser.parse_args()

    run_tpch_q06(args.source_lineitem, args.output_uri)
