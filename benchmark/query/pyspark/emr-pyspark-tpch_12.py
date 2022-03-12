import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, LongType, DecimalType, IntegerType, StringType, DateType

def run_tpch_q12(source_lineitem, source_orders, output_uri):
    
    with SparkSession.builder.appName("TPC-H_q12").getOrCreate() as spark:
       
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

        df_lineitem = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_lineitem)\
            .csv(source_lineitem)
        df_orders = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_orders)\
            .csv(source_orders)

        df_lineitem.createOrReplaceTempView("lineitem")
        df_orders.createOrReplaceTempView("orders")

        query_output = spark.sql("""
            SELECT l.l_shipmode
                 , SUM(CASE WHEN (o.o_orderpriority == '1-URGENT' OR  o.o_orderpriority == '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count
                 , SUM(CASE WHEN (o.o_orderpriority != '1-URGENT' AND o.o_orderpriority != '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count
              FROM orders AS o 
              JOIN lineitem AS l ON (l.l_orderkey = o.o_orderkey)
             WHERE l.l_shipmode IN ('MAIL', 'SHIP')
               AND l.l_commitdate < l.l_receiptdate
               AND l.l_shipdate < l.l_commitdate
               AND l.l_receiptdate >= date '1994-01-01'
               AND l.l_receiptdate < date '1995-01-01'
          GROUP BY l.l_shipmode
            """)
        query_output.collect()

        end = time.time()
        timing.append(end - start)


        # query_output.write.option("header", "true").mode("overwrite").csv(output_uri)
        # query_output.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_uri)
        # query_output.repartition(1).write.option("header", "true").mode("overwrite").csv(output_uri)
        timing_df = spark.createDataFrame(timing, FloatType())
        timing_df.coalesce(1).write.option("header", "false").mode("overwrite").csv(output_uri)
        timing_df.repartition(1).write.option("header", "false").mode("overwrite").csv(output_uri)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--source_lineitem')
    parser.add_argument('--source_orders')
    parser.add_argument('--output_uri')
    args = parser.parse_args()

    run_tpch_q12(args.source_lineitem, args.source_orders, args.output_uri)
