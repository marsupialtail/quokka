import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, LongType, DecimalType, IntegerType, StringType, DateType
from schema import * 

def run_tpch_q12(source_lineitem, source_orders, output_uri):
    
    with SparkSession.builder.appName("TPC-H_q12").getOrCreate() as spark:
       
        timing = []
        start = time.time()


        df_lineitem = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_lineitem)\
            .csv(source_lineitem)
        df_orders = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_orders)\
            .csv(source_orders)

        #df_lineitem = spark.read.parquet("s3://tpc-h-parquet/lineitem.parquet"); df_orders = spark.read.parquet("s3://tpc-h-parquet/orders.parquet")

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
