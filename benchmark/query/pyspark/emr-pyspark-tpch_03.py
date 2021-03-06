import argparse
import time
from tokenize import String

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, LongType, DecimalType, IntegerType, StringType, DateType
from schema import * 


def run_tpch_q03(source_lineitem, source_orders, source_customers, output_uri):
    
    with SparkSession.builder.appName("TPC-H_q06").getOrCreate() as spark:
       
        timing = []
        start = time.time()

        df_lineitem = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_lineitem)\
            .csv(source_lineitem)
        df_orders = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_orders)\
            .csv("s3://tpc-h-csv/orders/orders.tbl.1")
        df_customers = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_customers)\
            .csv("s3://tpc-h-csv/customer/customer.tbl.1")

        df_lineitem.createOrReplaceTempView("lineitem");df_orders.createOrReplaceTempView("orders");df_customers.createOrReplaceTempView("customer")

        query_output = spark.sql("""
            select
                    l_orderkey,
                    sum(l_extendedprice * (1 - l_discount)) as revenue,
                    o_orderdate,
                    o_shippriority
            from
                    customer,
                    orders,
                    lineitem
            where
                    c_mktsegment = 'BUILDING'
                    and c_custkey = o_custkey
                    and l_orderkey = o_orderkey
                    and o_orderdate < date '1995-03-15'
                    and l_shipdate > date '1995-03-15'
            group by
                    l_orderkey,
                    o_orderdate,
                    o_shippriority
            order by
                    revenue desc,
                    o_orderdate
            limit 10;
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
    parser.add_argument('--source_lineitem')
    parser.add_argument('--source_lineitem')

    parser.add_argument('--output_uri')
    args = parser.parse_args()

    run_tpch_q03(args.source_lineitem, args.output_uri)
