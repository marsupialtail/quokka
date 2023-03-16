from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
from tpch import * 

df_lineitem = spark.read.parquet("s3://tpc-h-parquet-100/lineitem.parquet")
df_customer = spark.read.parquet("s3://tpc-h-parquet-100/customer.parquet")
df_orders = spark.read.parquet("s3://tpc-h-parquet-100/orders.parquet")
df_partsupp = spark.read.parquet("s3://tpc-h-parquet-100/partsupp.parquet")
df_part = spark.read.parquet("s3://tpc-h-parquet-100/part.parquet")
df_region = spark.read.parquet("s3://tpc-h-parquet-100/region.parquet")
df_nation = spark.read.parquet("s3://tpc-h-parquet-100/nation.parquet")
df_supplier = spark.read.parquet("s3://tpc-h-parquet-100/supplier.parquet")
df_lineitem.createOrReplaceTempView("lineitem")
df_orders.createOrReplaceTempView("orders")
df_customer.createOrReplaceTempView("customer")
df_partsupp.createOrReplaceTempView("partsupp")
df_part.createOrReplaceTempView("part")
df_region.createOrReplaceTempView("region")
df_nation.createOrReplaceTempView("nation")
df_supplier.createOrReplaceTempView("supplier")

cubequery = """
select
        l_returnflag,
        l_linestatus,
        l_shipmode,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
from
        lineitem
where
        l_shipdate <= date '1998-12-01' - interval '90' day
group by cube(
        l_returnflag,
        l_linestatus,
        l_shipmode)

"""

import time, sys

queries = [query1, query2,query3,query4,query5,query6,query7,query8,query9,query10, query11, query12, query13, query14, query15, query16, query17, query18, query19, query20, query22]

start = time.time(); result = spark.sql(queries[int(sys.argv[1])]).collect(); print("QUERY TOOK", time.time() - start)