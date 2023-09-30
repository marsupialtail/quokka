from pyquokka.df import * 
from pyquokka.utils import LocalCluster, QuokkaClusterManager
from schema import * 
mode = "S3"
format = "csv"
disk_path = "/home/ziheng/tpc-h/"
#disk_path = "s3://yugan/tpc-h-out/"
s3_path_csv = "s3://tpc-h-csv-100/"
s3_path_parquet = "s3://tpc-h-parquet-100-native/"

import pyarrow as pa
import pyarrow.compute as compute
import numpy as np
from pyquokka.executors import Executor
import polars

OUTPUT_LINE_LIMIT = 1000000

if mode == "DISK":
    cluster = LocalCluster()
    output_path = "/home/ziheng/tpc-h/out/"
elif mode == "S3":
    manager = QuokkaClusterManager(key_name = "tony_key", key_location = "/home/ziheng/Downloads/tony_key.pem")
    cluster = manager.get_cluster_from_json("config.json")
    output_path = "s3://tpc-h-out/"
else:
    raise Exception

qc = QuokkaContext(cluster,4,2)

if mode == "DISK":
    if format == "csv":
        lineitem = qc.read_csv(disk_path + "lineitem.tbl", sep="|", has_header=True)
        orders = qc.read_csv(disk_path + "orders.tbl", sep="|", has_header=True)
        customer = qc.read_csv(disk_path + "customer.tbl",sep = "|", has_header=True)
        part = qc.read_csv(disk_path + "part.tbl", sep = "|", has_header=True)
        supplier = qc.read_csv(disk_path + "supplier.tbl", sep = "|", has_header=True)
        partsupp = qc.read_csv(disk_path + "partsupp.tbl", sep = "|", has_header=True)
        nation = qc.read_csv(disk_path + "nation.tbl", sep = "|", has_header=True)
        region = qc.read_csv(disk_path + "region.tbl", sep = "|", has_header=True)
    elif format == "parquet":
        lineitem = qc.read_parquet(disk_path + "lineitem.parquet")
        orders = qc.read_parquet(disk_path + "orders.parquet")
        customer = qc.read_parquet(disk_path + "customer.parquet")
        part = qc.read_parquet(disk_path + "part.parquet")
        supplier = qc.read_parquet(disk_path + "supplier.parquet")
        partsupp = qc.read_parquet(disk_path + "partsupp.parquet")
        nation = qc.read_parquet(disk_path + "nation.parquet")
        region = qc.read_parquet(disk_path + "region.parquet")
    else:
        raise Exception
elif mode == "S3":
    if format == "csv":
        lineitem = qc.read_csv(s3_path_csv + "lineitem.tbl", lineitem_scheme, sep="|").drop(["null"])
        orders = qc.read_csv(s3_path_csv + "orders.tbl", order_scheme, sep="|").drop(["null"])
        customer = qc.read_csv(s3_path_csv + "customer.tbl",customer_scheme, sep = "|").drop(["null"])
        part = qc.read_csv(s3_path_csv + "part.tbl", part_scheme, sep = "|").drop(["null"])
        supplier = qc.read_csv(s3_path_csv + "supplier.tbl", supplier_scheme, sep = "|").drop(["null"])
        partsupp = qc.read_csv(s3_path_csv + "partsupp.tbl", partsupp_scheme, sep = "|").drop(["null"])
        nation = qc.read_csv(s3_path_csv + "nation.tbl", nation_scheme, sep = "|").drop(["null"])
        region = qc.read_csv(s3_path_csv + "region.tbl", region_scheme, sep = "|").drop(["null"])
    elif format == "parquet":
        lineitem = qc.read_parquet(s3_path_parquet + "lineitem/data/*")
        #lineitem = qc.read_parquet("s3://yugan/tpc-h-out/*")
        orders = qc.read_parquet(s3_path_parquet + "orders/data/*")
        customer = qc.read_parquet(s3_path_parquet + "customer/data/*")
        part = qc.read_parquet(s3_path_parquet + "part/data/*") 
        supplier = qc.read_parquet(s3_path_parquet + "supplier/data/*")
        partsupp = qc.read_parquet(s3_path_parquet + "partsupp/data/*")
        nation = qc.read_parquet(s3_path_parquet + "nation/data/*")
        region = qc.read_parquet(s3_path_parquet + "region/data/*")

    else:
        raise Exception
else:
    raise Exception

def do_1_1():
    d = lineitem.filter("l_shipdate <= date '1998-09-02'")
    d = d.groupby("l_orderkey").agg_sql(
        """
        sum(l_quantity)                                       AS sum_qty,
        sum(l_extendedprice)                                  AS sum_base_price,
        sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        avg(l_quantity)                                       AS avg_qty,
        avg(l_extendedprice)                                  AS avg_price,
        avg(l_discount)                                       AS avg_disc,
        count(*)                                              AS count_order
        """
    )
    d = d.write_parquet(output_path + "testing.parquet", output_line_limit = OUTPUT_LINE_LIMIT)
    return d.collect()

def do_1_2():
    d = lineitem.filter("l_shipdate <= date '1998-09-02'")
    d = d.groupby("l_suppkey").agg_sql(
        """
        sum(l_quantity)                                       AS sum_qty,
        sum(l_extendedprice)                                  AS sum_base_price,
        sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        avg(l_quantity)                                       AS avg_qty,
        avg(l_extendedprice)                                  AS avg_price,
        avg(l_discount)                                       AS avg_disc,
        count(*)                                              AS count_order
        """
    )
    d = d.write_parquet(output_path + "testing.parquet", output_line_limit = OUTPUT_LINE_LIMIT)
    return d.collect()

def do_2():
    d = part.join(partsupp, left_on = "p_partkey", right_on = "ps_partkey")
    d = d.join(supplier, left_on = "ps_suppkey", right_on = "s_suppkey")
    d = d.join(nation, left_on = "s_nationkey", right_on = "n_nationkey")
    d = d.join(region, left_on = "n_regionkey", right_on = "r_regionkey")
    d = d.select(["s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment"]).write_parquet(output_path + "testing.parquet")
    return d.collect()

def do_3():
    d = customer.join(orders, left_on = "c_custkey", right_on = "o_custkey")
    d = d.join(lineitem, left_on = "o_orderkey", right_on = "l_orderkey")
    d = d.groupby(["o_orderkey", "o_orderdate", "o_shippriority"]).agg_sql(
        """
        sum(l_extendedprice * (1 - l_discount)) 		        AS revenue,
        sum(l_quantity)                                       AS sum_qty,
        sum(l_extendedprice)                                  AS sum_base_price,
        sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        avg(l_quantity)                                       AS avg_qty,
        avg(l_extendedprice)                                  AS avg_price,
        avg(l_discount)                                       AS avg_disc,
        count(*)                                              AS c
        """
    )
    d = d.write_parquet(output_path + "testing.parquet", output_line_limit = OUTPUT_LINE_LIMIT)
    return d.collect()

def do_4():
    d = lineitem.filter(lineitem["l_commitdate"] < lineitem["l_receiptdate"])
    d = orders.join(d, left_on="o_orderkey", right_on="l_orderkey", how = "semi")
    d = d.filter_sql("o_orderdate >= '1993-07-01'")
    d = d.select(["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"])\
        .write_parquet(output_path + "testing.parquet", output_line_limit = OUTPUT_LINE_LIMIT)
    return d.collect()

def do_5():
    qc.set_config("optimize_joins", False)
    
    my_nation = nation.join(region, left_on = "n_regionkey", right_on = "r_regionkey").filter("r_name IN ('ASIA', 'AMERICA', 'AFRICA')")
    d = supplier.join(my_nation, left_on = "s_nationkey", right_on = "n_nationkey")
    d = d.join(lineitem, left_on = "s_suppkey", right_on = "l_suppkey")
    d = d.join(orders, left_on = "l_orderkey", right_on = "o_orderkey")
    d = d.join(customer, left_on = "o_custkey", right_on = "c_custkey")
    d = d.filter("l_returnflag = 'A'")
    d = d.select(["c_custkey", "c_name", "c_phone", "c_mktsegment", "o_orderkey", "o_orderstatus", "o_totalprice", "o_orderdate",
                  "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", 
                  "l_receiptdate", "l_suppkey", "s_name", "s_acctbal", "s_phone", "n_name", "r_name"]).write_parquet(output_path + "testing.parquet", output_line_limit = OUTPUT_LINE_LIMIT)
    d.explain()
    result = d.collect()
    qc.set_config("optimize_joins", True)
    return result

def do_6():
    d = lineitem.filter("""
            l_shipdate >= date '1994-01-01'
            AND l_discount BETWEEN 0.05 AND 0.07
            AND l_quantity < 24
    """)
    d.select(["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", 
              "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", 
              "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"]).write_parquet(output_path + "testing.parquet", output_line_limit = OUTPUT_LINE_LIMIT)

def do_7():
    d1 = customer.join(nation, left_on = "c_nationkey", right_on = "n_nationkey")
    d1 = d1.join(orders, left_on = "c_custkey", right_on = "o_custkey", suffix = "_3")
    d2 = supplier.join(nation, left_on="s_nationkey", right_on = "n_nationkey")
    d2 = lineitem.join(d2, left_on = "l_suppkey", right_on = "s_suppkey", suffix = "_3")
    
    d = d1.join(d2, left_on = "o_orderkey", right_on = "l_orderkey",suffix="_4")
    d = d.rename({"n_name_4": "supp_nation", "n_name": "cust_nation"})
    d = d.filter("""l_shipdate between date '1995-01-01' and date '1996-12-31'""")
    d = d.with_column("l_year", polars.col("l_shipdate").dt.year(), required_columns = {"l_shipdate"})
    d = d.with_column("volume", polars.col("l_extendedprice") * (1 - polars.col("l_discount")), required_columns = {"l_extendedprice", "l_discount"})
    d.select(["l_year", "cust_nation", "supp_nation", "volume"]).write_parquet(output_path + "testing.parquet", output_line_limit = OUTPUT_LINE_LIMIT)

def do_8():
    america = region.filter("r_name = 'AMERICA'")
    american_nations = nation.join(america, left_on="n_regionkey",right_on="r_regionkey").select(["n_nationkey"])
    american_customers = customer.join(american_nations, left_on="c_nationkey", right_on="n_nationkey")
    american_orders = orders.join(american_customers, left_on = "o_custkey", right_on="c_custkey")
    d = lineitem.join(part, left_on="l_partkey", right_on="p_partkey")
    d = d.join(american_orders, left_on = "l_orderkey", right_on = "o_orderkey")
    d = d.join(supplier, left_on="l_suppkey", right_on="s_suppkey")
    d = d.join(nation, left_on="s_nationkey", right_on = "n_nationkey")
    d = d.filter("""
       o_orderdate between date '1995-01-01' and date '1996-12-31'
    """)
    d = d.with_column("o_year", polars.col("o_orderdate").dt.year(), required_columns = {"o_orderdate"})
    d = d.with_column("volume", polars.col("l_extendedprice") * (1 - polars.col("l_discount")), required_columns = {"l_extendedprice", "l_discount"})
    d = d.rename({"n_name" : "nation"})
    d.select(["o_year", "nation", "volume"]).write_parquet(output_path + "testing.parquet", output_line_limit = OUTPUT_LINE_LIMIT)

# do_1_1()
# do_1_2()
# do_2()
# do_3()
# do_4()
do_5()
do_6()
do_7()
do_8()
