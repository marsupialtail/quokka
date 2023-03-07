from pyquokka.df import * 
from pyquokka.utils import LocalCluster, QuokkaClusterManager
from schema import * 
mode = "DISK"
format = "csv"
disk_path = "/home/ziheng/tpc-h/"
#disk_path = "s3://yugan/tpc-h-out/"
s3_path_csv = "s3://tpc-h-csv/"
s3_path_parquet = "s3://books-iceberg/tpch.db/"

import pyarrow as pa
import pyarrow.compute as compute
import numpy as np
from pyquokka.executors import Executor
import polars

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
        lineitem = qc.read_csv(s3_path_csv + "lineitem/lineitem.tbl.1", lineitem_scheme, sep="|")
        orders = qc.read_csv(s3_path_csv + "orders/orders.tbl.1", order_scheme, sep="|")
        customer = qc.read_csv(s3_path_csv + "customer/customer.tbl.1",customer_scheme, sep = "|")
        part = qc.read_csv(s3_path_csv + "part/part.tbl.1", part_scheme, sep = "|")
        supplier = qc.read_csv(s3_path_csv + "supplier/supplier.tbl.1", supplier_scheme, sep = "|")
        partsupp = qc.read_csv(s3_path_csv + "partsupp/partsupp.tbl.1", partsupp_scheme, sep = "|")
        nation = qc.read_csv(s3_path_csv + "nation/nation.tbl", nation_scheme, sep = "|")
        region = qc.read_csv(s3_path_csv + "region/region.tbl", region_scheme, sep = "|")
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
    d.write_parquet(output_path + "testing.parquet")

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
    d.write_parquet(output_path + "testing.parquet")