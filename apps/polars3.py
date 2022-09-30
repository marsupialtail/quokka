from schema import * 
import polars as pl
from datetime import datetime
line_item_ds = pl.read_csv("/home/ziheng/tpc-h/lineitem.tbl",new_columns=lineitem_scheme,sep="|",has_header = False, parse_dates = True).drop("null")
orders_ds = pl.read_csv("/home/ziheng/tpc-h/orders.tbl",new_columns=order_scheme,sep="|",has_header = False).drop("null")
customer_ds = pl.read_csv("/home/ziheng/tpc-h/customer.tbl",new_columns=customer_scheme,sep="|",has_header = False).drop("null")
var1 = var2 = datetime(1995, 3, 15)
var3 = "BUILDING"
q_final = (
        customer_ds.filter(pl.col("c_mktsegment") == var3)
        .join(orders_ds, left_on="c_custkey", right_on="o_custkey")
        .join(line_item_ds, left_on="o_orderkey", right_on="l_orderkey")
        .filter(pl.col("o_orderdate") < var2)
        .filter(pl.col("l_shipdate") > var1)
        .with_column(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")
        )
        .groupby(["o_orderkey", "o_orderdate", "o_shippriority"])
        .agg([pl.sum("revenue")])
        .select(
            [
                pl.col("o_orderkey").alias("l_orderkey"),
                "revenue",
                "o_orderdate",
                "o_shippriority",
            ]
        )
        #.sort(by=["revenue", "o_orderdate"], reverse=[True, False])
        #.limit(10)
    )
