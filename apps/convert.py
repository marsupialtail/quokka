from schema import *
import polars as pl
from datetime import datetime
line_item_ds = pl.read_csv("/home/ziheng/tpc-h/lineitem.tbl",new_columns=lineitem_scheme,sep="|",has_header = False, parse_dates = True).drop("null")
line_item_ds.write_parquet("/home/ziheng/tpc-h/lineitem.parquet",row_group_size=100000)
orders_ds = pl.read_csv("/home/ziheng/tpc-h/orders.tbl",new_columns=order_scheme,sep="|",has_header = False, parse_dates = True).drop("null")
orders_ds.write_parquet("/home/ziheng/tpc-h/orders.parquet",row_group_size=100000)
customer_ds = pl.read_csv("/home/ziheng/tpc-h/customer.tbl",new_columns=customer_scheme,sep="|",has_header = False, parse_dates = True).drop("null")
customer_ds.write_parquet("/home/ziheng/tpc-h/customer.parquet",row_group_size=100000)
part_ds = pl.read_csv("/home/ziheng/tpc-h/part.tbl",new_columns=part_scheme,sep="|",has_header = False, parse_dates = True).drop("null")
part_ds.write_parquet("/home/ziheng/tpc-h/part.parquet",row_group_size=100000)
partsupp_ds = pl.read_csv("/home/ziheng/tpc-h/partsupp.tbl",new_columns=partsupp_scheme,sep="|",has_header = False, parse_dates = True).drop("null")
partsupp_ds.write_parquet("/home/ziheng/tpc-h/partsupp.parquet",row_group_size=100000)
supplier_ds = pl.read_csv("/home/ziheng/tpc-h/supplier.tbl",new_columns=supplier_scheme,sep="|",has_header = False, parse_dates = True).drop("null")
supplier_ds.write_parquet("/home/ziheng/tpc-h/supplier.parquet")
nation_ds = pl.read_csv("/home/ziheng/tpc-h/nation.tbl",new_columns=nation_scheme,sep="|",has_header = False, parse_dates = True).drop("null")
nation_ds.write_parquet("/home/ziheng/tpc-h/nation.parquet")
region_ds = pl.read_csv("/home/ziheng/tpc-h/region.tbl",new_columns=region_scheme,sep="|",has_header = False, parse_dates = True).drop("null")
region_ds.write_parquet("/home/ziheng/tpc-h/region.parquet")