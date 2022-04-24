import pandas as pd
import polars
from schema import * 
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.dataset import InputCSVDataset, InputMultiParquetDataset
from pyquokka.sql import AggExecutor, BroadcastJoinExecutor, PolarJoinExecutor
import pyarrow.compute as compute
import ray
import sys

ips = ['localhost', '172.31.11.134', '172.31.15.208', '172.31.11.188']
workers = 1
task_graph = TaskGraph()


nation = pd.read_csv("s3://tpc-h-csv/nation/nation.tbl",header = None, names = nation_scheme,sep="|")
region = pd.read_csv("s3://tpc-h-csv/region/region.tbl",header = None, names = region_scheme,sep="|")
region = region[region.r_name == "AMERICA"]
america = nation.merge(region,left_on="n_regionkey",right_on = "r_regionkey")
print(america)

lineitem_filter = lambda x: polars.from_arrow(x.drop(["null"])).select(["l_partkey","l_orderkey","l_suppkey", "l_extendedprice","l_discount"])
orders_filter = lambda x: polars.from_arrow(x.drop(["null"]).filter(compute.and_(compute.less(x['o_orderdate'] , compute.strptime("1996-12-31",format="%Y-%m-%d",unit="s")), 
compute.greater(x['o_orderdate'],compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s"))))).select(["o_orderkey","o_custkey", "o_orderdate"])
customer_filter = lambda x: polars.from_arrow(x.drop(["null"])).select(["c_custkey","c_nationkey"])
part_filter = lambda x: polars.from_arrow(x.drop(["null"]).filter(compute.equal(x["p_type"],"ECONOMY ANODIZED STEEL"))).select(["p_partkey"])
supplier_filter = lambda x:polars.from_arrow(x.drop(["null"])).select(["s_suppkey","s_nationkey"])


if sys.argv[1] == "csv":
    lineitem_csv_reader = InputCSVDataset("tpc-h-csv", "lineitem/lineitem.tbl.1", lineitem_scheme , sep="|", stride = 128 * 1024 * 1024)
    orders_csv_reader = InputCSVDataset("tpc-h-csv", "orders/orders.tbl.1", order_scheme , sep="|", stride = 128 * 1024 * 1024)
    customer_csv_reader = InputCSVDataset("tpc-h-csv", "customer/customer.tbl.1", customer_scheme , sep="|", stride = 128 * 1024 * 1024)
    part_csv_reader =  InputCSVDataset("tpc-h-csv", "part/part.tbl.1", part_scheme , sep="|", stride = 128 * 1024 * 1024)
    supplier_csv_reader = InputCSVDataset("tpc-h-csv", "supplier/supplier.tbl.1", supplier_scheme , sep="|", stride = 128 * 1024 * 1024)
    lineitem_csv_reader.get_csv_attributes(8 * workers)
    orders_csv_reader.get_csv_attributes(4 * workers)
    customer_csv_reader.get_csv_attributes(4 * workers)
    part_csv_reader.get_csv_attributes(4 * workers)
    supplier_csv_reader.get_csv_attributes(4 * workers)
    lineitem = task_graph.new_input_reader_node(lineitem_csv_reader,{ips[i]: 8 for i in range(workers)}, batch_func = lineitem_filter)
    orders = task_graph.new_input_reader_node(orders_csv_reader, {ips[i]: 4 for i in range(workers)}, batch_func = orders_filter)
    customer = task_graph.new_input_reader_node(customer_csv_reader,{ips[i]: 4 for i in range(workers)}, batch_func = customer_filter)
    part = task_graph.new_input_reader_node(part_csv_reader,{ips[i]: 4 for i in range(workers)}, batch_func = part_filter)
    supplier = task_graph.new_input_reader_node(supplier_csv_reader, {ips[i]: 4 for i in range(workers)}, batch_func =supplier_filter)
elif sys.argv[1] == "parquet":
    lineitem_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","lineitem.parquet", columns = ["l_partkey","l_orderkey","l_suppkey", "l_extendedprice","l_discount"])
    orders_parquet_reader =InputMultiParquetDataset("tpc-h-parquet","orders.parquet", columns = ["o_orderkey","o_custkey", "o_orderdate"],
    filters= [('o_orderdate','<',compute.strptime("1996-12-31",format="%Y-%m-%d",unit="s")),('o_orderdate','>',compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s"))])
    customer_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","customer.parquet", columns = ["c_custkey","c_nationkey"])
    part_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","part.parquet", columns = ["p_partkey"], filters = [('p_type','==','ECONOMY ANODIZED STEEL')])
    supplier_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","supplier.parquet", columns = ["s_suppkey","s_nationkey"])
    lineitem = task_graph.new_input_reader_node(lineitem_parquet_reader,{ips[i]: 4 for i in range(workers)})
    orders = task_graph.new_input_reader_node(orders_parquet_reader, {ips[i]: 4 for i in range(workers)})
    customer = task_graph.new_input_reader_node(customer_parquet_reader,{ips[i]: 4 for i in range(workers)})
    part = task_graph.new_input_reader_node(part_parquet_reader,{ips[i]: 4 for i in range(workers)})
    supplier = task_graph.new_input_reader_node(supplier_parquet_reader, {ips[i]: 4 for i in range(workers)})

def pass_thru(data, source_channel, target_channel):

    if source_channel//4 == target_channel:
        return data
    else:
        return None

# input is pandas! output is also pandas
def batch_func(x):
    x['o_year'] = pd.DatetimeIndex(x['o_orderdate']).year
    x['volume'] = x['l_extendedprice'] * (1 - x['l_discount'])
    x['brazil'] = x['volume'] * (x['n_name'] == "BRAZIL")
    result = x.groupby("o_year").agg({"brazil":["sum"], "volume":["sum"]})
    return result


def final_func(x):
    x["mkt_share"] = x["brazil"] / x["volume"]
    return x

bjoin = BroadcastJoinExecutor(america,small_on="n_nationkey",big_on="c_nationkey",columns=["c_custkey"])
filtered_customer = task_graph.new_non_blocking_node({0:customer},None,bjoin,{ip:1 for ip in ips[:workers]},{0:pass_thru})
join1 = PolarJoinExecutor(left_on="p_partkey",right_on="l_partkey",columns =["l_suppkey", "l_orderkey","l_extendedprice","l_discount"])
part_lineitem = task_graph.new_non_blocking_node({0:part, 1:lineitem},None,join1, {ip:1 for ip in ips[:workers]},{0:"p_partkey",1:"l_partkey"})
join2 = PolarJoinExecutor(left_on = "l_orderkey", right_on="o_orderkey", columns =["l_suppkey", "o_custkey", "o_orderdate", "l_extendedprice","l_discount"])
part_lineitem_orders = task_graph.new_non_blocking_node({0:part_lineitem, 1:orders},None,join2,{ip:1 for ip in ips[:workers]},{0:"l_orderkey",1:"o_orderkey"})
join3 = PolarJoinExecutor(left_on="o_custkey",right_on="c_custkey",columns = ["l_suppkey", "o_orderdate", "l_extendedprice","l_discount"])
part_lineitem_orders_customers = task_graph.new_non_blocking_node({0:part_lineitem_orders, 1:filtered_customer}, None, join3,{ip:1 for ip in ips[:workers]}, {0:"o_custkey",1:"c_custkey"})

bjoin2 = BroadcastJoinExecutor(nation, small_on = "n_nationkey",big_on="s_nationkey")
filtered_supplier = task_graph.new_non_blocking_node({0:supplier},None,bjoin2, {ip:1 for ip in ips[:workers]},{0:pass_thru})

join4 = PolarJoinExecutor(left_on="l_suppkey",right_on="s_suppkey",columns =["o_orderdate", "l_extendedprice","l_discount","n_name"], batch_func = batch_func)
all_nations = task_graph.new_non_blocking_node({0:part_lineitem_orders_customers, 1: filtered_supplier}, None, join4,{ip:1 for ip in ips[:workers]}, {0:"l_suppkey",1:"s_suppkey"} )

agg = AggExecutor(final_func=final_func)
result = task_graph.new_blocking_node({0:all_nations},None,agg,{'localhost':1},{0:None})
import time

task_graph.create()
start = time.time()
task_graph.run_with_fault_tolerance()
print("total time ", time.time() - start)

print(ray.get(result.to_pandas.remote()))
