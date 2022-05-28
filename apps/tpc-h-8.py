import pandas as pd
import polars
from schema import * 
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.dataset import InputS3CSVDataset, InputMultiParquetDataset
from pyquokka.sql import AggExecutor, BroadcastJoinExecutor, PolarJoinExecutor
import pyarrow.compute as compute
import ray
import sys
from pyquokka.utils import LocalCluster, QuokkaClusterManager

manager = QuokkaClusterManager()
cluster = manager.get_cluster_from_json("config.json")
#cluster = LocalCluster()

task_graph = TaskGraph(cluster)

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
    lineitem_csv_reader = InputS3CSVDataset("tpc-h-csv", "lineitem/lineitem.tbl.1", lineitem_scheme , sep="|", stride = 128 * 1024 * 1024)
    orders_csv_reader = InputS3CSVDataset("tpc-h-csv", "orders/orders.tbl.1", order_scheme , sep="|", stride = 128 * 1024 * 1024)
    customer_csv_reader = InputS3CSVDataset("tpc-h-csv", "customer/customer.tbl.1", customer_scheme , sep="|", stride = 128 * 1024 * 1024)
    part_csv_reader =  InputS3CSVDataset("tpc-h-csv", "part/part.tbl.1", part_scheme , sep="|", stride = 128 * 1024 * 1024)
    supplier_csv_reader = InputS3CSVDataset("tpc-h-csv", "supplier/supplier.tbl.1", supplier_scheme , sep="|", stride = 128 * 1024 * 1024)
    lineitem = task_graph.new_input_reader_node(lineitem_csv_reader,batch_func = lineitem_filter)
    orders = task_graph.new_input_reader_node(orders_csv_reader, batch_func = orders_filter)
    customer = task_graph.new_input_reader_node(customer_csv_reader,batch_func = customer_filter)
    part = task_graph.new_input_reader_node(part_csv_reader, batch_func = part_filter)
    supplier = task_graph.new_input_reader_node(supplier_csv_reader, batch_func =supplier_filter)
elif sys.argv[1] == "parquet":
    lineitem_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","lineitem.parquet", columns = ["l_partkey","l_orderkey","l_suppkey", "l_extendedprice","l_discount"])
    orders_parquet_reader =InputMultiParquetDataset("tpc-h-parquet","orders.parquet", columns = ["o_orderkey","o_custkey", "o_orderdate"],
    filters= [('o_orderdate','<',compute.strptime("1996-12-31",format="%Y-%m-%d",unit="s")),('o_orderdate','>',compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s"))])
    customer_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","customer.parquet", columns = ["c_custkey","c_nationkey"])
    part_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","part.parquet", columns = ["p_partkey"], filters = [('p_type','==','ECONOMY ANODIZED STEEL')])
    supplier_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","supplier.parquet", columns = ["s_suppkey","s_nationkey"])
    lineitem = task_graph.new_input_reader_node(lineitem_parquet_reader)
    orders = task_graph.new_input_reader_node(orders_parquet_reader)
    customer = task_graph.new_input_reader_node(customer_parquet_reader)
    part = task_graph.new_input_reader_node(part_parquet_reader)
    supplier = task_graph.new_input_reader_node(supplier_parquet_reader)


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
filtered_customer = task_graph.new_non_blocking_node({0:customer},bjoin)
join1 = PolarJoinExecutor(left_on="p_partkey",right_on="l_partkey",columns =["l_suppkey", "l_orderkey","l_extendedprice","l_discount"])
part_lineitem = task_graph.new_non_blocking_node({0:part, 1:lineitem},join1, partition_key_supplied={0:"p_partkey",1:"l_partkey"})
join2 = PolarJoinExecutor(left_on = "l_orderkey", right_on="o_orderkey", columns =["l_suppkey", "o_custkey", "o_orderdate", "l_extendedprice","l_discount"])
part_lineitem_orders = task_graph.new_non_blocking_node({0:part_lineitem, 1:orders},join2,partition_key_supplied={0:"l_orderkey",1:"o_orderkey"})
join3 = PolarJoinExecutor(left_on="o_custkey",right_on="c_custkey",columns = ["l_suppkey", "o_orderdate", "l_extendedprice","l_discount"])
part_lineitem_orders_customers = task_graph.new_non_blocking_node({0:part_lineitem_orders, 1:filtered_customer}, join3,partition_key_supplied={0:"o_custkey",1:"c_custkey"})

bjoin2 = BroadcastJoinExecutor(nation, small_on = "n_nationkey",big_on="s_nationkey")
filtered_supplier = task_graph.new_non_blocking_node({0:supplier},bjoin2)

join4 = PolarJoinExecutor(left_on="l_suppkey",right_on="s_suppkey",columns =["o_orderdate", "l_extendedprice","l_discount","n_name"], batch_func = batch_func)
all_nations = task_graph.new_non_blocking_node({0:part_lineitem_orders_customers, 1: filtered_supplier}, join4,partition_key_supplied={0:"l_suppkey",1:"s_suppkey"} )

agg = AggExecutor(final_func=final_func)
result = task_graph.new_blocking_node({0:all_nations},agg,ip_to_num_channel={cluster.leader_private_ip: 1}, partition_key_supplied={0:None})
import time

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(ray.get(result.to_pandas.remote()))
