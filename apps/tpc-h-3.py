import sys
import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.executors import AggExecutor, PolarJoinExecutor
from pyquokka.dataset import InputS3CSVDataset, InputMultiParquetDataset
from schema import * 
import pyarrow.compute as compute
import polars
import ray
from pyquokka.utils import LocalCluster, QuokkaClusterManager

manager = QuokkaClusterManager()
cluster = manager.get_cluster_from_json("config.json")
#cluster = LocalCluster()

task_graph = TaskGraph(cluster)

def batch_func2(df):
    df["product"] = df["l_extendedprice"] * (1 - df["l_discount"])
    return df.groupby(["o_orderkey", "o_orderdate", "o_shippriority"]).agg(revenue = ('product','sum'))

def final_func(state):
    return state.sort_values(['revenue','o_orderdate'],ascending = [False,True])[:10]

orders_filter = lambda x: polars.from_arrow(x.filter(compute.less(x['o_orderdate'] , compute.strptime("1995-03-03",format="%Y-%m-%d",unit="s"))).select(["o_orderkey","o_custkey","o_shippriority", "o_orderdate"]))
lineitem_filter = lambda x: polars.from_arrow(x.filter(compute.greater(x['l_shipdate'] , compute.strptime("1995-03-15",format="%Y-%m-%d",unit="s"))).select(["l_orderkey","l_extendedprice","l_discount"]))
customer_filter = lambda x: polars.from_arrow(x.filter(compute.equal(x["c_mktsegment"] , 'BUILDING')).select(["c_custkey"]))

if sys.argv[1] == "csv":
    
    lineitem_csv_reader = InputS3CSVDataset("tpc-h-csv", lineitem_scheme ,  key = "lineitem/lineitem.tbl.1",sep="|", stride = 128 * 1024 * 1024)
    orders_csv_reader = InputS3CSVDataset("tpc-h-csv", order_scheme , key = "orders/orders.tbl.1", sep="|", stride = 128 * 1024 * 1024)
    customer_csv_reader = InputS3CSVDataset("tpc-h-csv", customer_scheme , key = "customer/customer.tbl.1", sep="|", stride = 128 * 1024 * 1024)

    lineitem = task_graph.new_input_reader_node(lineitem_csv_reader,batch_func = lineitem_filter)
    orders = task_graph.new_input_reader_node(orders_csv_reader, batch_func = orders_filter)
    customer = task_graph.new_input_reader_node(customer_csv_reader,batch_func = customer_filter)

else:

    lineitem_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","lineitem.parquet", columns = ["l_orderkey","l_extendedprice","l_discount"], filters= [('l_shipdate','>',compute.strptime("1995-03-15",format="%Y-%m-%d",unit="s"))])
    orders_parquet_reader =InputMultiParquetDataset("tpc-h-parquet","orders.parquet", columns = ["o_orderkey","o_custkey","o_shippriority", "o_orderdate"], filters= [('o_orderdate','<',compute.strptime("1995-03-03",format="%Y-%m-%d",unit="s"))])
    customer_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","customer.parquet", columns = ["c_custkey"], filters= [("c_mktsegment","==","BUILDING")])

    lineitem = task_graph.new_input_reader_node(lineitem_parquet_reader)
    orders = task_graph.new_input_reader_node(orders_parquet_reader)
    customer = task_graph.new_input_reader_node(customer_parquet_reader)
    

# join order picked by hand, might not be  the best one!
join_executor1 = PolarJoinExecutor(left_on = "c_custkey", right_on = "o_custkey",columns=["o_orderkey", "o_orderdate", "o_shippriority"])
join_executor2 = PolarJoinExecutor(left_on="o_orderkey",right_on="l_orderkey",batch_func=batch_func2)
temp = task_graph.new_non_blocking_node({0:customer,1:orders},join_executor1,partition_key_supplied={0:"c_custkey", 1:"o_custkey"})
joined = task_graph.new_non_blocking_node({0:temp, 1: lineitem},join_executor2,partition_key_supplied={0: "o_orderkey", 1:"l_orderkey"})

agg_executor = AggExecutor(fill_value=0, final_func=final_func)
agged = task_graph.new_blocking_node({0:joined},  agg_executor, ip_to_num_channel={cluster.leader_private_ip: 1}, partition_key_supplied={0:None})

task_graph.create()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(agged.to_pandas())
