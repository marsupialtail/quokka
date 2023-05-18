import sys
import time
from pyquokka import QuokkaContext
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.executors import SQLAggExecutor, BuildProbeJoinExecutor
from pyquokka.dataset import InputDiskCSVDataset, InputS3CSVDataset, InputParquetDataset, InputEC2ParquetDataset
import boto3
import pyarrow.compute as compute
from pyquokka.target_info import BroadcastPartitioner, HashPartitioner, TargetInfo
from pyquokka.placement_strategy import * 
from pyquokka.utils import LocalCluster, QuokkaClusterManager
import polars
import ray
import sqlglot

manager = QuokkaClusterManager(key_name = "oregon-neurodb", key_location = "/home/ziheng/Downloads/oregon-neurodb.pem")
cluster = manager.get_cluster_from_json("config.json")
cluster.tag_instance("172.31.36.8", "A")
cluster.tag_instance("172.31.40.55", "A")
cluster.tag_instance("172.31.36.75", "B")
cluster.tag_instance("172.31.47.89", "")
# cluster = LocalCluster()
qc = QuokkaContext(cluster)
# qc.set_config("memory_limit", 0.01)

task_graph = TaskGraph(qc)

def batch_func(df):
    df = df.with_columns(((df["o_orderpriority"] == "1-URGENT") | (df["o_orderpriority"] == "2-HIGH")).alias("high"))
    df = df.with_columns(((df["o_orderpriority"] != "1-URGENT") & (df["o_orderpriority"] != "2-HIGH")).alias("low"))
    result = df.to_arrow().group_by("l_shipmode").aggregate([("high","sum"), ("low","sum")])
    return polars.from_arrow(result)


if sys.argv[1] == "csv":

    lineitem_csv_reader = InputDiskCSVDataset("/home/ziheng/Downloads/demo-tpch/lineitem.tbl", header = True, sep = "|", stride=16 * 1024 * 1024)
    orders_csv_reader = InputDiskCSVDataset("/home/ziheng/Downloads/demo-tpch/orders.tbl", header = True, sep = "|", stride=16 * 1024 * 1024)
    # lineitem_csv_reader = InputS3CSVDataset("tpc-h-csv", lineitem_scheme , key = "lineitem/lineitem.tbl.1", sep="|", stride = 128 * 1024 * 1024)
    # orders_csv_reader = InputS3CSVDataset("tpc-h-csv",  order_scheme , key ="orders/orders.tbl.1",sep="|", stride = 128 * 1024 * 1024)
    lineitem = task_graph.new_input_reader_node(lineitem_csv_reader, stage = -1)
    orders = task_graph.new_input_reader_node(orders_csv_reader)

elif sys.argv[1] == "parquet":
    s3 = boto3.client('s3')
    z = s3.list_objects_v2(Bucket="tpc-h-parquet-100-native-mine", Prefix="lineitem.parquet")
    files = ["tpc-h-parquet-100-native-mine/" + i['Key'] for i in z['Contents'] if i['Key'].endswith(".parquet")]
    lineitem_parquet_reader = InputEC2ParquetDataset(files,columns=['l_shipdate','l_commitdate','l_shipmode','l_receiptdate','l_orderkey'], filters= [('l_shipmode', 'in', ['SHIP','MAIL']),('l_receiptdate','<',compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s")), ('l_receiptdate','>=',compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s"))])
    z = s3.list_objects_v2(Bucket="tpc-h-parquet-100-native-mine", Prefix="orders.parquet")
    files = ["tpc-h-parquet-100-native-mine/" + i['Key'] for i in z['Contents'] if i['Key'].endswith(".parquet")]
    orders_parquet_reader = InputEC2ParquetDataset(files,columns = ['o_orderkey','o_orderpriority'])
    lineitem = task_graph.new_input_reader_node(lineitem_parquet_reader, stage = -1, placement_strategy = TaggedCustomChannelsStrategy(2, "A"))
    orders = task_graph.new_input_reader_node(orders_parquet_reader, placement_strategy = TaggedCustomChannelsStrategy(2, "B"))
      

join_executor = BuildProbeJoinExecutor(left_on="o_orderkey",right_on="l_orderkey")

if sys.argv[1] == "csv":
    output_stream = task_graph.new_non_blocking_node({0:orders,1:lineitem},join_executor,
        source_target_info={0:TargetInfo(partitioner = HashPartitioner("o_orderkey"), 
                                        predicate = None,
                                        projection = ["o_orderkey", "o_orderpriority"],
                                        batch_funcs = []), 
                            1:TargetInfo(partitioner = HashPartitioner("l_orderkey"),
                                        predicate = sqlglot.parse_one("l_shipmode IN ('MAIL','SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and \
            l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1995-01-01'"),
                                        projection = ["l_orderkey","l_shipmode"],
                                        batch_funcs = [])})
else:
    output_stream = task_graph.new_non_blocking_node({0:orders,1:lineitem},join_executor,
        source_target_info={0:TargetInfo(partitioner = HashPartitioner("o_orderkey"), 
                                        predicate = None,
                                        projection = None,
                                        batch_funcs = []), 
                            1:TargetInfo(partitioner = HashPartitioner("l_orderkey"),
                                        predicate = sqlglot.parse_one("l_commitdate < l_receiptdate and l_shipdate < l_commitdate "),
                                        projection = ["l_orderkey","l_shipmode"],
                                        batch_funcs = [])})

agg_executor = SQLAggExecutor(["l_shipmode"], None, "sum(high_sum) as high, sum(low_sum) as low")

agged = task_graph.new_blocking_node({0:output_stream},  agg_executor, placement_strategy = SingleChannelStrategy(), 
    source_target_info={0:TargetInfo(
        partitioner = BroadcastPartitioner(),
        predicate = None,
        projection = None,
        batch_funcs = [batch_func]
    )})

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(ray.get(qc.dataset_manager.to_df.remote(agged)))
