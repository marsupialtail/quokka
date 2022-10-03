import ray
import sys
import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.dataset import InputS3CSVDataset, InputMultiParquetDataset
from pyquokka.executors import AggExecutor
from schema import * 
import pandas as pd
import pyarrow.compute as compute
from pyquokka.utils import LocalCluster, QuokkaClusterManager

manager = QuokkaClusterManager()
cluster = manager.get_cluster_from_json("config.json")
#cluster = LocalCluster()
task_graph = TaskGraph(cluster)


def lineitem_filter(df):
    
    filtered_df = df.filter(compute.and_(compute.and_(compute.greater(df["l_shipdate"], compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s")), compute.greater_equal(df["l_discount"], 0.05)), compute.and_(compute.less_equal(df["l_discount"],0.07), compute.less(df["l_quantity"], 24)))).to_pandas()
    return pd.DataFrame([ (filtered_df.l_extendedprice * filtered_df.l_discount).sum()],columns=['sum'])

def lineitem_filter_parquet(df):
    
    filtered_df = df.to_pandas()
    return pd.DataFrame([ (filtered_df.l_extendedprice * filtered_df.l_discount).sum()],columns=['sum'])

if sys.argv[2] == "csv":
    if sys.argv[1] == "small":
        lineitem_csv_reader = InputS3CSVDataset("tpc-h-small", lineitem_scheme , key="lineitem.tbl", sep="|")
        lineitem = task_graph.new_input_reader_node(lineitem_csv_reader, batch_func = lineitem_filter)
    else:
        lineitem_csv_reader = InputS3CSVDataset("tpc-h-csv", lineitem_scheme , key="lineitem/lineitem.tbl.1", sep="|")
        lineitem = task_graph.new_input_reader_node(lineitem_csv_reader, batch_func=lineitem_filter)

elif sys.argv[2] == "parquet":
    if sys.argv[1] == "small":
        raise Exception("not implemented")
    else:
        lineitem_parquet_reader = InputMultiParquetDataset("tpc-h-parquet","lineitem.parquet",columns=["l_discount","l_extendedprice"], filters = [('l_shipdate','>',compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s")), ('l_discount','>=',0.05), ('l_discount','<=',0.07),('l_quantity','<',24)])
        lineitem = task_graph.new_input_reader_node(lineitem_parquet_reader,batch_func=lineitem_filter_parquet)

agg_executor = AggExecutor()
agged = task_graph.new_blocking_node({0:lineitem},  agg_executor, ip_to_num_channel={cluster.leader_private_ip: 1}, partition_key_supplied={0:None})

task_graph.create()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)
print(agged.to_pandas())
