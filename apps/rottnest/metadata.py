
from pyquokka import QuokkaContext
from pyquokka.utils import LocalCluster, QuokkaClusterManager
from pyquokka.target_info import HashPartitioner
from pyquokka.executors import Executor
from pyquokka.placement_strategy import CustomChannelsStrategy

from pyarrow.fs import S3FileSystem
import pyarrow.parquet as pq
import polars
import boto3
import pyarrow as pa

manager = QuokkaClusterManager(key_name = "zihengw", key_location = "/home/ziheng/Downloads/zihengw.pem")
# manager.start_cluster("config_2.json")
cluster = manager.get_cluster_from_json("config_2.json")
qc = QuokkaContext(cluster, 2, 4)

s3 = boto3.client('s3')
z = s3.list_objects_v2(Bucket="nasdaq-tick-data", Prefix = "yearly-trades-parquet-all/2022")
if 'Contents' not in z:
    raise Exception("Wrong S3 path")
files = ["nasdaq-tick-data" + "/" + i['Key'] for i in z['Contents'] if i['Key'].endswith(".parquet")]
print(len(files))

class ReWriter(Executor):

    def __init__(self):
        self.state = None

    def execute(self,batches, stream_id, executor_id):
        batch = polars.from_arrow(pa.concat_tables(batches))
        files = batch["files"].to_list()
        s3fs = S3FileSystem()
        for file in files:
            table = pq.read_table(file, filesystem = s3fs)
            pq.write_table(table, file, filesystem = s3fs)
        return polars.from_dict({"written": files})
    
    def done(self,executor_id):
        return

rewriter = ReWriter()
df = qc.from_polars(polars.from_dict({"files": files}))
df = df.stateful_transform(rewriter, new_schema = ["written"], required_columns={"files"}, partitioner=HashPartitioner("files"), 
                           placement_strategy = CustomChannelsStrategy(1))
print(df.collect())