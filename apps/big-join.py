from pyquokka.df import *
from pyquokka.utils import LocalCluster, QuokkaClusterManager
from schema import *
mode = "S3"
format = "parquet"
disk_path = "/home/ziheng/tpc-h/"
#disk_path = "s3://yugan/tpc-h-out/"
s3_path_parquet = "s3://tpc-h-parquet-100-native-mine/"

import pyarrow as pa
import pyarrow.compute as compute
import polars
polars.Config().set_tbl_cols(10)

manager = QuokkaClusterManager(key_name = "oregon-neurodb", key_location = "/home/ziheng/Downloads/oregon-neurodb.pem")
#cluster = manager.get_cluster_from_json("config.json")
manager.start_cluster("4_cluster.json")
cluster = manager.get_cluster_from_json("4_cluster.json")
qc = QuokkaContext(cluster,4,2)
qc.set_config("fault_tolerance", False)

build = qc.read_parquet("s3://big-join/probe/*")
probe = qc.read_parquet("s3://big-join/probe/*")
build.join(probe, on = "id").
