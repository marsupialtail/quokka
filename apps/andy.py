from pyquokka.df import * 
from pyquokka.utils import LocalCluster, QuokkaClusterManager
import pyarrow as pa
import pyarrow.compute as compute
import polars
import numpy as np
polars.Config().set_tbl_cols(10)

# manager = QuokkaClusterManager(key_name = "oregon-neurodb", key_location = "/home/ziheng/Downloads/oregon-neurodb.pem")
# cluster = manager.get_cluster_from_json("ray_config.json")
# qc = QuokkaContext(cluster,2,2)

qc = QuokkaContext()
qc.set_config("fault_tolerance", False)

# andy_data = qc.read_parquet("s3://andy-demo-floats/*")

andy_data = qc.read_csv("/home/ziheng/tpc-h/floats.csv", has_header = True)
z = andy_data.approximate_quantile(andy_data.schema, [0.1, 0.9]).collect()
print(z)
clipped = andy_data.clip(z.to_dict())
# print(andy_data.covariance(andy_data.schema))
print(clipped.covariance(clipped.schema))
