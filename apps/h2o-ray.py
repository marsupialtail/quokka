from pyquokka.df import * 
from pyquokka.utils import LocalCluster, QuokkaClusterManager

mode = "S3"
format = "csv"
manager = QuokkaClusterManager()
cluster = manager.get_cluster_from_json("config.json")
# cluster = LocalCluster()
qc = QuokkaContext(cluster)

left = qc.read_csv("s3://h2oai-benchmark/left.csv", has_header = True)
small = qc.read_csv("s3://h2oai-benchmark/small.csv", has_header = True)
medium = qc.read_csv("s3://h2oai-benchmark/medium.csv", has_header = True)

# left = qc.read_csv("/home/ziheng/db-benchmark/data/J1_1e7_NA_0_0.csv", has_header = True)
# small = qc.read_csv("/home/ziheng/db-benchmark/data/J1_1e7_1e1_0_0.csv", has_header = True)
# medium = qc.read_csv("/home/ziheng/db-benchmark/data/J1_1e7_1e4_0_0.csv", has_header = True)

result = left.join(small, on = "id1").compute()
#result = left.count()
print(result)
#arrow_refs = result.to_arrow_refs()
