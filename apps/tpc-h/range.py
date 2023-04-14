from pyquokka.df import * 
from pyquokka.utils import LocalCluster, QuokkaClusterManager

manager = QuokkaClusterManager(key_name = "oregon-neurodb", key_location = "/home/ziheng/Downloads/oregon-neurodb.pem")
cluster = manager.get_cluster_from_json("config.json")
#cluster = LocalCluster()

qc = QuokkaContext(cluster, 4, 2)

trades = qc.read_sorted_parquet("s3://quokka-asof-parquet/trades/*", sorted_by = "time")
quotes = qc.read_sorted_parquet("s3://quokka-asof-parquet/quotes/*", sorted_by = "time")

joined = trades.join_asof(quotes, on = "time", by = "symbol")
z = joined.sum("asize")
print(z)
# z = joined.count(collect=False)
# z.explain()

# print(trades.count())
