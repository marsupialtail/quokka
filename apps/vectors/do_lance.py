import pyquokka 
import polars
import duckdb
import numpy as np
DISK_PATH = "/home/ziheng/vectors/"
import pyarrow as pa
K = 10
from pyquokka.utils import LocalCluster, QuokkaClusterManager, get_cluster_from_ip
manager = QuokkaClusterManager(key_name = "oregon-neurodb", key_location = "/home/ziheng/Downloads/oregon-neurodb.pem")
#cluster = manager.get_cluster_from_json("config.json")
cluster = manager.get_cluster_from_json("gpu.json")
# cluster = get_cluster_from_ip("52.36.233.173", "/data")
qc = pyquokka.QuokkaContext(cluster)

def do_parquet():
    probe_df = polars.from_dict({"key":np.arange(10), "probe_vec": np.random.random(size=(10, 128))})
    vecs = qc.read_parquet(DISK_PATH + "vecs.parquet")
    results = vecs.nn_probe(probe_df, vec_column_left = "vec", vec_column_right = "probe_vec", k = K)
    results.explain()
    results = results.collect()
    print(results)


    ref_df = polars.read_parquet(DISK_PATH + "vecs.parquet")
    ref_vecs = np.stack(ref_df["vec"].to_numpy())
    ref_vecs = ref_vecs / np.linalg.norm(ref_vecs, axis = 1, keepdims = True)
    probe_vecs = np.stack(probe_df["probe_vec"].to_numpy()) 
    probe_vecs = probe_vecs / np.linalg.norm(probe_vecs, axis = 1, keepdims = True)
    distances = np.dot(ref_vecs, probe_vecs.T)
    indices = np.argsort(distances, axis = 0)[-K:].T.flatten()
    ref_results = ref_df[indices]
    print(ref_results)

    assert (ref_results["key"] - results["key"]).sum() == 0

def do_lance():
    import lance
    ref = lance.dataset("/home/ziheng/vec_data.lance")
    sample = duckdb.query("SELECT vector FROM ref USING SAMPLE 10").to_df()
    query_vectors = np.array([np.array(x) for x in sample.vector])
 
    probe_df = polars.from_dict({"key":np.arange(10), "probe_vec": query_vectors})
    vecs = qc.read_lance("/home/ziheng/vec_data.lance", "vector")
    # vecs = vecs.filter_sql("id < 100000")
    results = vecs.nn_probe(probe_df, vec_column_left = "vector", vec_column_right = "probe_vec", k = K)
    results.explain()
    a = results.collect()["id"].sort()

    rs = [ref.to_table(nearest={"column": "vector", "k": 10, "q": q}) for q in query_vectors]
    b = polars.from_arrow(pa.concat_tables(rs)["id"]).sort()
    
    assert (a - b).sum() == 0

def do_lance_s3(N):
    import lance
    ref = lance.dataset("s3://microsoft-turing-ann/vec_data_0.lance")
    sample = duckdb.query("SELECT vector FROM ref USING SAMPLE {}".format(N)).to_df()
    query_vectors = np.array([np.array(x) for x in sample.vector])
 
    probe_df = polars.from_dict({"key":np.arange(N), "probe_vec": query_vectors})
    # vecs = qc.read_lance("s3://microsoft-turing-ann/*", "vector")
    vecs = qc.read_parquet("s3://microsoft-turing-ann-parquet/*")
    # vecs = vecs.filter_sql("id < 100000")
    results = vecs.nn_probe(probe_df, vec_column_left = "vector", vec_column_right = "probe_vec", k = K)
    results.explain()
    a = results.collect()["id"].sort()

    print(a)


def find_nearest_polars(data_df, probe_df, data_col, probe_col, k):

    vectors = np.stack(data_df[data_col].to_numpy())
    normalized_vectors = vectors / np.linalg.norm(vectors, axis = 1, keepdims = True)

    query_vecs = np.stack(probe_df[probe_col].to_numpy())
    query_vecs = query_vecs / np.linalg.norm(query_vecs, axis = 1, keepdims = True)

    with threadpool_limits(limits=8, user_api='blas'):
        distances = np.dot(normalized_vectors, query_vecs.T)
    indices = np.argsort(distances, axis = 0)[-k:].T.flatten()
    return data_df[indices]

# do_lance()
for i in [10, 100, 1000]:
    do_lance_s3(i)
