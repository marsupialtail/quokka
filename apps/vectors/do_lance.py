import pyquokka 
import polars
import duckdb
import os
import numpy as np
DISK_PATH = "/home/ziheng/vectors/"
import pyarrow as pa
K = 10
from pyquokka.utils import LocalCluster, QuokkaClusterManager, get_cluster_from_ip
manager = QuokkaClusterManager(key_name = "oregon-neurodb", key_location = "/home/ziheng/Downloads/oregon-neurodb.pem")
# cluster = get_cluster_from_ip("52.36.233.173", "/data")
cluster = LocalCluster()
qc = pyquokka.QuokkaContext(cluster)

#fake data
keys = np.random.randint(10000,size=10000)
vec_np = np.random.random(size=(10000, 128))
vecs = polars.from_dict({"key": keys, "vec": vec_np})
products = polars.from_dict({"key": keys, "price": np.random.randint(10000,size=10000)})
vecs.write_parquet(DISK_PATH + "vecs.parquet")
products.write_parquet(DISK_PATH + "products.parquet")

import lance
from lance.vector import vec_to_table
table = vec_to_table(dict(zip(keys, vec_np)))
uri = DISK_PATH + "vec_data.lance"
if not os.path.exists(uri):
    dataset = lance.write_dataset(table, uri)
    dataset.create_index("vector",
                        index_type="IVF_PQ", 
                        num_partitions=256,  # IVF
                        num_sub_vectors=16) 

def find_nearest_polars(data_df, probe_df, data_col, probe_col, k):

    vectors = np.stack(data_df[data_col].to_numpy())
    normalized_vectors = vectors / np.linalg.norm(vectors, axis = 1, keepdims = True)

    query_vecs = np.stack(probe_df[probe_col].to_numpy())
    query_vecs = query_vecs / np.linalg.norm(query_vecs, axis = 1, keepdims = True)

    distances = np.dot(normalized_vectors, query_vecs.T)
    indices = np.argsort(distances, axis = 0)[-k:].T.flatten()
    return data_df[indices]

def do_parquet():

    # the Quokka way. Parquet doesn't have indices, so this will be an exact nearest neighbor search. Fine for things with less than 100K rows, even on local machine.

    probe_df = polars.from_dict({"key":np.arange(10), "probe_vec": np.random.random(size=(10, 128))})
    vecs = qc.read_parquet(DISK_PATH + "vecs.parquet")
    results = vecs.vector_nn_join(probe_df, vec_column_left = "vec", vec_column_right = "probe_vec", k = K, probe_side = "right")
    results.explain()
    results = results.collect()
    print(results)

    # the Reference way

    ref_df = polars.read_parquet(DISK_PATH + "vecs.parquet")
    ref_results = find_nearest_polars(ref_df, probe_df, "vec", "probe_vec", K)
    print(ref_results)

    # make sure they are the same

    assert (ref_results["key"] - results["key"]).sum() == 0

def do_parquet_with_filter():

    # you can filter the data before doing the vector nearest neighbor search. This will make sure that you still get 10 neighbors per probe on the filtered DataStream.

    probe_df = polars.from_dict({"key":np.arange(10), "probe_vec": np.random.random(size=(10, 128))})
    vecs = qc.read_parquet(DISK_PATH + "vecs.parquet")
    vecs = vecs.filter_sql("key < 1000")
    results = vecs.vector_nn_join(probe_df, vec_column_left = "vec", vec_column_right = "probe_vec", k = K, probe_side = "right")
    results.explain()
    results = results.collect()
    print(results)

    # the Reference way

    ref_df = polars.read_parquet(DISK_PATH + "vecs.parquet")
    ref_df = ref_df.filter(polars.col("key") < 1000)
    ref_results = find_nearest_polars(ref_df, probe_df, "vec", "probe_vec", K)
    print(ref_results)

    assert (ref_results["key"] - results["key"]).sum() == 0

def do_parquet_with_filter_and_join():

    # you can filter and join the data before doing the vector nearest neighbor search. This will make sure that you still get 10 neighbors per probe on the filtered DataStream.
    # this is what to do if you are doing recommendations on cheap stuff!

    probe_df = polars.from_dict({"key":np.arange(10), "probe_vec": np.random.random(size=(10, 128))})
    vecs = qc.read_parquet(DISK_PATH + "vecs.parquet")
    products = qc.read_parquet(DISK_PATH + "products.parquet")
    vecs = vecs.join(products, on = "key")
    vecs = vecs.filter_sql("price < 1000")
    results = vecs.vector_nn_join(probe_df, vec_column_left = "vec", vec_column_right = "probe_vec", k = K, probe_side = "right")
    results.explain()
    results = results.collect()
    print(results)

    # the Reference way

    ref_df = polars.read_parquet(DISK_PATH + "vecs.parquet")
    products = polars.read_parquet(DISK_PATH + "products.parquet")
    ref_df = ref_df.join(products, on = "key")
    ref_df = ref_df.filter(polars.col("price") < 1000)
    ref_results = find_nearest_polars(ref_df, probe_df, "vec", "probe_vec", K)
    print(ref_results)

    assert (ref_results["key"] - results["key"]).sum() == 0

def do_lance():
    import lance
    ref = lance.dataset(DISK_PATH + "vec_data.lance")
    sample = duckdb.query("SELECT vector FROM ref USING SAMPLE 10").to_df()
    query_vectors = np.array([np.array(x) for x in sample.vector])
 
    probe_df = polars.from_dict({"key":np.arange(10), "probe_vec": query_vectors})
    vecs = qc.read_lance(DISK_PATH + "vec_data.lance", "vector")

    # this will automatically use the Lance index if one exists!
    results = vecs.vector_nn_join(probe_df, vec_column_left = "vector", vec_column_right = "probe_vec", k = K, probe_side = "right")
    results.explain()
    a = results.collect()["id"].sort()

    rs = [ref.to_table(nearest={"column": "vector", "k": 10, "q": q}) for q in query_vectors]
    b = polars.from_arrow(pa.concat_tables(rs)["id"]).sort()
    
    assert (a - b).sum() == 0

def do_lance_with_filter():

    import lance
    ref = lance.dataset(DISK_PATH + "vec_data.lance")
    sample = duckdb.query("SELECT vector FROM ref USING SAMPLE 10").to_df()
    query_vectors = np.array([np.array(x) for x in sample.vector])
 
    probe_df = polars.from_dict({"key":np.arange(10), "probe_vec": query_vectors})
    vecs = qc.read_lance(DISK_PATH + "vec_data.lance", "vector")
    vecs = vecs.filter_sql("id < 1000")
    # this CAN NO LONGER USE THE INDEX! Since the vector index has to be done before the filter, this will change the meaning of the query.
    results = vecs.vector_nn_join(probe_df, vec_column_left = "vector", vec_column_right = "probe_vec", k = 5, probe_side = "right")
    results.explain()
    a = results.collect()["id"].sort()

    print(a)


def do_lance_s3(N):

    print("Email me or at least ping me on Discord if you are intending to use this on S3 dataset.")
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

# do_lance()
# do_parquet()
# do_parquet_with_filter()
# do_parquet_with_filter_and_join()
do_lance()
do_lance_with_filter()
