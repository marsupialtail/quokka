import pyquokka 
import polars
import numpy as np
DISK_PATH = "/home/ziheng/vectors/"

K = 10

qc = pyquokka.QuokkaContext()
qc.set_config("memory_limit", 0.01)
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
