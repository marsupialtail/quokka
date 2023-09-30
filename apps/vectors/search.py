import numpy as np
from pyarrow.fs import S3FileSystem
import ray
from pyquokka.utils import QuokkaClusterManager
from pyquokka.df import * 

manager = QuokkaClusterManager()
cluster = manager.get_cluster_from_json("config.json")
qc = QuokkaContext(cluster)

@ray.remote
def search_partition(vectors, assignments):
    old_partition = -1
    partition = None
    dt = np.dtype(np.float32)
    dt = dt.newbyteorder('<')
    s3 = boto3.client('s3', region_name = 'us-west-1')
    results = []
    for i in range(len(vectors)):
        vector = vectors[i]
        assignment = assignments[i].item()
        if assignment != old_partition:
            old_partition = assignment
            partition = None
            print(assignment)
            b = s3.get_object(Bucket='vectors-and-shit', Key='partitioned/split-' + str(assignment) + '.bin')['Body'].read()
            partition = np.frombuffer(b, dtype = dt)
            l = partition.shape[0]
            assert l % 100 == 0
            partition = partition.reshape((l // 100, 100))
        
        closest = np.argmax(np.dot(partition, vector))
        results.append(np.copy(partition[closest]))
    return results

Q = 10000
WORKERS = 32
WORKERS_PER_MACHINE = 8
queries = np.random.normal(size=(Q, 100))
queries = queries / np.linalg.norm(queries, axis = 1).reshape((Q, 1))

start = time.time()
s3fs = S3FileSystem(region = 'us-west-1')
index = s3fs.open_input_file('vectors-and-shit/index.npy')
index_npy = np.load(index)
print(index_npy)

product = np.dot(queries, np.transpose(index_npy))
assignment = np.argmax(product, axis = 1)

shuffled = np.argsort(assignment)
assignment = assignment[shuffled]
queries = queries[shuffled]

print(assignment)

vectors_per_worker = (Q - 1) // WORKERS + 1
futures = {}
private_ips = list(cluster.private_ips.values())
for worker in range(WORKERS):
    ip = private_ips[worker // WORKERS_PER_MACHINE]
    futures[worker] = search_partition.options(resources={"node:" + ip : 0.001}).remote(queries[worker * vectors_per_worker : worker * vectors_per_worker + vectors_per_worker], \
            assignment[worker * vectors_per_worker : worker * vectors_per_worker + vectors_per_worker])

results = []
for worker in range(WORKERS):
    results.extend(ray.get(futures[worker]))

# print(queries)
# print(results)
# print(np.linalg.norm(queries - results, axis = 1))

print(time.time() - start)