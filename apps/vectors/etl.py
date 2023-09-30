import torch
import numpy as np
from tqdm import tqdm
import pyarrow.parquet as pq
import pyarrow as pa
centroids = np.load("iter49.npy")

f = open('data.fbin','rb')
dt = np.dtype(np.float32)
dt = dt.newbyteorder('<')

transposed_centroids = torch.from_numpy(np.transpose(centroids)).cuda().half()

TOTAL = 1000000000
K = 10000
B = 250000


for start in tqdm(range(0, TOTAL * 400, B * 400)):
    buf = f.read(B * 400)
    vectors = np.frombuffer(buf, dtype = dt).reshape(B, 100)
    data = torch.from_numpy(vectors).cuda().half()
    product = torch.matmul(data, transposed_centroids)
    assignment = torch.argmax(product, axis = 1).cpu().numpy()

    indices = np.argsort(assignment)
    assignment = assignment.take(indices)
    vectors = vectors.take(indices, axis = 0)
    offsets = list(np.where(assignment != np.roll(assignment, 1))[0])
    offsets.append(len(assignment))

    for k in range(len(offsets) - 1):
        start = offsets[k]
        end = offsets[k + 1]
        cluster = assignment[start]
        w = open("splits/split-" + str(int(cluster)) + ".bin", "ab")
        w.write(vectors[int(start) : int(end)].tobytes())
        w.close()

for k in range(K):
    w = open("splits/split-" + str(int(k)) + ".bin", "rb")
    buf = w.read()
    vectors = np.frombuffer(buf, dtype = dt).reshape(-1, 100)

    key = np.random.choice(1000000, len(vectors), replace=False)
    vectors = [data[i].tobytes() for i in range(len(vectors))]
    table = pa.Table.from_pydict({"key" : key, "embedding": vectors})

    pq.write_table(table, "splits/split-" + str(int(k)) + ".parquet")        