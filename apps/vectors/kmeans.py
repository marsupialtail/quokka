import numpy as np
import torch
import time
from tqdm import tqdm
f = open('data.fbin','rb')

# there will be one billion rows, each with length 400 bytes, total 400GB
# we will load 4GB at a time into memory
# we will cluster them into 10K centroids

TOTAL = 1000000000
K = 10000
B = 5000000

print("generating zeros")
dt = np.dtype(np.float32)
dt = dt.newbyteorder('<')
print("generating choices")

#choices = np.random.choice(TOTAL, K, replace = False)

choices = np.random.choice(B, K, replace = False)
centroids = []
for choice in choices:
    f.seek(choice * 100 * 400)
    buf = f.read(400)
    centroids.append(np.frombuffer(buf, dtype = dt))

print("done with centroids")
# shape will be K * 100
centroids = np.vstack(centroids)
transposed_centroids = np.transpose(centroids)

running_centroids = np.zeros(centroids.shape)
running_sums = np.zeros((K, ))

for iter in range(20):
    # expectation
    f.seek(0)
    transposed_centroids = torch.from_numpy(transposed_centroids).cuda().half()

    # each time we will try to select B vectors total. This will be done by selecting K starting points and selecting 1000 vectors from each starting point. 
    choices = np.random.choice(TOTAL//10000, B//10000, replace = False)
    projected_choices = choices * 10000

    arrays = []
    for choice in tqdm(projected_choices):
        f.seek(choice * 400)
        buf = f.read(400 * 10000)
        arrays.append(np.frombuffer(buf, dtype = dt).reshape(10000, 100))

    # shape will be B * 100
    vectors = np.vstack(arrays)
    print(vectors.shape)
    vectors = torch.from_numpy(vectors).pin_memory()

    start = time.time()
    bump = []
    for b in range(0, B, 100000):
        loaded = vectors[b : b + 100000].cuda(non_blocking = True).half()
        product = torch.matmul(loaded, transposed_centroids)
        assignment = torch.argmax(product, axis = 1).cpu().numpy()
        bump.append(assignment)

    # shape will be (B,)
    assignment = np.hstack(bump)
    print(time.time() - start)

    # maximization
    current_centroids = np.zeros((K , 100))
    current_sums = np.zeros((K, ))

    vectors = vectors.numpy()
    # we basically want to do a sum(vector) from vectors group by assignment
    print(len(np.unique(assignment)))

    indices = np.argsort(assignment)
    assignment = assignment.take(indices)
    vectors = vectors.take(indices, axis = 0)
    offsets = list(np.where(assignment != np.roll(assignment, 1))[0])
    offsets.append(len(assignment))

    for k in range(len(offsets) - 1):
        start = offsets[k]
        end = offsets[k + 1]
        cluster = assignment[start]
        current_centroids[int(cluster)] += np.sum(vectors[int(start) : int(end)], axis = 0)
        current_sums[int(cluster)] += end - start

    running_centroids += current_centroids
    running_sums += current_sums
    for i in range(K):
        centroids[i] = running_centroids[i] / running_sums[i]

    transposed_centroids = np.transpose(centroids)

    np.save("iter" + str(iter) + ".npy", centroids)

