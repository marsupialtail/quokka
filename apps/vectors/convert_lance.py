import lance
import numpy as np
from lance.vector import vec_to_table
from tqdm import tqdm

f = open('data.fbin','rb')
dt = np.dtype(np.float32)
dt = dt.newbyteorder('<')


TOTAL = 1000000000
K = 10000
B = 250000

keys = np.arange(TOTAL)

count = 0
for start in tqdm(range(0, TOTAL * 400, B * 400)):
    buf = f.read(B * 400)
    vectors = np.frombuffer(buf, dtype = dt).reshape(B, 100)
    keys = np.random.randint(TOTAL,size=(B))
    dd = dict(zip(keys, vectors))
    table = vec_to_table(dd)
    uri = "vec_data_{}.lance".format(count)
    count += 1
    sift1m = lance.write_dataset(table, uri, max_rows_per_group=8192, max_rows_per_file=1024*1024)
    sift1m.create_index("vector",
                    index_type="IVF_PQ", 
                    num_partitions=256,  # IVF
                    num_sub_vectors=16) 