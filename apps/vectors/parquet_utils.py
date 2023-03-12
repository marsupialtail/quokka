import numpy as np
import pyarrow as pa

N = 1000
D = 100

data = np.random.normal(size=(N,D)).astype(np.float32)
z = [data[i].tobytes() for i in range(N)]
table = pa.Table.from_pydict({"da": z})

# how to recover a float vector
# dt = np.dtype(np.float32)
# dt = dt.newbyteorder('<')
# np.frombuffer(table["da"][0].as_py(), dtype = dt)