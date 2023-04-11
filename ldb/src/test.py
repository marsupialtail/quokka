#!/usr/bin/env python3
import pyarrow as pa
import numpy as np
from pyarrow.cffi import ffi
c_schema = ffi.new("struct ArrowSchema*")
schema_ptr = int(ffi.cast("uintptr_t", c_schema))
c_array = ffi.new("struct ArrowArray*")
array_ptr = int(ffi.cast("uintptr_t", c_array))
import polars
# lineitem = polars.read_parquet("/home/ziheng/tpc-h/lineitem.parquet")
# arr = lineitem.to_arrow()["l_tax"].combine_chunks()
test1 = polars.from_dict({"a":np.random.normal(size=(6001215))/ 100})
arr = test1.to_arrow()["a"].combine_chunks()
print("length of array", len(arr))
import ldb, time
a = ldb.TDigest(100,500)
start = time.time()
arr._export_to_c(array_ptr, schema_ptr)
a.add_arrow(array_ptr, schema_ptr)
print(a.quantile(0.5))
print(time.time() - start)
import pyarrow.compute as pac
start = time.time()
print(pac.tdigest(arr, 0.5))
print(time.time() - start)


