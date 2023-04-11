#!/usr/bin/env python3
import pyarrow as pa
import numpy as np
from pyarrow.cffi import ffi
c_schema = ffi.new("struct ArrowSchema*")
schema_ptr = int(ffi.cast("uintptr_t", c_schema))
c_array = ffi.new("struct ArrowArray*")
array_ptr = int(ffi.cast("uintptr_t", c_array))
import polars
lineitem = polars.read_parquet("/home/ziheng/tpc-h/lineitem.parquet")
arr = lineitem.to_arrow()["l_tax"].combine_chunks()
arr._export_to_c(array_ptr, schema_ptr)
import ldb, time
a = ldb.NTDigest(20,100,10000)
start = time.time()
for i in range(10):
    a.batch_add_arrow([array_ptr] * 20, [schema_ptr] * 20)
print(a.quantile(0, 0.5))
print(a.quantile(1, 0.1))
print(time.time() - start)
import pyarrow.compute as pac
start = time.time()
print(pac.tdigest(arr, 0.5))
print(time.time() - start)


