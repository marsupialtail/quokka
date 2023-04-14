import ldb
import polars

cep = ldb.CEP([5,5])
from pyarrow.cffi import ffi

ts = polars.from_dict({"ts":[0,1,2,3,4,5,6]}, schema={"ts":polars.UInt64()}).to_arrow()['ts']
a = polars.from_dict({"a":[0]}, schema={"a":polars.UInt64()}).to_arrow()['a']
b = polars.from_dict({"b":[1,4]}, schema={"b":polars.UInt64()}).to_arrow()['b']
c = polars.from_dict({"c":[2,5,6]}, schema={"c":polars.UInt64()}).to_arrow()['c']
arrs = [ts, a, b, c]

array_ptrs = []
schema_ptrs = []
c_schemas = []
c_arrays = []
list_of_arrs = []
for i, col in enumerate(arrs):
    c_schema = ffi.new("struct ArrowSchema*")
    c_array = ffi.new("struct ArrowArray*")
    c_schemas.append(c_schema)
    c_arrays.append(c_array)
    schema_ptr = int(ffi.cast("uintptr_t", c_schema))
    array_ptr = int(ffi.cast("uintptr_t", c_array))
    list_of_arrs.append(col.combine_chunks())
    list_of_arrs[-1]._export_to_c(array_ptr, schema_ptr)
    # arr = arrow_batch[col].combine_chunks()
    # arr._export_to_c(array_ptr, schema_ptr)
    # self.state[i].add_arrow(array_ptr, schema_ptr)
    array_ptrs.append(array_ptr)
    schema_ptrs.append(schema_ptr)

# start = time.time()
# print(array_ptrs, schema_ptrs)
result = cep.do_arrow_batch(array_ptrs, schema_ptrs)
print(result)
