import pyarrow.parquet as pq
import polars
import ray
        

@ray.remote
class Catalog:
    def __init__(self) -> None:
        self.tables = {}