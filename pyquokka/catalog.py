import pyarrow
import duckdb
import ray
import sqlglot

@ray.remote
class Catalog:
    def __init__(self) -> None:
        self.table_id = 0
        self.tables = {}
        self.con = duckdb.connect().execute('PRAGMA threads=%d' % 8)
    
    def register_sample(self, sample):
        assert type(sample) == pyarrow.Table
        self.tables[self.table_id] = sample
        self.table_id += 1
        return self.table_id - 1
    
    def estimate_cardinality(self, table_id, predicate):
        assert type(predicate) == 
        batch_arrow = self.tables[table_id]
        self.con.execute("select count(*) as c from batch_arrow where " + predicate.sql())
        