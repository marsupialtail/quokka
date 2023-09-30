from pyquokka import QuokkaContext
from pyquokka.utils import LocalCluster, QuokkaClusterManager
from pyarrow.fs import S3FileSystem
from pyquokka.target_info import HashPartitioner
from pyquokka.executors import Executor
import pyarrow.parquet as pq
import pyarrow as pa
import polars

manager = QuokkaClusterManager(key_name = "oregon-neurodb", key_location = "/home/ziheng/Downloads/oregon-neurodb.pem")
# manager.start_cluster("config.json")
cluster = manager.get_cluster_from_json("config_2.json")
qc = QuokkaContext(cluster, 2, 2)

class Builder(Executor):

    def __init__(self):
        self.state = {}

    def execute(self,batches, stream_id, executor_id):
        batch = polars.from_arrow(pa.concat_tables(batches))
        d = batch.partition_by("symbol", as_dict = True)
        for symbol in d:
            if symbol not in self.state:
                self.state[symbol] = d[symbol]
            else:
                self.state[symbol].vstack(d[symbol], in_place = True)
    
    def done(self,executor_id):
        result = []
        s3fs = S3FileSystem()
        for symbol in self.state:
            table = self.state[symbol].sort(["date", "candle"])
            # partition the table by year
            table = table.with_columns(polars.col("date").str.slice(0,4).alias("year"))
            # now partiion by year
            tables = table.partition_by("year", as_dict = True)
            for year in tables:
                table = tables[year]
                table_name = "nasdaq-tick-data/ohlcv-1min-by-symbol/{}/{}.parquet".format(year, symbol)
                pq.write_table(table.to_arrow(), table_name, filesystem = s3fs)
            result.append(symbol)
        return polars.from_dict({"written": result})

executor = Builder()
df = qc.read_parquet("s3://nasdaq-tick-data/ohlcv-1min-by-date/*", nthreads=4, name_column="date")
df = df.stateful_transform(executor, ["written"], set(df.schema), HashPartitioner("symbol"))
print(df.collect())