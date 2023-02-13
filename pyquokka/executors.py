import os
import polars
import pandas as pd
os.environ['ARROW_DEFAULT_MEMORY_POOL'] = 'system'
import redis
import pyarrow as pa
import time
import numpy as np
import os, psutil
import pyarrow.parquet as pq
import pyarrow.csv as csv
from collections import deque
import pyarrow.compute as compute
import random
import sys
from pyarrow.fs import S3FileSystem, LocalFileSystem
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pickle
import concurrent.futures
import duckdb
import multiprocessing
from pyquokka.windowtypes import *

class Executor:
    def __init__(self) -> None:
        raise NotImplementedError
    def execute(self,batches,stream_id, executor_id):
        raise NotImplementedError
    def done(self,executor_id):
        raise NotImplementedError    

class UDFExecutor:
    def __init__(self, udf) -> None:
        self.udf = udf

    def serialize(self):
        return {}, "all"
    
    def deserialize(self, s):
        pass

    def execute(self,batches,stream_id, executor_id):
        batches = [i for i in batches if i is not None]
        if len(batches) > 0:
            return self.udf(polars.concat(batches, rechunk=False))
        else:
            return None

    def done(self,executor_id):
        return

# this is not fault tolerant. If the storage is lost you just re-read
class StorageExecutor(Executor):
    def __init__(self) -> None:
        pass
    def serialize(self):
        return {}, "all"
    def deserialize(self, s):
        pass
    
    def execute(self,batches,stream_id, executor_id):
        # print("executing storage node")
        batches = [batch for batch in batches if batch is not None and len(batch) > 0]
        #print(batches)
        if len(batches) > 0:
            if type(batches[0]) == polars.internals.DataFrame:
                return polars.concat(batches)
            else:
                return polars.concat([polars.from_arrow(batch) for batch in batches])

    def done(self,executor_id):
        return

"""
We are going to support four kinds of windows:
- hopping (defined by window length, hop size)
- sliding (defined by window length)
- session (defined by gap length)

We will expect the batches to come in sorted order.
"""

class HoppingWindowExecutor(Executor):
    def __init__(self, time_col, by_col, window,  trigger) -> None:
        self.time_col = time_col
        self.by_col = by_col
        self.state = None
        assert issubclass(type(window), HoppingWindow)
        assert issubclass(type(trigger), Trigger)
        self.window = window
        self.trigger = trigger

        # hopping window - event trigger is not supported. It is very complicated and probably not worth it.
        if type(trigger) == OnEventTrigger and type(window) == HoppingWindow:
            raise Exception("OnEventTrigger is not supported for hopping windows")

    def execute(self, batches, stream_id, executor_id):
        
        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        batch = polars.concat(batches)

        # current polars implementation cannot support floating point groupby dynamic and rolling operations.
        assert (batch[self.time_col].dtype in {polars.Int32, polars.Int64, polars.Datetime, polars.Date} )

        size = self.window.size_polars
        hop = self.window.hop_polars            
        result = None

        # for a hopping window, we want to make sure that we delegate all the rows in uncompleted windows to the next execute call.
        # therefore we need to compute the end time of the last completed window. 
        timestamp_of_last_row = batch[self.time_col][-1]
        if type(timestamp_of_last_row) == datetime.datetime:
            last_start = (timestamp_of_last_row - self.window.size).timestamp() // self.window.hop.total_seconds() * self.window.hop.total_seconds()
            last_end = last_start + self.window.size.total_seconds()
            new_state = batch.filter(polars.col(self.time_col) > datetime.datetime.fromtimestamp(last_end))
            batch = batch.filter(polars.col(self.time_col) <= datetime.datetime.fromtimestamp(last_end))
            
        elif type(timestamp_of_last_row) == int:
            last_start = (timestamp_of_last_row - self.window.size) // self.window.hop * self.window.hop
            last_end = last_start + self.window.size
            new_state = batch.filter(polars.col(self.time_col) > last_end)
            batch = batch.filter(polars.col(self.time_col) <= last_end)
        else:
            raise NotImplementedError
        
        if self.state is not None:
            batch = polars.concat([self.state, batch])
        self.state = new_state

        if type(self.trigger) == OnCompletionTrigger:
            # we are going to use polars groupby dynamic
            result = batch.groupby_dynamic(self.time_col, every = hop, period= size, by = self.by_col).agg(self.window.polars_aggregations()).sort(self.time_col)

        elif type(self.trigger) == OnEventTrigger:
        
            # we will assign a window id to each row, then use DuckDB's SQL window functions.
            # this is not the most efficient way to do this, but it is the easiest.

            assert type(self.window) == TumblingWindow
            if timestamp_of_last_row == datetime.datetime:
                batch = batch.with_column((polars.col(self.time_col).cast(polars.Int64) // self.window.size.total_seconds()).alias("__window_id"))
            else:
                batch = batch.with_column((polars.col(self.time_col) // self.window.size).alias("__window_id"))

            batch_arrow = batch.to_arrow()

            aggregations = self.window.sql_aggregations()
            con = duckdb.connect().execute('PRAGMA threads=%d' % 8)

            result = con.execute("""
                SELECT 
                    BY_COL,
                    TIME_COL,
                    AGG_FUNCS
                FROM batch_arrow
                WINDOW win AS (
                    PARTITION BY BY_COL, __window_id
                    ORDER BY TIME_COL
                    RANGE unbounded preceding
                )
            """.replace("TIME_COL", self.time_col).replace("BY_COL", self.by_col).replace("AGG_FUNCS", aggregations)).arrow()

            result = polars.from_arrow(result)
    
        else:
            raise NotImplementedError("unrecognized trigger type")
        
        return result

    def done(self, executor_id):

        if type(self.trigger) == OnCompletionTrigger:
            size = self.window.size_polars
            hop = self.window.hop_polars
            if self.state is not None and len(self.state) > 0:
                result = self.state.groupby_dynamic(self.time_col, every = hop, period= size, by = self.by_col).agg(self.aggregations).sort(self.time_col)
            else:
                result = None
        elif type(self.trigger) == OnEventTrigger:
            assert type(self.window) == TumblingWindow
            if self.state is not None and len(self.state) > 0:
                batch = self.state
                timestamp_of_last_row = batch[self.time_col][-1]
                if timestamp_of_last_row == datetime.datetime:
                    batch = batch.with_column((polars.col(self.time_col).cast(polars.Int64) // self.window.size.total_seconds()).alias("__window_id"))
                else:
                    batch = batch.with_column((polars.col(self.time_col) // self.window.size).alias("__window_id"))

                batch_arrow = batch.to_arrow()

                aggregations = self.window.sql_aggregations()
                con = duckdb.connect().execute('PRAGMA threads=%d' % 8)

                result = con.execute("""
                    SELECT 
                        BY_COL,
                        TIME_COL,
                        AGG_FUNCS
                    FROM batch_arrow
                    WINDOW win AS (
                        PARTITION BY BY_COL, __window_id
                        ORDER BY TIME_COL
                        RANGE unbounded preceding
                    )
                """.replace("TIME_COL", self.time_col).replace("BY_COL", self.by_col).replace("AGG_FUNCS", aggregations)).arrow()

                result = polars.from_arrow(result)
            else:
                result = None

        else:
            raise NotImplementedError("unrecognized trigger type")
        
        self.state = None
        return result

class SlidingWindowExecutor(Executor):
    def __init__(self, time_col, by_col, window,  trigger) -> None:
        self.time_col = time_col
        self.by_col = by_col
        self.state = None
        assert issubclass(type(window), SlidingWindow)
        assert issubclass(type(trigger), Trigger)
        self.window = window
        self.trigger = trigger

        # hopping window - event trigger is not supported. It is very complicated and probably not worth it.
        if type(trigger) == OnCompletionTrigger:
            print("Trying to use completion trigger with sliding window. This will result in the same behavior as an OnEventTrigger.")
            print("The completion time of a sliding window is when the last event comes, so they are the same. Timeout for completion trigger is ignored currently.")
        

    def execute(self, batches, stream_id, executor_id):

        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        batch = polars.concat(batches)

        # current polars implementation cannot support floating point groupby dynamic and rolling operations.
        assert (batch[self.time_col].dtype in {polars.Int32, polars.Int64, polars.Datetime, polars.Date} )

        size = self.window.size_before_polars
        to_discard = None
        if self.state is not None:
            batch = polars.concat([self.state, batch], rechunk=True)
            to_discard = len(self.state)

        timestamp_of_last_row = batch[self.time_col][-1]
        # python dynamic typing -- this will work for both timedelta window size and int window size
        self.state = batch.filter(polars.col(self.time_col) > timestamp_of_last_row - self.window.size_before)
        # print(len(self.state))
        # partitions = batch.partition_by(self.by_col)
        # results = []
        # for partition in partitions:
        #     results.append(partition.groupby_rolling(self.time_col, period = size).agg(self.window.polars_aggregations()))
        # result = polars.concat(results)
        result = batch.groupby_rolling(self.time_col, period= size, by = self.by_col).agg(self.window.polars_aggregations())#.sort(self.time_col)
        if to_discard is not None:
            result = result[to_discard:]

        return result
    
    def done(self, executor_id):
        return None


class SessionWindowExecutor(Executor):
    def __init__(self, time_col, by_col, window,  trigger) -> None:
        self.time_col = time_col
        self.by_col = by_col
        self.state = None
        assert issubclass(type(window), SessionWindow)
        assert issubclass(type(trigger), Trigger)
        self.window = window
        self.trigger = trigger

        # hopping window - event trigger is not supported. It is very complicated and probably not worth it.
        if type(trigger) == OnCompletionTrigger:
            print("Trying to use completion trigger with sliding window. This will result in the same behavior as an OnEventTrigger.")
            print("The completion time of a sliding window is when the last event comes, so they are the same. Timeout for completion trigger is ignored currently.")
    
    def execute(self, batches, stream_id, executor_id):
        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        batch = polars.concat(batches)

        # current polars implementation cannot support floating point groupby dynamic and rolling operations.
        assert (batch[self.time_col].dtype in {polars.Int32, polars.Int64, polars.Datetime, polars.Date} )
        timeout = self.window.timeout

        if self.state is not None:
            batch = polars.concat([self.state, batch])

        lazy_batch = batch.lazy()
        windowed_batch = lazy_batch.select([self.time_col, self.by_col]).groupby(self.by_col).agg(
            [
                polars.col(self.time_col),
                (polars.col("ts") - polars.col("ts").shift(1) > timeout).cumsum().alias("__window_id"),
            ]
        ).explode([self.time_col, "__window_id"]).fill_null(0).join(lazy_batch, on = [self.by_col, self.time_col]).collect()

        # you will need to collect rows corresponding to the last window id for each of the elements in by_col

        last_window_id = windowed_batch.groupby(self.by_col).agg(polars.max("__window_id"))
        # now collect the rows in windowed batch with last_window_id
        self.state = windowed_batch.join(last_window_id, on = [self.by_col, "__window_id"]).drop("__window_id")
        windowed_batch = windowed_batch.join(last_window_id, on = [self.by_col, "__window_id"], how = "anti")        

        if type(self.trigger) == OnCompletionTrigger:
            result = windowed_batch.groupby([self.by_col, "__window_id"]).agg(self.window.polars_aggregations())
        elif type(self.trigger) == OnEventTrigger:
            batch_arrow = windowed_batch.to_arrow()

            aggregations = self.window.sql_aggregations()
            con = duckdb.connect().execute('PRAGMA threads=%d' % 8)

            result = con.execute("""
                SELECT 
                    BY_COL,
                    TIME_COL,
                    AGG_FUNCS
                FROM batch_arrow
                WINDOW win AS (
                    PARTITION BY BY_COL, __window_id
                    ORDER BY TIME_COL
                    RANGE unbounded preceding
                )
            """.replace("TIME_COL", self.time_col).replace("BY_COL", self.by_col).replace("AGG_FUNCS", aggregations)).arrow()

        return result

    def done(self, executor_id):
        
        if self.state is None or len(self.state) == 0:
            return 
        else:
            if type(self.trigger) == OnCompletionTrigger:
                result = self.state.with_column(polars.lit(1).alias("__window_id")).groupby("__window_id").agg(self.window.polars_aggregations())
            elif type(self.trigger) == OnEventTrigger:
                batch_arrow = self.state.to_arrow()

                aggregations = self.window.sql_aggregations()
                con = duckdb.connect().execute('PRAGMA threads=%d' % 8)

                result = con.execute("""
                    SELECT 
                        BY_COL,
                        TIME_COL,
                        AGG_FUNCS
                    FROM batch_arrow
                    WINDOW win AS (
                        PARTITION BY BY_COL, __window_id
                        ORDER BY TIME_COL
                        RANGE unbounded preceding
                    )
                """.replace("TIME_COL", self.time_col).replace("BY_COL", self.by_col).replace("AGG_FUNCS", aggregations)).arrow()
        
            return result
        
class OutputExecutor(Executor):
    def __init__(self, filepath, format, prefix = "part", mode = "local", row_group_size = 5500000) -> None:
        self.num = 0
        assert format == "csv" or format == "parquet"
        self.format = format
        self.filepath = filepath
        self.prefix = prefix
        self.row_group_size = row_group_size
        self.my_batches = []
        self.name = 0
        self.mode = mode
        self.executor = None

    def serialize(self):
        return {}, "all"
    
    def deserialize(self, s):
        pass

    def execute(self,batches,stream_id, executor_id):

        fs = LocalFileSystem() if self.mode == "local" else S3FileSystem(region='us-west-1')
        self.my_batches.extend([i for i in batches if i is not None])


        '''
        You want to use Payrrow's write table API to flush everything at once. to get the parallelism.
        Being able to write multiple row groups at once really speeds things up. It's okay if you are slow at first
        because you are uploading/writing things one at a time
        '''

        lengths = [len(batch) for batch in self.my_batches]
        total_len = np.sum(lengths)

        # print(time.time(),[len(batch) for batch in self.my_batches], total_len)

        if total_len <= self.row_group_size:
            return

        write_len = total_len // self.row_group_size * self.row_group_size
        full_batches_to_take = np.where(np.cumsum(lengths) >= write_len)[0][0]        

        write_batch = polars.concat(self.my_batches[:full_batches_to_take]) if full_batches_to_take > 0 else None
        rows_to_take = int(write_len - np.sum(lengths[:full_batches_to_take]))
        self.my_batches = self.my_batches[full_batches_to_take:]
        if rows_to_take > 0:
            if write_batch is not None:
                write_batch.vstack(self.my_batches[0][:rows_to_take], in_place=True)
            else:
                write_batch = self.my_batches[0][:rows_to_take]
            self.my_batches[0] = self.my_batches[0][rows_to_take:]

        write_batch = write_batch.to_arrow()
        if self.format == "csv":
            for i, (col_name, type_) in enumerate(zip(write_batch.schema.names, write_batch.schema.types)):
                if pa.types.is_decimal(type_):
                    write_batch = write_batch.set_column(i, col_name, compute.cast(write_batch.column(col_name), pa.float64()))


        assert len(write_batch) % self.row_group_size == 0
        # print("WRITING", self.filepath,self.mode )

        if self.executor is None:
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)

        def upload_parquet(table, where):
            pq.write_table(table, where, filesystem=fs)
            return True
        def upload_csv(table, where):
            f = fs.open_output_stream(where)
            csv.write_csv(table, f)
            f.close()
            return True

        futures = []

        for i in range(0, len(write_batch), self.row_group_size):
            current_batch = write_batch[i * self.row_group_size : (i+1) * self.row_group_size]
            basename_template = self.filepath + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.name)  + "." + self.format
            self.name += 1
            if self.format == "parquet":
                futures.append(self.executor.submit(upload_parquet, current_batch, basename_template))
            else:
                futures.append(self.executor.submit(upload_csv, current_batch, basename_template))
        
        assert all([fut.result() for fut in futures])

        # ds.write_dataset(write_batch,base_dir = self.filepath, 
        #     basename_template = self.prefix + "-" + str(executor_id) + "-" + str(self.name) + "-{i}." + self.format, format=self.format, filesystem = fs,
        #     existing_data_behavior='overwrite_or_ignore',
        #     max_rows_per_file=self.row_group_size,max_rows_per_group=self.row_group_size)
        # print("wrote the dataset")
        return_df = polars.from_dict({"filename":[(self.prefix + "-" + str(executor_id) + "-" + str(self.name) + "-" + str(i) + "." + self.format) for i in range(len(write_batch) // self.row_group_size) ]})
        return return_df.to_arrow()

    def done(self,executor_id):
        df = polars.concat(self.my_batches)
        #print(df)
        fs = LocalFileSystem() if self.mode == "local" else S3FileSystem(region='us-west-1')
        write_batch = df.to_arrow()
        if self.format == "csv":
            for i, (col_name, type_) in enumerate(zip(write_batch.schema.names, write_batch.schema.types)):
                if pa.types.is_decimal(type_):
                    write_batch = write_batch.set_column(i, col_name, compute.cast(write_batch.column(col_name), pa.float64()))

        ds.write_dataset(write_batch,base_dir = self.filepath, 
            basename_template = self.prefix + "-" + str(executor_id) + "-" + str(self.name) + "-{i}." + self.format, format=self.format, filesystem = fs,
            existing_data_behavior='overwrite_or_ignore',
            max_rows_per_file=self.row_group_size,max_rows_per_group=self.row_group_size)
        
        return_df = polars.from_dict({"filename":[(self.prefix + "-" + str(executor_id) + "-" + str(self.name) + "-" + str(i) + "." + self.format) for i in range((len(write_batch) -1) // self.row_group_size + 1) ]})
        return return_df.to_arrow()

class BroadcastJoinExecutor(Executor):
    # batch func here expects a list of dfs. This is a quark of the fact that join results could be a list of dfs.
    # batch func must return a list of dfs too
    def __init__(self, small_table, on = None, small_on = None, big_on = None, suffix = "_small", how = "inner"):

        self.suffix = suffix

        assert how in {"inner", "left", "semi"}
        self.how = how
        self.batch_how = how if how != "left" else "inner"

        if type(small_table) == pd.core.frame.DataFrame:
            self.state = polars.from_pandas(small_table)
        elif type(small_table) == polars.internals.DataFrame:
            self.state = small_table
        else:
            raise Exception("small table data type not accepted")
        
        if how == "left" or how == "anti":
            self.left_null = None
            self.first_row_right = small_table[0]

        if on is not None:
            assert small_on is None and big_on is None
            self.small_on = on
            self.big_on = on
        else:
            assert small_on is not None and big_on is not None
            self.small_on = small_on
            self.big_on = big_on
        
        assert self.small_on in self.state.columns
    
    def checkpoint(self, conn, actor_id, channel_id, seq):
        pass
    
    def restore(self, conn, actor_id, channel_id, seq):
        pass

    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):
        # state compaction
        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        if len(batches) == 0:
            return
        batch = polars.concat(batches)

        if self.how != "anti":
            try:
                result = batch.join(self.state, left_on = self.big_on, right_on = self.small_on, how = self.batch_how, suffix = self.suffix)
            except:
                print(batch, self.state)

        new_left_null = None
        if self.how == "left" or self.how == "anti":
            new_left_null = batch.join(self.state1, left_on = self.left_on, right_on= self.right_on, how = "anti", suffix = self.suffix)
        
        if (self.how == "left" or self.how == "anti") and new_left_null is not None and len(new_left_null) > 0:
            if self.left_null is None:
                self.left_null = new_left_null
            else:
                self.left_null.vstack(new_left_null, in_place= True)

        if self.how != "anti" and result is not None and len(result) > 0:
            return result
    
    def done(self,executor_id):
        #print(len(self.state0),len(self.state1))
        #print("done join ", executor_id)
        
        if (self.how == "left" or self.how == "anti") and self.left_null is not None and len(self.left_null) > 0:
            if self.how == "left":
                return self.left_null.join(self.first_row_right, left_on= self.left_on, right_on= self.right_on, how = "left", suffix = self.suffix)
            if self.how == "anti":
                return self.left_null

# this is an inner join executor that must return outputs in a sorted order based on sorted_col
# the operator will maintain the sortedness of the probe side
# 0/left is probe, 1/right is build.
class BuildProbeJoinExecutor(Executor):

    def __init__(self, on = None, left_on = None, right_on = None, how = "inner", key_to_keep = "left"):

        self.state = None

        if on is not None:
            assert left_on is None and right_on is None
            self.left_on = on
            self.right_on = on
        else:
            assert left_on is not None and right_on is not None
            self.left_on = left_on
            self.right_on = right_on
        
        self.phase = "build"
        assert how in {"inner", "left", "semi", "anti"}
        self.how = how
        self.key_to_keep = key_to_keep
        self.things_seen = []

    def execute(self,batches, stream_id, executor_id):
        # state compaction
        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        if len(batches) == 0:
            return
        batch = polars.concat(batches)
        self.things_seen.append((stream_id, len(batches)))

        # build
        if stream_id == 1:
            assert self.phase == "build", (self.left_on, self.right_on, self.things_seen)
            self.state = batch if self.state is None else self.state.vstack(batch, in_place = True)
               
        # probe
        elif stream_id == 0:
            self.phase = "probe"
            result = batch.join(self.state,left_on = self.left_on, right_on = self.right_on ,how= self.how)
            if self.key_to_keep == "right":
                result = result.rename({self.left_on: self.right_on})
            return result
    
    def done(self,executor_id):
        pass

class JoinExecutor(Executor):
    # batch func here expects a list of dfs. This is a quark of the fact that join results could be a list of dfs.
    # batch func must return a list of dfs too
    def __init__(self, on = None, left_on = None, right_on = None, how = "inner"):

        self.state0 = None
        self.state1 = None

        if on is not None:
            assert left_on is None and right_on is None
            self.left_on = on
            self.right_on = on
        else:
            assert left_on is not None and right_on is not None
            self.left_on = left_on
            self.right_on = right_on
        
        assert how in {"inner", "left",  "semi"}
        self.how = how
        if how == "inner":
            self.batch_how = "inner"
        elif how == "semi":
            self.batch_how = "semi"
        elif how == "left":
            self.batch_how = "inner"
        
        if how == "left" or how =="semi":
            self.left_null = None
            self.first_row_right = None # this is a hack to produce the left join NULLs at the end.
            self.left_null_last_ckpt = 0

        # keys that will never be seen again, safe to delete from the state on the other side

        self.state0_last_ckpt = 0
        self.state1_last_ckpt = 0
        self.s3fs = None
    
    def checkpoint(self, bucket, actor_id, channel_id, seq):
        # redis.Redis('localhost',port=6800).set(pickle.dumps(("ckpt", actor_id, channel_id, seq)), pickle.dumps((self.state0, self.state1)))
        
        if self.s3fs is None:
            self.s3fs = S3FileSystem()

        if self.state0 is not None:
            state0_to_ckpt = self.state0[self.state0_last_ckpt : ]
            self.state0_last_ckpt += len(state0_to_ckpt)
            pq.write_table(self.state0.to_arrow(), bucket + "/" + str(actor_id) + "-" + str(channel_id) + "-" + str(seq) + "-0.parquet", filesystem=self.s3fs)

        if self.state1 is not None:
            state1_to_ckpt = self.state1[self.state1_last_ckpt : ]
            self.state1_last_ckpt += len(state1_to_ckpt)
            pq.write_table(self.state1.to_arrow(), bucket + "/" + str(actor_id) + "-" + str(channel_id) + "-" + str(seq) + "-1.parquet", filesystem=self.s3fs)
        
    
    def restore(self, bucket, actor_id, channel_id, seq):
        # self.state0, self.state1 = pickle.loads(redis.Redis('localhost',port=6800).get(pickle.dumps(("ckpt", actor_id, channel_id, seq))))
        
        if self.s3fs is None:
            self.s3fs = S3FileSystem()
        try:
            print(bucket + "/" + str(actor_id) + "-" + str(channel_id) + "-" + str(seq) + "-0.parquet")
            self.state0 = polars.from_arrow(pq.read_table(bucket + "/" + str(actor_id) + "-" + str(channel_id) + "-" + str(seq) + "-0.parquet", filesystem=self.s3fs))
            print(self.state0)
        except:
            self.state0 = None
        try:
            print(bucket + "/" + str(actor_id) + "-" + str(channel_id) + "-" + str(seq) + "-1.parquet")
            self.state1 = polars.from_arrow(pq.read_table(bucket + "/" + str(actor_id) + "-" + str(channel_id) + "-" + str(seq) + "-1.parquet", filesystem=self.s3fs))
            print(self.state1)
        except:
            self.state1 = None

    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):
        # state compaction
        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        if len(batches) == 0:
            return
        batch = polars.concat(batches)

        result = None
        new_left_null = None

        # if random.random() > 0.9 and redis.Redis('172.31.54.141',port=6800).get("input_already_failed") is None:
        #     redis.Redis('172.31.54.141',port=6800).set("input_already_failed", 1)
        #     ray.actor.exit_actor()
        # if random.random() > 0.9 and redis.Redis('localhost',port=6800).get("input_already_failed") is None:
        #     redis.Redis('localhost',port=6800).set("input_already_failed", 1)
        #     ray.actor.exit_actor()

        if stream_id == 0:
            if self.state1 is not None:
                result = batch.join(self.state1,left_on = self.left_on, right_on = self.right_on ,how=self.batch_how)
                if self.how == "left" or self.how == "semi":
                    new_left_null = batch.join(self.state1, left_on = self.left_on, right_on= self.right_on, how = "anti")
            else:
                if self.how == "left" or self.how == "semi":
                    new_left_null = batch

            if self.how != "semi":
                if self.state0 is None:
                    self.state0 = batch
                else:
                    self.state0.vstack(batch, in_place = True)

            if (self.how == "left" or self.how == "semi") and new_left_null is not None and len(new_left_null) > 0:
                if self.left_null is None:
                    self.left_null = new_left_null
                else:
                    self.left_null.vstack(new_left_null, in_place= True)
             
        elif stream_id == 1:

            if self.state0 is not None and self.how != "semi":
                result = self.state0.join(batch,left_on = self.left_on, right_on = self.right_on ,how=self.batch_how)
                
            if self.how == "semi" and self.left_null is not None:
                result = self.left_null.join(batch, left_on = self.left_on, right_on = self.right_on, how = "semi")
            
            if (self.how == "left" or self.how == "semi") and self.left_null is not None:
                self.left_null = self.left_null.join(batch, left_on = self.left_on, right_on = self.right_on, how = "anti")

            if self.state1 is None:
                if self.how == "left":
                    self.first_row_right = batch[0]
                self.state1 = batch
            else:
                self.state1.vstack(batch, in_place = True)
        
        if result is not None and len(result) > 0:
            return result
    
    def update_sources(self, remaining_sources):
        #print(remaining_sources)
        if self.how == "inner":
            if 0 not in remaining_sources:
                #print("DROPPING STATE!")
                self.state1 = None
            if 1 not in remaining_sources:
                #print("DROPPING STATE!")
                self.state0 = None
    
    def done(self,executor_id):
        self.update_sources({})
        #print(len(self.state0),len(self.state1))
        #print("done join ", executor_id)
        if self.how == "left" and self.left_null is not None and len(self.left_null) > 0:
            assert self.first_row_right is not None, "empty RHS"
            return self.left_null.join(self.first_row_right, left_on= self.left_on, right_on= self.right_on, how = "left")

        # print("DONE", executor_id)

class DuckJoinExecutor(Executor):
    # batch func here expects a list of dfs. This is a quark of the fact that join results could be a list of dfs.
    # batch func must return a list of dfs too
    # no need for suffix because we will already insert a rename node before the join node
    def __init__(self, on = None, left_on = None, right_on = None, how = "inner"):

        self.state0 = None
        self.state1 = None

        if on is not None:
            assert left_on is None and right_on is None
            self.left_on = on
            self.right_on = on
        else:
            assert left_on is not None and right_on is not None
            self.left_on = left_on
            self.right_on = right_on
        
        assert how in {"inner"}
        self.how = how
        self.batch_how = how

        # keys that will never be seen again, safe to delete from the state on the other side

        self.state0_last_ckpt = 0
        self.state1_last_ckpt = 0
        self.s3fs = None
        self.con = None
    
    def checkpoint(self, bucket, actor_id, channel_id, seq):
        # redis.Redis('localhost',port=6800).set(pickle.dumps(("ckpt", actor_id, channel_id, seq)), pickle.dumps((self.state0, self.state1)))
        
        if self.s3fs is None:
            self.s3fs = S3FileSystem()

        if self.state0 is not None:
            state0_to_ckpt = self.state0[self.state0_last_ckpt : ]
            self.state0_last_ckpt += len(state0_to_ckpt)
            pq.write_table(self.state0.to_arrow(), bucket + "/" + str(actor_id) + "-" + str(channel_id) + "-" + str(seq) + "-0.parquet", filesystem=self.s3fs)

        if self.state1 is not None:
            state1_to_ckpt = self.state1[self.state1_last_ckpt : ]
            self.state1_last_ckpt += len(state1_to_ckpt)
            pq.write_table(self.state1.to_arrow(), bucket + "/" + str(actor_id) + "-" + str(channel_id) + "-" + str(seq) + "-1.parquet", filesystem=self.s3fs)
        
    
    def restore(self, bucket, actor_id, channel_id, seq):
        # self.state0, self.state1 = pickle.loads(redis.Redis('localhost',port=6800).get(pickle.dumps(("ckpt", actor_id, channel_id, seq))))
        
        if self.s3fs is None:
            self.s3fs = S3FileSystem()
        try:
            print(bucket + "/" + str(actor_id) + "-" + str(channel_id) + "-" + str(seq) + "-0.parquet")
            self.state0 = polars.from_arrow(pq.read_table(bucket + "/" + str(actor_id) + "-" + str(channel_id) + "-" + str(seq) + "-0.parquet", filesystem=self.s3fs))
            print(self.state0)
        except:
            self.state0 = None
        try:
            print(bucket + "/" + str(actor_id) + "-" + str(channel_id) + "-" + str(seq) + "-1.parquet")
            self.state1 = polars.from_arrow(pq.read_table(bucket + "/" + str(actor_id) + "-" + str(channel_id) + "-" + str(seq) + "-1.parquet", filesystem=self.s3fs))
            print(self.state1)
        except:
            self.state1 = None

    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):
        # state compaction
        batches = [i for i in batches if i is not None and len(i) > 0]
        if len(batches) == 0:
            return

        batch = pa.concat_tables(batches)

        if self.con is None:
            self.con = duckdb.connect().execute('PRAGMA threads=%d' % 8)

        result = None

        if stream_id == 0:
            query = "select * from batch inner join state_arrow on batch." + self.left_on + " = state_arrow." + self.right_on
            if self.state1 is not None:
                state_arrow = self.state1
                # polars conversion will automatically take care of the duplicate join column emitted. 
                start = time.time()
                result = self.con.execute(query).arrow()
                print("duckdb time", time.time() - start)
                result = polars.from_arrow(result)

            if self.state0 is None:
                self.state0 = batch
            else:
                self.state0 = pa.concat_tables([self.state0, batch])

        elif stream_id == 1:
            query = "select * from batch inner join state_arrow on batch." + self.right_on + " = state_arrow." + self.left_on
            if self.state0 is not None:
                
                state_arrow = self.state0
                
                start = time.time()
                result = self.con.execute(query).arrow()
                print("duckdb time", time.time() - start)
                result = polars.from_arrow(result)
            
            if self.state1 is None:
                self.state1 = batch
            else:
                self.state1 = pa.concat_tables([self.state1, batch])

        if result is not None and len(result) > 0:
            return result
    
    def update_sources(self, remaining_sources):
        #print(remaining_sources)
        if self.how == "inner":
            if 0 not in remaining_sources:
                #print("DROPPING STATE!")
                self.state1 = None
            if 1 not in remaining_sources:
                #print("DROPPING STATE!")
                self.state0 = None
    
    def done(self,executor_id):
        self.update_sources({})
        # print("DONE", executor_id)

class AntiJoinExecutor(Executor):
    # batch func here expects a list of dfs. This is a quark of the fact that join results could be a list of dfs.
    # batch func must return a list of dfs too
    def __init__(self, on = None, left_on = None, right_on = None, suffix="_right"):

        self.left_null = None
        self.state1 = None
        self.ckpt_start0 = 0
        self.ckpt_start1 = 0
        self.suffix = suffix

        if on is not None:
            assert left_on is None and right_on is None
            self.left_on = on
            self.right_on = on
        else:
            assert left_on is not None and right_on is not None
            self.left_on = left_on
            self.right_on = right_on
        
        self.batch_size = 1000000
        
        # keys that will never be seen again, safe to delete from the state on the other side
    
    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):
        # state compaction
        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        if len(batches) == 0:
            return
        batch = polars.concat(batches)

        new_left_null = None

        if stream_id == 0:
            if self.state1 is not None:
                new_left_null = batch.join(self.state1, left_on = self.left_on, right_on= self.right_on, how = "anti", suffix = self.suffix)
            else:
                new_left_null = batch

            if new_left_null is not None and len(new_left_null) > 0:
                if self.left_null is None:
                    self.left_null = new_left_null
                else:
                    self.left_null.vstack(new_left_null, in_place= True)
             
        elif stream_id == 1:
            if self.left_null is not None:
                self.left_null = self.left_null.join(batch, left_on = self.left_on, right_on = self.right_on, how = "anti", suffix = self.suffix)
            
            if self.state1 is None:
                self.state1 = batch
            else:
                self.state1.vstack(batch, in_place = True)
    
    def done(self,executor_id):
        #print(len(self.state0),len(self.state1))
        #print("done join ", executor_id)
        if self.left_null is not None and len(self.left_null) > 0:
            for i in range(0, len(self.left_null), self.batch_size):
                yield self.left_null[i: i + self.batch_size]

class DistinctExecutor(Executor):
    def __init__(self, keys) -> None:

        self.keys = keys
        self.state = None
    
    def checkpoint(self, conn, actor_id, channel_id, seq):
        pass
    
    def restore(self, conn, actor_id, channel_id, seq):
        pass

    def execute(self, batches, stream_id, executor_id):
        
        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        if len(batches) == 0:
            return
        batch = polars.concat(batches)
        batch = batch.unique()

        if self.state is None:
            self.state = batch
            return batch
        else:
            contribution = batch.join(self.state, on = self.keys, how="anti")
            self.state.vstack(contribution, in_place = True)
            return contribution
    
    def serialize(self):
        return {0:self.seen}, "all"
    
    def deserialize(self, s):
        # the default is to get a list of things 
        assert type(s) == list and len(s) == 1
        self.seen = s[0][0]
    
    def done(self, executor_id):
        return


class DuckAggExecutor(Executor):

    def __init__(self, groupby_keys, orderby_keys, aggregation_dict, mean_cols, count):

        self.state = None
        self.emit_count = count
        self.do_count = count
        assert type(groupby_keys) == list
        self.groupby_keys = groupby_keys
        self.mean_cols = mean_cols
        self.length_limit = 1000000
        self.con = None        
        self.count_col = "__count_sum"
        
        # we could use SQLGlot here but that's overkill.
        self.agg_clause = "select"
        for key in groupby_keys:
            self.agg_clause += "\n\t" + key + ","
        
        for key in aggregation_dict:
            agg_type = aggregation_dict[key]
            assert agg_type in {
                    "max", "min", "mean", "sum"}, "only support max, min, mean and sum for now"
            if agg_type == "mean":
                self.do_count = True
                self.agg_clause += "\n\tsum(" + key + ") as " + key + "_sum,"
            else:
                self.agg_clause += "\n\t" + agg_type + "(" + key + ") as " + key + "_" + agg_type + ","
        
        if self.do_count:
            self.agg_clause += "\n\tsum(__count_sum) as __count_sum,"
        
        # remove trailing comma
        self.agg_clause = self.agg_clause[:-1]
        self.agg_clause += "\nfrom\n\tbatch_arrow\n"
        if len(groupby_keys) > 0:
            self.agg_clause += "group by "
            for key in groupby_keys:
                self.agg_clause += key + ","
            self.agg_clause = self.agg_clause[:-1]

        if orderby_keys is not None:
            self.agg_clause += "\norder by "
            for key, dir in orderby_keys:
                if dir == "desc":
                    self.agg_clause += key + " desc,"
                else:
                    self.agg_clause += key + ","
            self.agg_clause = self.agg_clause[:-1]
        # print(self.agg_clause)

    def checkpoint(self, conn, actor_id, channel_id, seq):
        pass
    
    def restore(self, conn, actor_id, channel_id, seq):
        pass
    
    def execute(self,batches, stream_id, executor_id):

        if self.con is None:
            self.con = duckdb.connect().execute('PRAGMA threads=%d' % multiprocessing.cpu_count())

        batch = pa.concat_tables(batches)
        if self.state is None:
            self.state = batch
        else:
            self.state = pa.concat_tables([self.state, batch])
        if len(self.state) > self.length_limit:
            batch_arrow = self.state
            self.state = self.con.execute(self.agg_clause).arrow()
            del batch_arrow
    
    def done(self, executor_id):
        if self.state is None:
            return None
        batch_arrow = self.state
        self.state = polars.from_arrow(self.con.execute(self.agg_clause).arrow())
        del batch_arrow

        for key in self.mean_cols:
            keep_sum = self.mean_cols[key]
            self.state = self.state.with_column(polars.Series(key + "_mean", self.state[key + "_sum"]/ self.state[self.count_col]))
            if not keep_sum:
                self.state = self.state.drop(key + "_sum")
        
        if self.do_count and not self.emit_count:
            self.state = self.state.drop(self.count_col)
        
        return self.state

class CountExecutor(Executor):
    def __init__(self) -> None:

        self.state = 0

    def checkpoint(self, conn, actor_id, channel_id, seq):
        pass
    
    def restore(self, conn, actor_id, channel_id, seq):
        pass

    def execute(self, batches, stream_id, executor_id):
        
        self.state += sum(len(batch) for batch in batches)
    
    def serialize(self):
        return {0:self.state}, "all"
    
    def deserialize(self, s):
        # the default is to get a list of things 
        assert type(s) == list and len(s) == 1
        self.state = s[0][0]
    
    def done(self, executor_id):
        #print("COUNT:", self.state)
        return polars.DataFrame([self.state])


class SuperFastSortExecutor(Executor):
    def __init__(self, key, record_batch_rows = 100000, output_batch_rows = 1000000, file_prefix = "mergesort") -> None:
        self.key = key
        self.record_batch_rows = record_batch_rows
        self.output_batch_rows = output_batch_rows
        self.fileno = 0
        self.prefix = file_prefix # make sure this is different for different executors
        self.data_dir = "/data/"
        self.in_mem_state = None
        self.executor = None

    def write_out_df_to_disk(self, target_filepath, input_mem_table):
        arrow_table = input_mem_table.to_arrow()
        batches = arrow_table.to_batches(1000000)
        writer =  pa.ipc.new_file(pa.OSFile(target_filepath, 'wb'), arrow_table.schema)
        for batch in batches:
            writer.write(batch)
        writer.close()
        # input_mem_table.write_parquet(target_filepath, row_group_size = self.record_batch_rows, use_pyarrow =True)

        return True

    def execute(self, batches, stream_id, executor_id):

        # if self.executor is None:
        #     self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

        # we are going to update the in memory index and flush out the sorted stuff
        
        flush_file_name = self.data_dir + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow"
        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        if len(batches) == 0:
            return None
        
        start = time.time()
        batch = polars.concat(batches)
        print("concat execute used", time.time() - start)

        start = time.time()
        sorted_batch = batch.sort(self.key)
        print("sort execute used", time.time() - start)
        
        start = time.time()
        self.write_out_df_to_disk(flush_file_name, sorted_batch)
        # future = self.executor.submit(self.write_out_df_to_disk, flush_file_name, sorted_batch)
        print("flush execute used", time.time() - start)

        start = time.time()
        new_in_mem_state = polars.from_dict({ "values": sorted_batch[self.key], "file_no": np.ones(len(batch), dtype=np.int32) * self.fileno})
        if self.in_mem_state is None:
            self.in_mem_state = new_in_mem_state
        else:
            self.in_mem_state.vstack(new_in_mem_state, in_place=True)
        
        print("update execute state used", time.time() - start)
        
        # assert future.result()
        self.fileno += 1
        
    
    def done(self, executor_id):

        # first sort the in memory state
        print("STARTING DONE", time.time())
        self.in_mem_state = self.in_mem_state.sort("values")
        
        # load the cache
        num_sources = self.fileno 
        sources =  {i : pa.ipc.open_file(pa.memory_map( self.data_dir + self.prefix + "-" + str(executor_id) + "-" + str(i) + ".arrow"  , 'rb')) for i in range(num_sources)}
        number_of_batches_in_source = { source: sources[source].num_record_batches for source in sources}
        cached_batches = {i : polars.from_arrow( pa.Table.from_batches([sources[i].get_batch(0)]) ) for i in sources}
        current_number_for_source = {i: 1 for i in sources}

        print("END DONE SETUP", time.time())

        # now start assembling batches of the output
        for k in range(0, len(self.in_mem_state), self.output_batch_rows):

            start = time.time()

            things_to_get = self.in_mem_state[k : k + self.output_batch_rows]
            file_requirements = things_to_get.groupby("file_no").count()
            desired_batches = []
            for i in range(len(file_requirements)):
                desired_length = file_requirements["count"][i]
                source = file_requirements["file_no"][i]
                while desired_length > len(cached_batches[source]):
                    if current_number_for_source[source] == number_of_batches_in_source[source]:
                        raise Exception
                    else:
                        cached_batches[source].vstack(polars.from_arrow( pa.Table.from_batches( [sources[source].get_batch(current_number_for_source[source])])), in_place=True)
                        current_number_for_source[source] += 1
                else:
                    desired_batches.append(cached_batches[source][:desired_length])
                    cached_batches[source] = cached_batches[source][desired_length:]
            
            result = polars.concat(desired_batches).sort(self.key)
            print("yield one took", time.time() - start)
            yield result
            

#table = polars.read_parquet("/home/ziheng/tpc-h/lineitem.parquet")
#exe = SuperFastSortExecutor("l_partkey", record_batch_rows = 10000, output_batch_rows = 1000000, file_prefix = "mergesort")
#for i in range(0, len(table), 1000000):
#    exe.execute([table[i:i+1000000]],0,0)
#for k in exe.done(0):
#    print(k["l_partkey"])

#executor = MergeSortedExecutor("l_partkey", record_batch_rows = 250000, length_limit = 500000)
#executor.filename_to_size = {i: 0 for i in range(95, 127, 2)}
#executor.filename_to_size[126] = 0
#da = executor.done(7)
#start = time.time()
#for bump in da:
#    pass
#print(time.time() - start)
#stuff = []
#exe = MergeSortedExecutor('0', length_limit=1000)
#for k in range(100):
#   item = polars.from_pandas(pd.DataFrame(np.random.normal(size=(random.randint(1, 2000),1000))))
#   exe.execute([item], 0, 0)
#da = exe.done(0)
#for bump in da:
#    pass

# exe = MergeSortedExecutor('0', 3000)
# a = polars.from_pandas(pd.DataFrame(np.random.normal(size=(10000,1000)))).sort('0')
# b = polars.from_pandas(pd.DataFrame(np.random.normal(size=(10000,1000)))).sort('0')

# exe.write_out_df_to_disk("file.arrow", a)
#exe = MergeSortedExecutor( "l_partkey", record_batch_rows = 1000000, length_limit = 1000000, file_prefix = "mergesort", output_line_limit = 1000000)
#exe.produce_sorted_file_from_two_sorted_files("/data/test.arrow","/data/mergesort-0-29.arrow","/data/mergesort-1-31.arrow")

# del a
# process = psutil.Process(os.getpid())
# print(process.memory_info().rss)
# exe.produce_sorted_file_from_sorted_file_and_in_memory("file2.arrow","file.arrow",b)
# exe.produce_sorted_file_from_two_sorted_files("file3.arrow","file2.arrow","file.arrow")


# exe = OutputCSVExecutor( "quokka-examples", "trash", output_line_limit = 1000)
# for k in range(100):
#    item = [polars.from_pandas(pd.DataFrame(np.random.normal(size=(200,100)))) for i in range(np.random.randint(0,10))]
#    exe.execute(item, 0,0)
    
