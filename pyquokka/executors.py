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
import ray
import pickle
import concurrent.futures

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
        batches = [batch for batch in batches if batch is not None and len(batch) > 0]
        #print(batches)
        if len(batches) > 0:
            if type(batches[0]) == polars.internals.DataFrame:
                return polars.concat(batches)
            else:
                return pd.vstack(batches)

    def done(self,executor_id):
        return

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

        print(time.time(),[len(batch) for batch in self.my_batches], total_len)

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
        print("WRITING", self.filepath,self.mode )

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
        print("wrote the dataset")
        return_df = polars.from_dict({"filename":[(self.prefix + "-" + str(executor_id) + "-" + str(self.name) + "-" + str(i) + "." + self.format) for i in range(len(write_batch) // self.row_group_size) ]})
        return return_df

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
        return return_df

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
        batches = [i for i in batches if i is not None and len(i) > 0]
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


class JoinExecutor(Executor):
    # batch func here expects a list of dfs. This is a quark of the fact that join results could be a list of dfs.
    # batch func must return a list of dfs too
    def __init__(self, on = None, left_on = None, right_on = None, suffix="_right", how = "inner"):

        self.state0 = None
        self.state1 = None
        self.suffix = suffix

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
        batches = [i for i in batches if i is not None and len(i) > 0]
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
                result = batch.join(self.state1,left_on = self.left_on, right_on = self.right_on ,how=self.batch_how, suffix=self.suffix)
                if self.how == "left" or self.how == "semi":
                    new_left_null = batch.join(self.state1, left_on = self.left_on, right_on= self.right_on, how = "anti", suffix = self.suffix)
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
                result = self.state0.join(batch,left_on = self.left_on, right_on = self.right_on ,how=self.batch_how, suffix=self.suffix)
            
            if self.how == "semi" and self.left_null is not None:
                result = self.left_null.join(batch, left_on = self.left_on, right_on = self.right_on, how = "semi", suffix = self.suffix)
            
            if (self.how == "left" or self.how == "semi") and self.left_null is not None:
                self.left_null = self.left_null.join(batch, left_on = self.left_on, right_on = self.right_on, how = "anti", suffix = self.suffix)

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
        #print(len(self.state0),len(self.state1))
        #print("done join ", executor_id)
        if self.how == "left" and self.left_null is not None and len(self.left_null) > 0:
            assert self.first_row_right is not None, "empty RHS"
            return self.left_null.join(self.first_row_right, left_on= self.left_on, right_on= self.right_on, how = "left", suffix = self.suffix)

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
        batches = [i for i in batches if i is not None and len(i) > 0]
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
        
        batches = [i for i in batches if i is not None and len(i) > 0]
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

class AggExecutor(Executor):
    '''
    aggregation_dict will define what you are going to do for
    '''
    def __init__(self, groupby_keys, orderby_keys, aggregation_dict, mean_cols, count):


        self.state = None
        self.emit_count = count
        assert type(groupby_keys) == list and len(groupby_keys) > 0
        self.groupby_keys = groupby_keys
        self.aggregation_dict = aggregation_dict
        self.mean_cols = mean_cols
        self.length_limit = 1000000
        # hope and pray there is no column called __&&count__
        self.pyarrow_agg_list = [("__count_sum", "sum")]
        self.count_col = "__count_sum"
        self.rename_dict = {"__count_sum_sum": self.count_col}
        for key in aggregation_dict:
            assert aggregation_dict[key] in {
                    "max", "min", "mean", "sum"}, "only support max, min, mean and sum for now"
            if aggregation_dict[key] == "mean":
                self.pyarrow_agg_list.append((key, "sum"))
                self.rename_dict[key + "_sum"] = key
            else:
                self.pyarrow_agg_list.append((key, aggregation_dict[key]))
                self.rename_dict[key + "_" + aggregation_dict[key]] = key
        
        self.order_list = []
        self.reverse_list = []
        if orderby_keys is not None:
            for key, dir in orderby_keys:
                self.order_list.append(key)
                self.reverse_list.append(True if dir == "desc" else False)

    def checkpoint(self, conn, actor_id, channel_id, seq):
        pass
    
    def restore(self, conn, actor_id, channel_id, seq):
        pass

    def serialize(self):
        return {0:self.state}, "all"
    
    def deserialize(self, s):
        # the default is to get a list of dictionaries.
        assert type(s) == list and len(s) == 1
        self.state = s[0][0]
    
    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):

        batches = [i for i in batches if i is not None]
        batch = polars.concat(batches)
        assert type(batch) == polars.internals.DataFrame, batch # polars add has no index, will have wierd behavior
        if self.state is None:
            self.state = batch
        else:
            self.state = self.state.vstack(batch)
        if len(self.state) > self.length_limit:
            arrow_state = self.state.to_arrow()
            arrow_state = arrow_state.group_by(self.groupby_keys).aggregate(self.pyarrow_agg_list)
            self.state = polars.from_arrow(arrow_state).rename(self.rename_dict)
            self.state = self.state.select(sorted(self.state.columns))


    def done(self,executor_id):

        # print("done", time.time())

        if self.state is None:
            return None
        
        arrow_state = self.state.to_arrow()
        arrow_state = arrow_state.group_by(self.groupby_keys).aggregate(self.pyarrow_agg_list)
        self.state = polars.from_arrow(arrow_state).rename(self.rename_dict)

        for key in self.aggregation_dict:
            if self.aggregation_dict[key] == "mean":
                self.state = self.state.with_column(polars.Series(key, self.state[key]/ self.state[self.count_col]))
        
        for key in self.mean_cols:
            keep_sum = self.mean_cols[key]
            self.state = self.state.with_column(polars.Series(key + "_mean", self.state[key + "_sum"]/ self.state[self.count_col]))
            if not keep_sum:
                self.state = self.state.drop(key + "_sum")
        
        if not self.emit_count:
            self.state = self.state.drop(self.count_col)
        
        if len(self.order_list) > 0:
            return self.state.sort(self.order_list, self.reverse_list)
        else:
            return self.state


class LimitExecutor(Executor):
    def __init__(self, limit) -> None:
        self.limit = limit
        self.state = []

    def execute(self, batches, stream_id, executor_id):

        batch = pd.concat(batches)
        self.state.append(batch)
        length = sum([len(i) for i in self.state])
        if length > self.limit:
            self.set_early_termination()
    
    def done(self):
        return pd.concat(self.state)[:self.limit]

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
        # arrow_table = input_mem_table.to_arrow()
        # batches = arrow_table.to_batches(1000000)
        # writer =  pa.ipc.new_file(pa.OSFile(target_filepath, 'wb'), arrow_table.schema)
        # for batch in batches:
        #     writer.write(batch)
        # writer.close()
        input_mem_table.write_parquet(target_filepath, row_group_size = self.record_batch_rows, use_pyarrow =True)

        return True

    def execute(self, batches, stream_id, executor_id):

        # if self.executor is None:
        #     self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

        # we are going to update the in memory index and flush out the sorted stuff
        
        flush_file_name = self.data_dir + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow"
        batches = [i for i in batches if i is not None and len(i) > 0]
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
    
