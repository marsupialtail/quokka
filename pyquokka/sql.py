import pickle
import os
os.environ["POLAR_MAX_THREADS"] = "1"
import polars
import pandas as pd
os.environ['ARROW_DEFAULT_MEMORY_POOL'] = 'system'

import pyarrow as pa
import time
import numpy as np
import os, psutil
import boto3
import gc
import pyarrow.csv as csv
from io import StringIO, BytesIO
from pyquokka.state import PersistentStateVariable
from collections import deque
import pyarrow.compute as compute
import random
import asyncio
import concurrent.futures
import sys
from aiobotocore.session import get_session


class Executor:
    def __init__(self) -> None:
        raise NotImplementedError
    def initialize(datasets):
        pass
    def serialize(self):
        pass
    def deserialize(self, s):
        pass
    def set_early_termination(self):
        self.early_termination = True
    def execute(self,batches,stream_id, executor_id):
        raise NotImplementedError
    def done(self,executor_id):
        raise NotImplementedError    

class UDFExecutor:
    def __init__(self, udf) -> None:
        self.udf = udf
        self.num_states = 0

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
        self.num_states = 0
    def serialize(self):
        return {}, "all"
    def deserialize(self, s):
        pass
    
    def execute(self,batches,stream_id, executor_id):
        batches = [batch for batch in batches if batch is not None and len(batch) > 0]
        if len(batches) > 0:
            if type(batches[0]) == polars.internals.frame.DataFrame:
                return polars.concat(batches)
            else:
                return pd.vstack(batches)

    def done(self,executor_id):
        return

class OutputCSVExecutor(Executor):
    def __init__(self, bucket, prefix, output_line_limit = 1000000) -> None:
        self.num = 0
        self.num_states = 0

        self.bucket = bucket
        self.prefix = prefix
        self.output_line_limit = output_line_limit
        self.name = 0
        self.my_batches = deque()
        self.exe = None
        self.executor_id = None
        self.session = get_session()

    def serialize(self):
        return {}, "all"
    
    def deserialize(self, s):
        pass

    def create_csv_file(self, data):
        da = BytesIO()
        csv.write_csv(data.to_arrow(), da,  write_options = csv.WriteOptions(include_header=False))
        return da

    async def do(self, client, data, name):
        da = await asyncio.get_running_loop().run_in_executor(self.exe, self.create_csv_file, data)
        resp = await client.put_object(Bucket=self.bucket,Key=self.prefix + "-" + str(self.executor_id) + "-" + str(name) + ".csv",Body=da.getvalue())
        #print(resp)

    async def go(self, datas):
        # this is expansion of async with, so you don't have to remake the client
        
        async with self.session.create_client("s3", region_name="us-west-2") as client:
            todos = []
            for i in range(len(datas)):
                todos.append(self.do(client,datas[i], self.name))
                self.name += 1
        
            await asyncio.gather(*todos)
        
    def execute(self,batches,stream_id, executor_id):

        if self.exe is None:
            self.exe = concurrent.futures.ThreadPoolExecutor(max_workers = 2)
        if self.executor_id is None:
            self.executor_id = executor_id
        else:
            assert self.executor_id == executor_id

        self.my_batches.extend([i for i in batches if i is not None])
        #print("MY OUTPUT CSV STATE", [len(i) for i in self.my_batches] )

        curr_len = 0
        i = 0
        datas = []
        process = psutil.Process(os.getpid())
        print("mem usage output", process.memory_info().rss, pa.total_allocated_bytes())
        while i < len(self.my_batches):
            curr_len += len(self.my_batches[i])
            #print(curr_len)
            i += 1
            if curr_len > self.output_line_limit:
                #print("writing")
                datas.append(polars.concat([self.my_batches.popleft() for k in range(i)], rechunk = True))
                i = 0
                curr_len = 0
        #print("writing ", len(datas), " files")
        asyncio.run(self.go(datas))
        #print("mem usage after", process.memory_info().rss, pa.total_allocated_bytes())

    def done(self,executor_id):
        if len(self.my_batches) > 0:
            datas = [polars.concat(list(self.my_batches), rechunk=True)]
            asyncio.run(self.go(datas))
        print("done")

class BroadcastJoinExecutor(Executor):
    # batch func here expects a list of dfs. This is a quark of the fact that join results could be a list of dfs.
    # batch func must return a list of dfs too
    def __init__(self, small_table, on = None, small_on = None, big_on = None, batch_func = None, columns = None):

        # how many things you might checkpoint, the number of keys in the dict
        self.num_states = 1
        self.state = None

        if type(small_table) == pd.core.frame.DataFrame:
            self.state = polars.from_pandas(small_table)
        elif type(small_table) == polars.internals.frame.DataFrame:
            self.state = small_table
        else:
            raise Exception("small table data type not accepted")

        self.columns = columns
        self.checkpointed = False

        if on is not None:
            assert small_on is None and big_on is None
            self.small_on = on
            self.big_on = on
        else:
            assert small_on is not None and big_on is not None
            self.small_on = small_on
            self.big_on = big_on
        
        assert self.small_on in self.state.columns

        self.batch_func = batch_func
        # keys that will never be seen again, safe to delete from the state on the other side

    def serialize(self):
        # if you have already checkpointed the small table, don't checkpoint anything

        # otherwise checkpoint the small table
        if not self.checkpointed:
            assert self.state is not None
            self.checkpointed = True
            return {0:self.state}, "all"
        else:
            return None, "inc" # second argument here doesn't really matter
    
    def deserialize(self, s):
        assert type(s) == list
        self.state = s[0][0]
    
    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):
        # state compaction
        batches = [i for i in batches if i is not None and len(i) > 0]
        if len(batches) == 0:
            return
        batch = polars.concat(batches)
        result = batch.join(self.state, left_on = self.big_on, right_on = self.small_on, how = "inner")
        
        if self.columns is not None and result is not None and len(result) > 0:
            result = result[self.columns]

        if result is not None and len(result) > 0:
            if self.batch_func is not None:
                da =  self.batch_func(result.to_pandas())
                return da
            else:
                return result
    
    def done(self,executor_id):
        #print(len(self.state0),len(self.state1))
        print("done join ", executor_id)


# the inputs must be sorted on the asof join column
class GroupAsOfJoinExecutor(Executor):
    # batch func here expects a list of dfs. This is a quark of the fact that join results could be a list of dfs.
    # batch func must return a list of dfs too
    def __init__(self, group_on= None, group_left_on = None, group_right_on = None, on = None, left_on = None, right_on = None, batch_func = None, columns = None):

        # how many things you might checkpoint, the number of keys in the dict
        self.num_states = 2

        self.trade = {}
        self.quote = {}
        self.ckpt_start0 = 0
        self.ckpt_start1 = 0
        self.columns = columns

        if on is not None:
            assert left_on is None and right_on is None
            self.left_on = on
            self.right_on = on
        else:
            assert left_on is not None and right_on is not None
            self.left_on = left_on
            self.right_on = right_on
        
        if group_on is not None:
            assert group_left_on is None and group_right_on is None
            self.group_left_on = group_on
            self.group_right_on = group_on
        else:
            assert group_left_on is not None and group_right_on is not None
            self.group_left_on = group_left_on
            self.group_right_on = group_right_on

        self.batch_func = batch_func
        # keys that will never be seen again, safe to delete from the state on the other side

    def serialize(self):
        result = {0:self.trade, 1:self.quote}        
        return result, "all"
    
    def deserialize(self, s):
        assert type(s) == list
        self.trade = s[0][0]
        self.quote = s[0][1]
    
    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):
        # state compaction
        batches = [i for i in batches if i is not None and len(i) > 0]
        if len(batches) == 0:
            return
        batch = polars.concat(batches)
        ret_vals = []
        # trade
        if stream_id == 0:
            frames = batch.partition_by(self.group_left_on)
            for frame in frames:
                symbol = frame["symbol"][0]
                if symbol in self.trade:
                    self.trade[symbol].vstack(frame,in_place=True)
                else:
                    self.trade[symbol] = frame
                # now do the join
                if symbol not in self.quote:
                    continue
                else:
                    trades = self.trade[symbol]
                    quotes = self.quote[symbol]
                    joinable_trades = trades[trades[self.left_on] < quotes[-1][self.right_on]]
                    joinable_quotes = quotes[quotes[self.right_on] < joinable_trades[-1][self.left_on]]
                    self.trade[symbol] = trades[len(joinable_trades):]
                    self.quote[symbol] = quotes[len(joinable_quotes):]
                    ret_vals.append(joinable_trades.join_asof(joinable_quotes.drop(self.group_right_on), left_on = self.left_on, right_on = self.right_on))

        #quote
        elif stream_id == 1:
            frames = batch.partition_by(self.group_right_on)
            for frame in frames:
                symbol = frame["symbol"][0]
                if symbol in self.quote:
                    self.quote[symbol].vstack(frame,in_place=True)
                else:
                    self.quote[symbol] = frame
                if symbol not in self.trade:
                    continue
                else:
                    trades = self.trade[symbol]
                    quotes = self.quote[symbol]
                    joinable_trades = trades[trades[self.left_on] < quotes[-1][self.right_on]]
                    joinable_quotes = quotes[quotes[self.right_on] < joinable_trades[-1][self.left_on]]
                    self.trade[symbol] = trades[len(joinable_trades):]
                    self.quote[symbol] = quotes[len(joinable_quotes):]
                    ret_vals.append(joinable_trades.join_asof(joinable_quotes.drop(self.group_right_on), left_on = self.left_on, right_on = self.right_on))


        result = polars.concat(ret_vals).drop_nulls()
        
        if self.columns is not None and result is not None and len(result) > 0:
            result = result[self.columns]

        if result is not None and len(result) > 0:
            if self.batch_func is not None:
                da =  self.batch_func(result.to_pandas())
                return da
            else:
                print("RESULT LENGTH",len(result))
                return result
    
    def done(self,executor_id):
        #print(len(self.state0),len(self.state1))
        ret_vals = []
        for symbol in self.trade:
            if symbol not in self.quote:
                continue
            else:
                trades = self.trade[symbol]
                quotes = self.quote[symbol]
                ret_vals.append(trades.join_asof(quotes.drop(self.group_right_on), left_on = self.left_on, right_on = self.right_on))
        
        print("done asof join ", executor_id)
        return polars.concat(ret_vals).drop_nulls()

class PolarJoinExecutor(Executor):
    # batch func here expects a list of dfs. This is a quark of the fact that join results could be a list of dfs.
    # batch func must return a list of dfs too
    def __init__(self, on = None, left_on = None, right_on = None, batch_func = None, columns = None):

        # how many things you might checkpoint, the number of keys in the dict
        self.num_states = 2

        self.state0 = None
        self.state1 = None
        self.ckpt_start0 = 0
        self.ckpt_start1 = 0
        self.columns = columns

        if on is not None:
            assert left_on is None and right_on is None
            self.left_on = on
            self.right_on = on
        else:
            assert left_on is not None and right_on is not None
            self.left_on = left_on
            self.right_on = right_on
        self.batch_func = batch_func
        # keys that will never be seen again, safe to delete from the state on the other side

    def serialize(self):
        result = {0:self.state0[self.ckpt_start0:] if (self.state0 is not None and len(self.state0[self.ckpt_start0:]) > 0) else None, 1:self.state1[self.ckpt_start1:] if (self.state1 is not None and len(self.state1[self.ckpt_start1:]) > 0) else None}
        if self.state0 is not None:
            self.ckpt_start0 = len(self.state0)
        if self.state1 is not None:
            self.ckpt_start1 = len(self.state1)
        return result, "inc"
    
    def deserialize(self, s):
        assert type(s) == list
        list0 = [i[0] for i in s if i[0] is not None]
        list1 = [i[1] for i in s if i[1] is not None]
        self.state0 = polars.concat(list0) if len(list0) > 0 else None
        self.state1 = polars.concat(list1) if len(list1) > 0 else None
        self.ckpt_start0 = len(self.state0) if self.state0 is not None else 0
        self.ckpt_start1 = len(self.state1) if self.state1 is not None else 0
    
    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):
        # state compaction
        batches = [i for i in batches if i is not None and len(i) > 0]
        if len(batches) == 0:
            return
        batch = polars.concat(batches)

        result = None
        if stream_id == 0:
            if self.state1 is not None:
                try:
                    result = batch.join(self.state1,left_on = self.left_on, right_on = self.right_on ,how='inner')
                except:
                    print(batch)
            if self.state0 is None:
                self.state0 = batch
            else:
                self.state0.vstack(batch, in_place = True)
             
        elif stream_id == 1:
            if self.state0 is not None:
                result = self.state0.join(batch,left_on = self.left_on, right_on = self.right_on ,how='inner')
            if self.state1 is None:
                self.state1 = batch
            else:
                self.state1.vstack(batch, in_place = True)
        
        if self.columns is not None and result is not None and len(result) > 0:
            result = result[self.columns]

        if result is not None and len(result) > 0:
            if self.batch_func is not None:
                da =  self.batch_func(result.to_pandas())
                return da
            else:
                print("RESULT LENGTH",len(result))
                return result
    
    def done(self,executor_id):
        #print(len(self.state0),len(self.state1))
        print("done join ", executor_id)


class OOCJoinExecutor(Executor):
    # batch func here expects a list of dfs. This is a quark of the fact that join results could be a list of dfs.
    # batch func must return a list of dfs too
    def __init__(self, on = None, left_on = None, right_on = None, left_primary = False, right_primary = False, batch_func = None):
        self.state0 = PersistentStateVariable()
        self.state1 = PersistentStateVariable()
        if on is not None:
            assert left_on is None and right_on is None
            self.left_on = on
            self.right_on = on
        else:
            assert left_on is not None and right_on is not None
            self.left_on = left_on
            self.right_on = right_on

        self.batch_func = batch_func

    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):

        batch = pd.concat(batches)
        results = []

        if stream_id == 0:
            if len(self.state1) > 0:
                results = [batch.merge(i,left_on = self.left_on, right_on = self.right_on ,how='inner',suffixes=('_a','_b')) for i in self.state1]
            self.state0.append(batch)
             
        elif stream_id == 1:
            if len(self.state0) > 0:
                results = [i.merge(batch,left_on = self.left_on, right_on = self.right_on ,how='inner',suffixes=('_a','_b')) for i in self.state0]
            self.state1.append(batch)
        
        if len(results) > 0:
            if self.batch_func is not None:
                return self.batch_func(results)
            else:
                return results
    
    def done(self,executor_id):
        print("done join ", executor_id)

# WARNING: aggregation on index match! Not on column match
class AggExecutor(Executor):
    def __init__(self, fill_value = 0, final_func = None):

        # how many things you might checkpoint, the number of keys in the dict
        self.num_states = 1

        self.state = None
        self.fill_value = fill_value
        self.final_func = final_func

    def serialize(self):
        return {0:self.state}, "all"
    
    def deserialize(self, s):
        # the default is to get a list of dictionaries.
        assert type(s) == list and len(s) == 1
        self.state = s[0][0]
    
    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):
        batches = [i for i in batches if i is not None]
        for batch in batches:
            assert type(batch) == pd.core.frame.DataFrame # polars add has no index, will have wierd behavior
            if self.state is None:
                self.state = batch 
            else:
                self.state = self.state.add(batch, fill_value = self.fill_value)
    
    def done(self,executor_id):
        if self.final_func:
            return self.final_func(self.state)
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

        # how many things you might checkpoint, the number of keys in the dict
        self.num_states = 1

        self.state = 0

    def execute(self, batches, stream_id, executor_id):
        
        self.state += sum(len(batch) for batch in batches)
    
    def serialize(self):
        return {0:self.state}, "all"
    
    def deserialize(self, s):
        # the default is to get a list of things 
        assert type(s) == list and len(s) == 1
        self.state = s[0][0]
    
    def done(self, executor_id):
        print("COUNT:", self.state)
        return polars.DataFrame([self.state])


class MergeSortedExecutor(Executor):
    def __init__(self, key, record_batch_rows = None, length_limit = 5000, file_prefix = "mergesort") -> None:
        self.num_states = 0
        self.states = []
        self.num = 1
        self.key = key
        self.record_batch_rows = record_batch_rows
        self.fileno = 0
        self.length_limit = length_limit
        self.prefix = file_prefix # make sure this is different for different executors

        self.filename_to_size = {}
        self.data_dir = "/data"
    
    def serialize(self):
        return {}, "all" # don't support fault tolerance of sort
    
    def deserialize(self, s):
        raise Exception

    def write_out_df_to_disk(self, target_filepath, input_mem_table):
        arrow_table = input_mem_table.to_arrow()
        batches = arrow_table.to_batches(self.record_batch_rows)
        writer =  pa.ipc.new_file(pa.OSFile(target_filepath, 'wb'), arrow_table.schema)
        for batch in batches:
            writer.write(batch)
        writer.close()
    
    # with minimal memory used!
    def produce_sorted_file_from_two_sorted_files(self, target_filepath, input_filepath1, input_filepath2):

        read_time = 0
        sort_time = 0
        write_time = 0

        source1 =  pa.ipc.open_file(pa.memory_map(input_filepath1, 'rb'))
        number_of_batches_in_source1 = source1.num_record_batches
        source2 =  pa.ipc.open_file(pa.memory_map(input_filepath2, 'rb'))
        number_of_batches_in_source2 = source2.num_record_batches

        next_batch_to_get1 = 1

        start = time.time()
        cached_batches_in_mem1 = polars.from_arrow(pa.Table.from_batches([source1.get_batch(0)]))
        next_batch_to_get2 = 1
        cached_batches_in_mem2 = polars.from_arrow(pa.Table.from_batches([source2.get_batch(0)]))
        read_time += time.time() - start

        writer =  pa.ipc.new_file(pa.OSFile(target_filepath, 'wb'), source1.schema)

        # each iteration will write a batch to the target filepath
        while len(cached_batches_in_mem1) > 0 and len(cached_batches_in_mem2) > 0:
            
            disk_portion1 = cached_batches_in_mem1[:self.record_batch_rows]
            disk_portion1['asdasd'] = np.zeros(len(disk_portion1))

            disk_portion2 = cached_batches_in_mem2[:self.record_batch_rows]
            disk_portion2['asdasd'] = np.ones(len(disk_portion2))
            
            start = time.time()
            new_batch = polars.concat([disk_portion1, disk_portion2]).sort(self.key)[:self.record_batch_rows]

            result_idx = polars.concat([disk_portion1.select([self.key, "asdasd"]), disk_portion2.select([self.key, "asdasd"])]).sort(self.key)[:self.record_batch_rows]
            disk_contrib2 = int(result_idx["asdasd"].sum())
            disk_contrib1 = len(result_idx) - disk_contrib2
            
            new_batch = polars.concat([disk_portion1[:disk_contrib1], disk_portion2[:disk_contrib2]]).sort(self.key)[:self.record_batch_rows]
            new_batch.drop_in_place('asdasd')
            sort_time += time.time() - start

            #print(source.schema, new_batch.to_arrow().schema)
            start = time.time()
            writer.write(new_batch.to_arrow().to_batches()[0])
            write_time += time.time() - start

            cached_batches_in_mem1 = cached_batches_in_mem1[disk_contrib1:]
            
            start = time.time()
            if len(cached_batches_in_mem1) < self.record_batch_rows and next_batch_to_get1 < number_of_batches_in_source1:
                next_batch = source1.get_batch(next_batch_to_get1)
                next_batch_to_get1 += 1
                next_batch = polars.from_arrow(pa.Table.from_batches([next_batch]))
                cached_batches_in_mem1 = cached_batches_in_mem1.vstack(next_batch)
            
            cached_batches_in_mem2 = cached_batches_in_mem2[disk_contrib2:]
            if len(cached_batches_in_mem2) < self.record_batch_rows and next_batch_to_get2 < number_of_batches_in_source2:
                next_batch = source2.get_batch(next_batch_to_get2)
                next_batch_to_get2 += 1
                next_batch = polars.from_arrow(pa.Table.from_batches([next_batch]))
                cached_batches_in_mem2 = cached_batches_in_mem2.vstack(next_batch)
            
            read_time += time.time() - start

        
        writer.close()

        process = psutil.Process(os.getpid())
        print("mem usage", process.memory_info().rss, pa.total_allocated_bytes())
        print(read_time, write_time, sort_time)

    def done(self, executor_id):
        
        # first merge all of the in memory states to a file. This makes programming easier and likely not horrible in terms of performance. And we can save some memory! 
        # yolo and hope that that you can concatenate all and not die
        if len(self.states) > 0:
            in_mem_state = polars.concat(self.states).sort(self.key)
            self.write_out_df_to_disk(self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow", in_mem_state)
            self.filename_to_size[self.fileno] = len(in_mem_state)
            self.fileno += 1
            del in_mem_state
        self.states = []

        # now all the states should be strs!
        print("MY DISK STATE", self.filename_to_size.keys())
        sources = [self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(k) + ".arrow" for k in self.filename_to_size]
        return sources
    
    # this is some crazy wierd algo that I came up with, might be there before.
    def execute(self, batches, stream_id, executor_id):
        print("NUMBER OF INCOMING BATCHES", len(batches))
        #print("MY SORT STATE", [(type(i), len(i)) for i in self.states if type(i) == polars.internals.frame.DataFrame])
        import os, psutil
        process = psutil.Process(os.getpid())
        print("mem usage", process.memory_info().rss, pa.total_allocated_bytes())
        batches = deque([batch for batch in batches if batch is not None and len(batch) > 0])
        if len(batches) == 0:
            return

        while len(batches) > 0:
            batch = batches.popleft()
            #batch = batch.sort(self.key)
            print("LENGTH OF INCOMING BATCH", len(batch))
            
            if self.record_batch_rows is None:
                self.record_batch_rows = len(batch)

            if len(batch) > self.length_limit:
                self.write_out_df_to_disk(self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow", batch)
                self.filename_to_size[self.fileno] = len(batch)
                self.fileno += 1
            elif sum([len(i) for i in self.states if type(i) == polars.internals.frame.DataFrame]) + len(batch) > self.length_limit:
                mega_batch = polars.concat([i for i in self.states if type(i) == polars.internals.frame.DataFrame] + [batch]).sort(self.key)
                self.write_out_df_to_disk(self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow", mega_batch)
                self.filename_to_size[self.fileno] = len(mega_batch)
                del mega_batch
                self.fileno += 1
                self.states = []
            else:
                self.states.append(batch)
            
            while len(self.filename_to_size) > 4:
                files_to_merge = [y[0] for y in sorted(self.filename_to_size.items(), key = lambda x: x[1])[:2]]
                self.produce_sorted_file_from_two_sorted_files(self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow", 
                self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(files_to_merge[0]) + ".arrow",
                self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(files_to_merge[1]) + ".arrow")
                self.filename_to_size[self.fileno] = self.filename_to_size.pop(files_to_merge[0]) + self.filename_to_size.pop(files_to_merge[1])
                self.fileno += 1
                os.remove(self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(files_to_merge[0]) + ".arrow")
                os.remove(self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(files_to_merge[1]) + ".arrow")
            
            
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
    
