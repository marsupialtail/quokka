import os
import polars
import pandas as pd
os.environ['ARROW_DEFAULT_MEMORY_POOL'] = 'system'

import pyarrow as pa
import time
import numpy as np
import os, psutil
import pyarrow.parquet as pq
from collections import deque
import pyarrow.compute as compute
import random
import sys
from pyarrow.fs import S3FileSystem, LocalFileSystem
import pyarrow.dataset as ds

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
    def __init__(self, filepath, format, prefix = "part", mode = "local", row_group_size = 5000000) -> None:
        self.num = 0
        assert format == "csv" or format == "parquet"
        self.format = format
        self.filepath = filepath
        self.prefix = prefix
        self.row_group_size = row_group_size
        self.my_batches = []
        self.name = 0
        self.mode = mode

    def serialize(self):
        return {}, "all"
    
    def deserialize(self, s):
        pass

    def execute(self,batches,stream_id, executor_id):

        fs = LocalFileSystem() if self.mode == "local" else S3FileSystem()
        self.my_batches.extend([i for i in batches if i is not None])


        '''
        You want to use Payrrow's write table API to flush everything at once. to get the parallelism.
        Being able to write multiple row groups at once really speeds things up. It's okay if you are slow at first
        because you are uploading/writing things one at a time
        '''

        lengths = [len(batch) for batch in self.my_batches]
        total_len = np.sum(lengths)

        print(time.time(), len(self.my_batches), total_len)

        write_len = total_len // self.row_group_size * self.row_group_size
        cum_sum = np.cumsum(lengths)
        if len(np.where(cum_sum > write_len)[0]) == 0:
            batches_to_take, rows_remaining, rows_to_take = len(lengths), 0,0
        batches_to_take = np.where(cum_sum > write_len)[0][0]

        if batches_to_take == 0:
            return None

        rows_remaining = cum_sum[batches_to_take] - write_len
        rows_to_take = lengths[batches_to_take] - rows_remaining

        write_batch = polars.concat(self.my_batches[:batches_to_take])

        self.my_batches = self.my_batches[batches_to_take:]
        if rows_to_take > 0:
            write_batch.vstack(self.my_batches[0][:rows_to_take], in_place=True)
            self.my_batches[0] = self.my_batches[0][rows_to_take:]

        write_batch = write_batch.to_arrow()
        if self.format == "csv":
            for i, (col_name, type_) in enumerate(zip(write_batch.schema.names, write_batch.schema.types)):
                if pa.types.is_decimal(type_):
                    write_batch = write_batch.set_column(i, col_name, compute.cast(write_batch.column(col_name), pa.float64()))


        assert len(write_batch) % self.row_group_size == 0
        ds.write_dataset(write_batch,base_dir = self.filepath, 
            basename_template = self.prefix + "-" + str(executor_id) + "-" + str(self.name) + "-{i}." + self.format, format=self.format, filesystem = fs,
            existing_data_behavior='overwrite_or_ignore',
            max_rows_per_file=self.row_group_size,max_rows_per_group=self.row_group_size)
        
        return_df = polars.from_dict({"filename":[(self.prefix + "-" + str(executor_id) + "-" + str(self.name) + "-" + str(i) + "." + self.format) for i in range(len(write_batch) // self.row_group_size) ]})
        self.name += 1
        return return_df

    def done(self,executor_id):
        df = polars.concat(self.my_batches)
        #print(df)
        fs = LocalFileSystem() if self.mode == "local" else S3FileSystem()
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
        self.how = how

        if type(small_table) == pd.core.frame.DataFrame:
            self.state = polars.from_pandas(small_table)
        elif type(small_table) == polars.internals.DataFrame:
            self.state = small_table
        else:
            raise Exception("small table data type not accepted")

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
        result = batch.join(self.state, left_on = self.big_on, right_on = self.small_on, how = self.how, suffix = self.suffix)

        if result is not None and len(result) > 0:
            return result
    
    def done(self,executor_id):
        #print(len(self.state0),len(self.state1))
        #print("done join ", executor_id)
        pass


class GroupAsOfJoinExecutor():
    # batch func here expects a list of dfs. This is a quark of the fact that join results could be a list of dfs.
    # batch func must return a list of dfs too
    def __init__(self, group_on= None, group_left_on = None, group_right_on = None, on = None, left_on = None, right_on = None, suffix="_right"):

        self.trade = {}
        self.quote = {}
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
        
        if group_on is not None:
            assert group_left_on is None and group_right_on is None
            self.group_left_on = group_on
            self.group_right_on = group_on
        else:
            assert group_left_on is not None and group_right_on is not None
            self.group_left_on = group_left_on
            self.group_right_on = group_right_on

    def serialize(self):
        result = {0:self.trade, 1:self.quote}        
        return result, "all"
    
    def deserialize(self, s):
        assert type(s) == list
        self.trade = s[0][0]
        self.quote = s[0][1]
    
    def find_second_smallest(self, batch, key):
        smallest = batch[0][key]
        for i in range(len(batch)):
            if batch[i][key] > smallest:
                return batch[i][key]
    
    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):
        # state compaction
        batches = [i for i in batches if len(i) > 0]
        if len(batches) == 0:
            return
        
        # self.trade will be a dictionary of lists. 
        # self.quote will be a dictionary of lists.

        # trade
        ret_vals = []
        if stream_id == 0:
            for batch in batches:
                frames = batch.partition_by(self.group_left_on)
                for trade_chunk in frames:
                    symbol = trade_chunk["symbol"][0]
                    min_trade_ts = trade_chunk[self.left_on][0]
                    max_trade_ts = trade_chunk[self.left_on][-1]
                    if symbol not in self.quote:
                        if symbol in self.trade:
                            self.trade[symbol].append(trade_chunk)
                        else:
                            self.trade[symbol] = [trade_chunk]
                        continue
                    current_quotes_for_symbol = self.quote[symbol]
                    for i in range(len(current_quotes_for_symbol)):
                        quote_chunk = current_quotes_for_symbol[i]
                        min_quote_ts = quote_chunk[self.right_on][0]
                        max_quote_ts = quote_chunk[self.right_on][-1]
                        #print(max_trade_ts, min_quote_ts, min_trade_ts, max_quote_ts)
                        if max_trade_ts < min_quote_ts or min_trade_ts > max_quote_ts:
                            # no overlap.
                            continue
                        else:
                            second_smallest_quote_ts = self.find_second_smallest(quote_chunk, self.right_on)
                            joinable_trades = trade_chunk[(trade_chunk[self.left_on] >= second_smallest_quote_ts) & (trade_chunk[self.left_on] < max_quote_ts)]
                            if len(joinable_trades) == 0:
                                continue
                            trade_start_ts = joinable_trades[self.left_on][0]
                            trade_end_ts = joinable_trades[self.left_on][-1]
                            if len(joinable_trades) == 0:
                                continue
                            quote_start_ts = quote_chunk[self.right_on][quote_chunk[self.right_on] <= trade_start_ts][-1]
                            quote_end_ts = quote_chunk[self.right_on][quote_chunk[self.right_on] <= trade_end_ts][-1]
                            joinable_quotes = quote_chunk[(quote_chunk[self.right_on] >= quote_start_ts) & (quote_chunk[self.right_on] <= quote_end_ts)]
                            if len(joinable_quotes) == 0:
                                continue
                            trade_chunk = trade_chunk[(trade_chunk[self.left_on] < trade_start_ts) | (trade_chunk[self.left_on] > trade_end_ts)]
                            new_chunk = quote_chunk[(quote_chunk[self.right_on] < quote_start_ts) | (quote_chunk[self.left_on] > quote_end_ts)]
                            
                            self.quote[symbol][i] = new_chunk
                            
                            ret_vals.append(joinable_trades.join_asof(joinable_quotes.drop(self.group_right_on), left_on = self.left_on, right_on = self.right_on))
                            if len(trade_chunk) == 0:
                                break
                    
                    self.quote[symbol] = [i for i in self.quote[symbol] if len(i) > 0]

                    if len(trade_chunk) == 0:
                        continue
                    if symbol in self.trade:
                        self.trade[symbol].append(trade_chunk)
                    else:
                        self.trade[symbol] = [trade_chunk]
        #quote
        elif stream_id == 1:
            for batch in batches:
                frames = batch.partition_by(self.group_right_on)
                for quote_chunk in frames:
                    symbol = quote_chunk["symbol"][0]
                    min_quote_ts = quote_chunk[self.right_on][0]
                    max_quote_ts = quote_chunk[self.right_on][-1]
                    if symbol not in self.trade:
                        if symbol in self.quote:
                            self.quote[symbol].append(quote_chunk)
                        else:
                            self.quote[symbol] = [quote_chunk]
                        continue
                        
                    current_trades_for_symbol = self.trade[symbol]
                    for i in range(len(current_trades_for_symbol)):
                        trade_chunk = current_trades_for_symbol[i]
                        #print(current_trades_for_symbol)
                        min_trade_ts = trade_chunk[self.left_on][0]
                        max_trade_ts = trade_chunk[self.left_on][-1]
                        if max_trade_ts < min_quote_ts or min_trade_ts > max_quote_ts:
                            # no overlap.
                            continue
                        else:
                            second_smallest_quote_ts = self.find_second_smallest(quote_chunk, self.right_on)
                            joinable_trades = trade_chunk[(trade_chunk[self.left_on] >= second_smallest_quote_ts) &( trade_chunk[self.left_on] < max_quote_ts)]
                            if len(joinable_trades) == 0:
                                continue
                            trade_start_ts = joinable_trades[self.left_on][0]
                            trade_end_ts = joinable_trades[self.left_on][-1]
                            if len(joinable_trades) == 0:
                                continue
                            quote_start_ts = quote_chunk[self.right_on][quote_chunk[self.right_on] <= trade_start_ts][-1]
                            quote_end_ts = quote_chunk[self.right_on][quote_chunk[self.right_on] <= trade_end_ts][-1]
                            joinable_quotes = quote_chunk[(quote_chunk[self.right_on] >= quote_start_ts) & (quote_chunk[self.right_on] <= quote_end_ts)]
                            if len(joinable_quotes) == 0:
                                continue
                            quote_chunk = quote_chunk[(quote_chunk[self.right_on] < quote_start_ts ) | (quote_chunk[self.left_on] > quote_end_ts)]
                            new_chunk = trade_chunk[(trade_chunk[self.left_on] < trade_start_ts) | (trade_chunk[self.left_on] > trade_end_ts)]
                            
                            self.trade[symbol][i] = new_chunk

                            ret_vals.append(joinable_trades.join_asof(joinable_quotes.drop(self.group_right_on), left_on = self.left_on, right_on = self.right_on))
                            if len(quote_chunk) == 0:
                                break
                    
                    self.trade[symbol] = [i for i in self.trade[symbol] if len(i) > 0]
                    if len(quote_chunk) == 0:
                        continue
                    if symbol in self.quote:
                        self.quote[symbol].append(quote_chunk)
                    else:
                        self.quote[symbol] = [quote_chunk]
        #print(ret_vals)

        if len(ret_vals) == 0:
            return
        for thing in ret_vals:
            print(len(thing))
            print(thing[thing.symbol=="ZU"])
        result = polars.concat(ret_vals).drop_nulls()

        if result is not None and len(result) > 0:
            return result
    
    def done(self,executor_id):
        #print(len(self.state0),len(self.state1))
        ret_vals = []
        for symbol in self.trade:
            if symbol not in self.quote:
                continue
            else:
                trades = polars.concat(self.trade[symbol]).sort(self.left_on)
                quotes = polars.concat(self.quote[symbol]).sort(self.right_on)
                ret_vals.append(trades.join_asof(quotes.drop(self.group_right_on), left_on = self.left_on, right_on = self.right_on, suffix=self.suffix))
        
        print("done asof join ", executor_id)
        return polars.concat(ret_vals).drop_nulls()

class PolarJoinExecutor(Executor):
    # batch func here expects a list of dfs. This is a quark of the fact that join results could be a list of dfs.
    # batch func must return a list of dfs too
    def __init__(self, on = None, left_on = None, right_on = None, suffix="_right", how = "inner"):

        self.state0 = None
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
        self.how = how
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
                    result = batch.join(self.state1,left_on = self.left_on, right_on = self.right_on ,how=self.how, suffix=self.suffix)
                except:
                    print(batch)
            if self.state0 is None:
                self.state0 = batch
            else:
                self.state0.vstack(batch, in_place = True)
             
        elif stream_id == 1:
            if self.state0 is not None:
                result = self.state0.join(batch,left_on = self.left_on, right_on = self.right_on ,how=self.how, suffix=self.suffix)
            if self.state1 is None:
                self.state1 = batch
            else:
                self.state1.vstack(batch, in_place = True)

        if result is not None and len(result) > 0:
            return result
    
    def done(self,executor_id):
        #print(len(self.state0),len(self.state1))
        #print("done join ", executor_id)
        pass

class DistinctExecutor(Executor):
    def __init__(self, keys) -> None:

        self.keys = keys
        self.state = None

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
            self.state.vstack(contribution)
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
        return self.state


class MergeSortedExecutor(Executor):
    def __init__(self, key, record_batch_rows = None, length_limit = 5000, file_prefix = "mergesort") -> None:
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
        #print("MY SORT STATE", [(type(i), len(i)) for i in self.states if type(i) == polars.internals.DataFrame])
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
            elif sum([len(i) for i in self.states if type(i) == polars.internals.DataFrame]) + len(batch) > self.length_limit:
                mega_batch = polars.concat([i for i in self.states if type(i) == polars.internals.DataFrame] + [batch]).sort(self.key)
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
    
