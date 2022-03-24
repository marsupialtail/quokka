import pickle
import os
os.environ["POLAR_MAX_THREADS"] = "1"
import polars
import pandas as pd
import pyarrow as pa
import time
import numpy as np
import os, psutil
import boto3
import pyarrow.csv as csv
from io import StringIO, BytesIO
from state import PersistentStateVariable
WRITE_MEM_LIMIT = 16 * 1024 * 1024

class Executor:
    def __init__(self) -> None:
        raise NotImplementedError
    def initialize(datasets):
        pass
    def set_early_termination(self):
        self.early_termination = True
    def execute(self,batch,stream_id, executor_id):
        raise NotImplementedError
    def done(self,executor_id):
        raise NotImplementedError    

class OutputCSVExecutor(Executor):
    def __init__(self, parallelism, bucket, prefix) -> None:
        self.num = 0
        self.parallelism = parallelism
        self.bucket = bucket
        self.prefix = prefix
        self.dfs =[]
        pass
    def execute(self,batches,stream_id, executor_id):
        
        #self.num += 1
        batch = pd.concat(batches)
        self.dfs.append(batch)
        if sum([i.memory_usage().sum() for i in self.dfs]) > WRITE_MEM_LIMIT:
            name = "s3://" + self.bucket + "/" + self.prefix + "-" + str(self.num * self.parallelism + executor_id) + ".csv"
            pd.concat(self.dfs).to_csv(name)
            self.num += 1
            self.dfs = []

    def done(self,executor_id):
        name = "s3://" + self.bucket + "/" + self.prefix + "-" + str(self.num * self.parallelism + executor_id) + ".csv"
        pd.concat(self.dfs).to_csv(name)
        print("done")


class PolarJoinExecutor(Executor):
    # batch func here expects a list of dfs. This is a quark of the fact that join results could be a list of dfs.
    # batch func must return a list of dfs too
    def __init__(self, on = None, left_on = None, right_on = None, batch_func = None):

        # how many things you might checkpoint, the number of keys in the dict
        self.num_states = 2

        self.state0 = None
        self.state1 = None
        self.ckpt_start0 = 0
        self.ckpt_start1 = 0
        self.lengths = {0:0, 1:0}

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
        batch = polars.concat(batches)
        self.lengths[stream_id] += 1
        print("state", self.lengths)
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
        
        if result is not None and len(result) > 0:
            if self.batch_func is not None:
                da =  self.batch_func(result.to_pandas())
                return da
            else:
                print("RESULT LENGTH",len(result))
                return result
    
    def done(self,executor_id):
        print(len(self.state0),len(self.state1))
        print("done join ", executor_id)


class Polar3JoinExecutor(Executor):
    # batch func here expects a list of dfs. This is a quark of the fact that join results could be a list of dfs.
    # batch func must return a list of dfs too
    def __init__(self, on = None, left_on = None, right_on = None, batch_func = None):
        self.state0 = None
        self.state1 = None
        self.lengths = {0:0, 1:0}

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
        return pickle.dumps({"state0":self.state0, "state1":self.state1})
    
    def deserialize(self, s):
        stuff = pickle.loads(s)
        self.state0 = stuff["state0"]
        self.state1 = stuff["state1"]
    
    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):
        # state compaction
        batch = polars.concat(batches)
        self.lengths[stream_id] += 1
        print("state", self.lengths)
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
        
        if result is not None and len(result) > 0:
            if self.batch_func is not None:
                da =  self.batch_func(result.to_pandas())
                return da
            else:
                print("RESULT LENGTH",len(result))
                return result
    
    def done(self,executor_id):
        print(len(self.state0),len(self.state1))
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
        for batch in batches:
            assert type(batch) == pd.core.frame.DataFrame # polars add has no index, will have wierd behavior
            if self.state is None:
                self.state = batch 
            else:
                self.state = self.state.add(batch, fill_value = self.fill_value)
        assert(len(self.state) == 2)
    
    def done(self,executor_id):
        print(self.state)
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

class StorageExecutor(Executor):
    def __init__(self) -> None:
        pass
    def execute(self, batch, stream_id, executor_id):
        return batch
    def done(self, executor_id):
        pass

class MergedStorageExecutor(Executor):
    def __init__(self, final_func = None) -> None:
        self.state = []
        self.num_states = 0
    def execute(self, batches, stream_id, executor_id):
        self.state.extend(batches)
    def done(self, executor_id):
        return pd.concat(self.state)

class MergeSortedExecutor(Executor):
    def __init__(self, key, record_batch_rows = None, length_limit = 5000, file_prefix = "mergesort", output_line_limit = 1000) -> None:
        self.num_states = 0
        self.states = []
        self.num = 1
        self.key = key
        self.record_batch_rows = record_batch_rows
        self.fileno = 0
        self.length_limit = length_limit
        self.prefix = file_prefix # make sure this is different for different executors

        self.output_line_limit = output_line_limit
        self.bucket = "quokka-sorted-lineitem"
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

        source1 =  pa.ipc.open_file(pa.memory_map(input_filepath1, 'rb'))
        number_of_batches_in_source1 = source1.num_record_batches
        source2 =  pa.ipc.open_file(pa.memory_map(input_filepath2, 'rb'))
        number_of_batches_in_source2 = source2.num_record_batches

        next_batch_to_get1 = 1
        cached_batches_in_mem1 = polars.from_arrow(pa.Table.from_batches([source1.get_batch(0)]))

        next_batch_to_get2 = 1
        cached_batches_in_mem2 = polars.from_arrow(pa.Table.from_batches([source2.get_batch(0)]))

        writer =  pa.ipc.new_file(pa.OSFile(target_filepath, 'wb'), source1.schema)

        # each iteration will write a batch to the target filepath
        while len(cached_batches_in_mem1) > 0 and len(cached_batches_in_mem2) > 0:
            
            disk_portion1 = cached_batches_in_mem1[:self.record_batch_rows]
            disk_portion1['asdasd'] = np.zeros(len(disk_portion1))

            disk_portion2 = cached_batches_in_mem2[:self.record_batch_rows]
            disk_portion2['asdasd'] = np.ones(len(disk_portion2))

            new_batch = polars.concat([disk_portion1, disk_portion2]).sort(self.key)[:self.record_batch_rows]
            disk_contrib2 = int(new_batch['asdasd'].sum())
            disk_contrib1 = len(new_batch) - disk_contrib2
            new_batch = new_batch.drop('asdasd')

            #print(source.schema, new_batch.to_arrow().schema)
            writer.write(new_batch.to_arrow().to_batches()[0])
            
            cached_batches_in_mem1 = cached_batches_in_mem1[disk_contrib1:]
            if len(cached_batches_in_mem1) < self.record_batch_rows and next_batch_to_get1 < number_of_batches_in_source1:
                next_batch = source1.get_batch(next_batch_to_get1)
                next_batch_to_get1 += 1
                next_batch = polars.from_arrow(pa.Table.from_batches([next_batch]))
                cached_batches_in_mem1 = polars.concat([cached_batches_in_mem1, next_batch])
            
            cached_batches_in_mem2 = cached_batches_in_mem2[disk_contrib2:]
            if len(cached_batches_in_mem2) < self.record_batch_rows and next_batch_to_get2 < number_of_batches_in_source2:
                next_batch = source2.get_batch(next_batch_to_get2)
                next_batch_to_get2 += 1
                next_batch = polars.from_arrow(pa.Table.from_batches([next_batch]))
                cached_batches_in_mem2 = polars.concat([cached_batches_in_mem2, next_batch])
        
        writer.close()

    # with minimal memory used!
    def produce_sorted_file_from_sorted_file_and_in_memory(self, target_filepath, input_filepath, input_mem_table):

        source =  pa.ipc.open_file(pa.memory_map(input_filepath, 'rb'))
        number_of_batches_in_source = source.num_record_batches

        # always keep two record batches from the disk file in memory. This one and the next one.
        next_batch_to_get = 1
        # all the batches must be of size self.record_batch_rows, except possibly the last one.
        cached_batches_in_mem = polars.from_arrow(pa.Table.from_batches([source.get_batch(0)]))

        current_ptr_in_mem = 0

        writer =  pa.ipc.new_file(pa.OSFile(target_filepath, 'wb'), source.schema)

        # each iteration will write a batch to the target filepath
        while len(cached_batches_in_mem) > 0 and current_ptr_in_mem < len(input_mem_table):
            in_mem_portion = input_mem_table[current_ptr_in_mem: current_ptr_in_mem + self.record_batch_rows]
            # let's hope that there is no column called asdasd. That's why I am not going to use a more common name like idx.
            in_mem_portion['asdasd'] = np.zeros(len(in_mem_portion))
            disk_portion = cached_batches_in_mem[:self.record_batch_rows]
            disk_portion['asdasd'] = np.ones(len(disk_portion))

            new_batch = polars.concat([in_mem_portion, disk_portion]).sort(self.key)[:self.record_batch_rows]
            disk_contrib = int(new_batch['asdasd'].sum())
            in_mem_contrib = len(new_batch) - disk_contrib
            new_batch = new_batch.drop('asdasd')

            #print(source.schema, new_batch.to_arrow().schema)
            writer.write(new_batch.to_arrow().to_batches()[0])
            # now get rid of the contributions
            current_ptr_in_mem += in_mem_contrib
            cached_batches_in_mem = cached_batches_in_mem[disk_contrib:]
            if len(cached_batches_in_mem) < self.record_batch_rows and next_batch_to_get < number_of_batches_in_source:
                next_batch = source.get_batch(next_batch_to_get)
                next_batch_to_get += 1
                next_batch = polars.from_arrow(pa.Table.from_batches([next_batch]))
                cached_batches_in_mem = polars.concat([cached_batches_in_mem, next_batch])
        
        writer.close()
        
    # this is some crazy wierd algo that I came up with, might be there before.
    def execute(self, batches, stream_id, executor_id):

        batch = polars.concat(batches)
        if self.record_batch_rows is None:
            self.record_batch_rows = len(batch)

        highest_power_of_two = int(np.log2(self.num & (~(self.num - 1))))

        if highest_power_of_two == 0:
            self.states.append(batch)
        else:
            self.states[-1] = polars.concat([self.states[-1], batch]).sort(self.key)
            for k in range(1, highest_power_of_two):
                if type(self.states[-2]) == polars.internals.frame.DataFrame and type(self.states[-1]) == polars.internals.frame.DataFrame:
                    self.states[-2 ] = polars.concat([self.states[-2 ], self.states[-1]]).sort(self.key)
                    if len(self.states[-2 ]) >  self.length_limit:
                        self.write_out_df_to_disk(self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow", self.states[-2])
                        self.states[-2] = self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow"
                        self.fileno += 1
                    del self.states[-1 ]
                elif type(self.states[-2]) == str and type(self.states[-1]) == str:
                    self.produce_sorted_file_from_two_sorted_files(self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow", self.states[-2], self.states[-1])
                    os.remove(self.states[-2])
                    self.states[-2] = self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow"
                    self.fileno += 1
                    os.remove(self.states[-1])
                    del self.states[-1]
                elif type(self.states[-2]) == str and type(self.states[-1]) == polars.internals.frame.DataFrame:
                    self.produce_sorted_file_from_sorted_file_and_in_memory(self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow", self.states[-2], self.states[-1])
                    os.remove(self.states[-2])
                    self.states[-2] = self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow"
                    self.fileno += 1
                    del self.states[-1]
                else:
                    raise Exception("this should never happen", self.states[-2],self.states[-1])

        self.num += 1
    
    def done(self, executor_id):
        if len(self.states) == 1:
            return self.states[0]
        while len(self.states) > 1:
            if type(self.states[-2]) == polars.internals.frame.DataFrame and type(self.states[-1]) == polars.internals.frame.DataFrame:
                self.states[-2 ] = polars.concat([self.states[-2 ], self.states[-1]]).sort(self.key)
                if len(self.states[-2 ]) >  self.length_limit:
                    self.write_out_df_to_disk(self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow", self.states[-2])
                    self.states[-2] = self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow"
                    self.fileno += 1
                del self.states[-1 ]
            elif type(self.states[-2]) == str and type(self.states[-1]) == polars.internals.frame.DataFrame:
                self.produce_sorted_file_from_sorted_file_and_in_memory(self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow", self.states[-2], self.states[-1])
                os.remove(self.states[-2])
                self.states[-2] = self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow"
                self.fileno += 1
                del self.states[-1]
            elif type(self.states[-2]) == str and type(self.states[-1]) == str:
                self.produce_sorted_file_from_two_sorted_files(self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow", self.states[-2], self.states[-1])
                os.remove(self.states[-2])
                self.states[-2] = self.data_dir + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow"
                self.fileno += 1
                os.remove(self.states[-1])
                del self.states[-1]

        s3_resource = boto3.resource('s3')

        name = 0
        if type(self.states[0]) == polars.internals.frame.DataFrame:
            
            for start in range(0, len(self.states[0]), self.output_line_limit):
                da = BytesIO()
                csv.write_csv(self.states[0][start: start + self.output_line_limit].to_arrow(), da, write_options = csv.WriteOptions(include_header=False))
                s3_resource.Object(self.bucket,str(executor_id) + "-" + str(name) + ".csv").put(Body=da.getvalue())
                name += 1

        elif type(self.states[0]) == str:
            source =  pa.ipc.open_file(pa.memory_map(self.states[0], 'rb'))
            number_of_batches_in_source = source.num_record_batches
            batch = polars.from_arrow(pa.Table.from_batches([source.get_batch(0)]))
            for i in range(1, number_of_batches_in_source):
                while len(batch) > self.output_line_limit:
                    da = BytesIO()
                    csv.write_csv(batch[:self.output_line_limit].to_arrow(), da, write_options = csv.WriteOptions(include_header=False))
                    s3_resource.Object(self.bucket,str(executor_id) + "-" + str(name) + ".csv").put(Body=da.getvalue())
                    print(name)
                    name += 1
                    batch = batch[self.output_line_limit:]
                batch.vstack(polars.from_arrow(pa.Table.from_batches([source.get_batch(i)])),in_place=True)
            print(len(batch),name)
            for start in range(0, len(batch), self.output_line_limit):
                da = BytesIO()
                csv.write_csv(batch[start: start + self.output_line_limit].to_arrow(), da, write_options = csv.WriteOptions(include_header=False))
                s3_resource.Object(self.bucket,str(executor_id) + "-" + str(name) + ".csv").put(Body=da.getvalue())
                name += 1

        return self.states[0]
        # self.state = polars.concat(self.states).sort(self.key)
        # print(self.state)

#stuff = []
#exe = MergeSortedExecutor('0', length_limit=10000)
#for k in range(10):
#    item = polars.from_pandas(pd.DataFrame(np.random.normal(size=(1000 - k * 50,1000))))
#    exe.execute(item, 0, 0)
#exe.done(0)

# exe = MergeSortedExecutor('0', 3000)
# a = polars.from_pandas(pd.DataFrame(np.random.normal(size=(10000,1000)))).sort('0')
# b = polars.from_pandas(pd.DataFrame(np.random.normal(size=(10000,1000)))).sort('0')

# exe.write_out_df_to_disk("file.arrow", a)

# del a
# process = psutil.Process(os.getpid())
# print(process.memory_info().rss)
# exe.produce_sorted_file_from_sorted_file_and_in_memory("file2.arrow","file.arrow",b)
# exe.produce_sorted_file_from_two_sorted_files("file3.arrow","file2.arrow","file.arrow")
