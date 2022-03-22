import pickle
import os
os.environ["POLAR_MAX_THREADS"] = "1"
import polars
import pandas as pd
import time

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
        result = {0:self.state0[self.ckpt_start0:], 1:self.state1[self.ckpt_start1:]}
        if self.state0 is not None:
            self.ckpt_start0 += len(self.state0)
        if self.state1 is not None:
            self.ckpt_start1 += len(self.state1)
        return result, "inc"
    
    def deserialize(self, s):
        assert type(s) == list
        self.state0 = polars.concat([i[0] for i in s if i[0] is not None])
        self.state1 = polars.concat([i[1] for i in s if i[1] is not None])
        self.ckpt_start0 = len(self.state0)
        self.ckpt_start1 = len(self.state1)
    
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
    def execute(self, batches, stream_id, executor_id):
        self.state.extend(batches)
    def done(self, executor_id):
        return pd.concat(self.state)

class MergeSortedExecutor(Executor):
    def __init__(self) -> None:
        self.state = None
