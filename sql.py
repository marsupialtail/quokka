import pandas as pd
import time

from state import PersistentStateVariable
WRITE_MEM_LIMIT = 16 * 1024 * 1024

class StatelessExecutor:
    def __init__(self) -> None:
        raise NotImplementedError
    def set_early_termination(self):
        self.early_termination = True
    def execute(self,batch,stream_id, executor_id):
        raise NotImplementedError
    def done(self,executor_id):
        raise NotImplementedError

class OutputCSVExecutor(StatelessExecutor):
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

class JoinExecutor(StatelessExecutor):
    # batch func here expects a list of dfs. This is a quark of the fact that join results could be a list of dfs.
    # batch func must return a list of dfs too
    def __init__(self, on = None, left_on = None, right_on = None, left_primary = False, right_primary = False, batch_func = None):
        self.state0 = []
        self.state1 = []
        if on is not None:
            assert left_on is None and right_on is None
            self.left_on = on
            self.right_on = on
        else:
            assert left_on is not None and right_on is not None
            self.left_on = left_on
            self.right_on = right_on
        
        self.left_primary = left_primary
        self.right_primary = right_primary

        # keys that will never be seen again, safe to delete from the state on the other side
        self.left_gone_keys = set()
        self.right_gone_keys = set()

        self.batch_func = batch_func
        self.epoch = 0


    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):
        print("STATE SIZE:", (sum([i.memory_usage().sum() for i in self.state0]) + sum([i.memory_usage().sum() for i in self.state1])) / 1024 / 1024)
        print("STATE LEN:", len(self.state0), len(self.state1))
        batch = pd.concat(batches)
        results = []

        self.epoch += 1

        # state compaction
        if self.epoch % 20 == 0:
            if len(self.state0) > 10:
                self.state0 = [pd.concat(self.state0)]
            if len(self.state1) > 10:
                self.state1 = [pd.concat(self.state1)]
            
            if len(self.left_gone_keys) > 0:
                self.state1 = [i[~i[self.right_on].isin(self.left_gone_keys)].copy() for i in self.state1]
            if len(self.right_gone_keys) > 0:
                self.state0 = [i[~i[self.left_on].isin(self.right_gone_keys)].copy() for i in self.state0]

        if stream_id == 0:
            if len(self.state1) > 0:
                results = [batch.merge(i,left_on = self.left_on, right_on = self.right_on ,how='inner',suffixes=('_a','_b')) for i in self.state1]
            self.state0.append(batch)
            if self.left_primary:
                self.left_gone_keys = self.left_gone_keys | set(batch[self.left_on])
             
        elif stream_id == 1:
            if len(self.state0) > 0:
                results = [i.merge(batch,left_on = self.left_on, right_on = self.right_on ,how='inner',suffixes=('_a','_b')) for i in self.state0]
            self.state1.append(batch)
            if self.right_primary:
                self.right_gone_keys = self.right_gone_keys | set(batch[self.right_on])
        
        if len(results) > 0:
            if self.batch_func is not None:
                return self.batch_func(results)
            else:
                return results
    
    def done(self,executor_id):
        print("done join ", executor_id)

class OOCJoinExecutor(StatelessExecutor):
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

class AggExecutor(StatelessExecutor):
    def __init__(self, fill_value = 0, final_func = None):
        self.state = None
        self.fill_value = fill_value
        self.final_func = final_func

    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):

        for batch in batches:
            if self.state is None:
                self.state = batch 
            else:
                self.state = self.state.add(batch, fill_value = self.fill_value)
    
    def done(self,executor_id):
        if self.final_func:
            print(self.final_func(self.state))
        else:
            print(self.state)

class LimitExecutor(StatelessExecutor):
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
        print(pd.concat(self.state)[:self.limit])

class CountExecutor(StatelessExecutor):
    def __init__(self) -> None:
        self.state = 0

    def execute(self, batches, stream_id, executor_id):
        self.state += sum(len(batch) for batch in batches)
    
    def done(self, executor_id):
        print("COUNT:", self.state)

class MergeSortedExecutor(StatelessExecutor):
    def __init__(self) -> None:
        self.state = None
