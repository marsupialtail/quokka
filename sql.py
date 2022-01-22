import pandas as pd
import time
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
    def execute(self,batch,stream_id, executor_id):
        
        #self.num += 1
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
    def __init__(self, on = None, left_on = None, right_on = None):
        self.state0 = []
        self.state1 = []
        self.temp_results = []
        if on is not None:
            assert left_on is None and right_on is None
            self.left_on = on
            self.right_on = on
        else:
            assert left_on is not None and right_on is not None
            self.left_on = left_on
            self.right_on = right_on


    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batch, stream_id, executor_id):
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
            return results
    
    def done(self,executor_id):
        print("done join ", executor_id)

class AggExecutor(StatelessExecutor):
    def __init__(self, fill_value = 0):
        self.state = None
        self.fill_value = fill_value

    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batch, stream_id, executor_id):
        if self.state is None:
            self.state = batch 
        else:
            self.state = self.state.add(batch, fill_value = self.fill_value)
    
    def done(self,executor_id):
        print(self.state)

class LimitExecutor(StatelessExecutor):
    def __init__(self, limit) -> None:
        self.limit = limit
        self.state = []

    def execute(self, batch, stream_id, executor_id):
        self.state.append(batch)
        length = sum([len(i) for i in self.state])
        if length > self.limit:
            self.set_early_termination()
    
    def done(self):
        print(pd.concat(self.state)[:self.limit])

class CountExecutor(StatelessExecutor):
    def __init__(self) -> None:
        self.state = 0

    def execute(self, batch, stream_id, executor_id):
        self.state += len(batch)
    
    def done(self, executor_id):
        print("COUNT:", self.state)