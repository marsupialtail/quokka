import pandas as pd
import time
WRITE_MEM_LIMIT = 16 * 1024 * 1024

class OutputCSVExecutor:
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

class JoinExecutor:
    def __init__(self, key):
        self.state0 = []
        self.state1 = []
        self.temp_results = []
        self.key = key

    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batch, stream_id, executor_id):
        results = []
        start = time.time()        
        if stream_id == 0:
            if len(self.state1) > 0:
                results = [batch.merge(i,on='key',how='inner',suffixes=('_a','_b')) for i in self.state1]
            self.state0.append(batch)
             
        elif stream_id == 1:
            if len(self.state0) > 0:
                results = [i.merge(batch,on='key',how='inner',suffixes=('_a','_b')) for i in self.state0]
            self.state1.append(batch)
        
        if len(results) > 0:
            self.temp_results.extend(results)
            return results
    
    def done(self,executor_id):
        print("temp results",sum([len(i) for i in self.temp_results]))
