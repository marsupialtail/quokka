import pandas as pd

WRITE_MEM_LIMIT = 10 * 1024 * 1024

class JoinExecutor:
    def __init__(self, key):
        self.state0 = pd.DataFrame()
        self.state1 = pd.DataFrame()
        self.temp_results = pd.DataFrame()
        self.key = key

    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batch, stream_id):
        results = None

        if stream_id == 0:
            if len(self.state1) > 0:
                results = batch.merge(self.state1,on='key',how='inner',suffixes=('_a','_b'))
            self.state0 = pd.concat([self.state0, batch])
             
        elif stream_id == 1:
            if len(self.state0) > 0:
                results = self.state0.merge(batch,on='key',how='inner',suffixes=('_a','_b'))
            self.state1 = pd.concat([self.state1, batch])
        
        if results is not None:
            self.temp_results = pd.concat([self.temp_results, results])
            print("temp results",len(self.temp_results))
        if self.temp_results.memory_usage().sum() > WRITE_MEM_LIMIT:
            print(len(self.temp_results))
