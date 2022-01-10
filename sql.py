import pandas as pd
import time
WRITE_MEM_LIMIT = 10 * 1024 * 1024

class JoinExecutor:
    def __init__(self, key):
        self.state0 = []
        self.state1 = []
        self.temp_results = []
        self.key = key

    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batch, stream_id):
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
            print("temp results",sum([len(i) for i in self.temp_results]))
        print("execute,",time.time()-start)
