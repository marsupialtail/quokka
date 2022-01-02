from io import BytesIO
from typing import Dict
import redis 
from multiprocessing import Pool, Process, Value
import boto3 
import pandas as pd
from io import BytesIO
from dataset import * 
import pyarrow as pa
import copy

'''
What the person should be able to write in the high level API

qc = QuokkaContext()
quotes = qc.InputCSVDataset("yugan","a.csv", ["key","avalue1", "avalue2"], 0)
trades = qc.InputCSVDataset('yugan',"b.csv",["key","bvalue1","bvalue2"], 1)

result = quotes.merge(trades) --defines the computation

# these will cache the result in the Redis clusters on the reducers. 
result.start() -- starts the computation
result.get_current() -- current result, only useful for aggregations etc. 
result.join() -- await finish of computation, synchronous

What the person should be able to do in the low level API
qc = QuokkaContext()
quotes = qc.InputCSVDataset("yugan","a.csv", ["key","avalue1", "avalue2"], 0)
trades = qc.InputCSVDataset('yugan',"b.csv",["key","bvalue1","bvalue2"], 1)

Class Mapper(qc.statelessTask):
	Def __init__():
        pass

    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
	Def execute(input_batch : pd.DataFrame , stream_id: int) -> "dict[int, pd.DataFrame]":
        res = dict(tuple(batch.groupby("key"))) 
        return res  

mapper = Mapper()

# right now there probably can only be several kinds of DSVs, pd.DataFrame(), numpy.Array(), List or Dict
dsv1 = qc.DSV(pd.DataFrame())
dsv2 = qc.DSV(pd.DataFrame())

Class Reducer(qc.statefulTask):
	Def __init__(dsv1, dsv2):
		self.dsvs = [dsv1, dsv2]

    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
	Def execute(input_batch, stream_id):
        if stream_id == 0:
		    self.dsvs[0].append(input_batch)
            return input_batch.merge(self.dsvs[1].local_partition())

        elif stream_id == 1:
            self.dsvs[1].append(input_batch)
            return input_batch.merge(self.dsvs[0].local_partition())

reducer = Reducer(dsv1, dsv2)

Alternatively, if you don't want the runtime to redistribute the state variables, you can just use stateless tasks here too

Class Reducer(qc.statelessTask):
	Def __init__(dsv1, dsv2):
		self.state0 = pd.DataFrame()
        self.state1 = pd.DataFrame()

    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
	Def execute(input_batch, stream_id):
        if stream_id == 0:
		    self.state0 = pd.concat([self.state0, batch])
            return input_batch.merge(self.state1.local_partition())

        elif stream_id == 1:
            self.state1 = pd.concat([self.state1, batch])
            return input_batch.merge(self.state0.local_partition())

reducer = Reducer(dsv1, dsv2)

mapped_quotes_stream = qc.stateless_task(InputStreams = [quotes], FunctionObject = mapper, parallelism = 2) #parallelism is an optional parameter
mapped_trades_stream = qc.stateless_task(InputStreams = [trades], FunctionObject = mapper, parallelism = 2)

joined_stream = qc.stateful_task(InputStreams = [mapped_quotes_stream, mapped_trades_stream], FunctionObject = reducer)
task_handle = joined_stream.to_csv("yugan","test.csv")
task_handle.join()

'''


NUM_REDUCER = 2
MAILBOX_MEM_LIMIT = 1024 * 1024 # 1MB
WRITE_MEM_LIMIT = 10 * 1024 * 1024 # 10MB
context = pa.default_serialization_context()

def mapper(batch: pd.DataFrame ) -> "dict[int, pd.DataFrame]":
    # this needs to produce a dictionary denoting what to forward to each reducer. 

    res = dict(tuple(batch.groupby("key"))) # this is slow. needs to be replaced, but pretty cool it's one line!
    
    return res
        
def mapper_runtime(data: InputCSVDataset, mapper_id: int, mapper):
    
    input_generator = data.get_next_batch(mapper_id)
    
    redisClients = {i: redis.Redis(host='localhost', port=6379, db=i) for i in range(NUM_REDUCER)}

    # what you want is an event loop based implementation with:
    # rule that produces batches to a queue
    # rule that fires off the mapper and pushes reduce results to NUM_REDUCER queues
    # a rule for each reducer that dequeues from the reducer queue and sends it off to the reducer. 

    # current implementation will block upon a single reducer mailbox blockage!

    for batch in input_generator:
        
        result = mapper(batch)

        messages = {i : [] for i in range(NUM_REDUCER)}

        for key in result:

            # we need to replace this simple fixed hash function with something dynamic and synchronized.
            # the first version can just be a multiprocessing.Value which is NUM_REDUCER
            target = int(key) % NUM_REDUCER 
            payload = result[key]
            messages[target].append(payload)

        for target in range(NUM_REDUCER):

            payload = pd.concat(messages[target])
            payload_size = payload.memory_usage().sum()
            r = redisClients[target]

            while True:
                # check if the mailbox is full. if it is full then just spin on this.

                available = r.get("mailbox-mem-used")
                if available is None:
                    continue
                available = int(available)
                # there is a potential problem here where two clients both conclude that there is available space,
                # when there is room for only one of them. But let's ignore this for now. 
                
                if available < MAILBOX_MEM_LIMIT:
                    pipeline = r.pipeline()
                    
                    pipeline.set("mailbox-mem-used",int(available + payload_size))
                    pipeline.lpush("mailbox-id",data.id)
                    pipeline.lpush("mailbox",context.serialize(payload).to_buffer().to_pybytes())
                    results = pipeline.execute()
                    if False in results:
                        raise Exception
                    break
    
    # how do I notify people I am done
    for r in redisClients:
        
        redisClients[r].lpush("mailbox","done")
        redisClients[r].lpush("mailbox-id",data.id)
        print("Im'done")

    

def reducer_runtime(data: OutputCSVDataset, reducer_id : int, left : list):

    import pandas as pd

    r = redis.Redis(host='localhost', port=6379, db=reducer_id)
    r.set("mailbox-mem-used",0)

    # what you really want is an event loop implementation with:
    # rule to dequeue from mailbox and mailbox id and write to output queue
    # rule to dequeue from output queue to sink

    stateA = pd.DataFrame()
    stateB = pd.DataFrame()
    temp_results = pd.DataFrame()

    # we have a problem here, in which we are continuously appending to a state variable
    # this is pretty bad from a memory management perspective, especially in Python
    # ideally we want some kind of static memory allocation and some doubling scheme and the state just grows in this memory region
    # guess need to use C for good perf

    # in this prototype we are just going to use stateA and stateB and pd.concat everytime. Perf will be trash

    while len(left) > 0:

        available = r.llen("mailbox")

        if available:

            pipeline = r.pipeline()
            pipeline.rpop("mailbox")
            pipeline.rpop("mailbox-id")
            [first, second] = pipeline.execute()
            
            df_id = int(second)

            results = None

            if len(first) < 10 and first.decode("utf-8") == "done":
                left.remove(df_id)
                print("done", df_id)
            else:
                batch = context.deserialize(first)
                #print(len(batch),df_id)
                if r.decrby("mailbox-mem-used",int(batch.memory_usage().sum())) is False:
                    raise Exception

                if df_id == 0:
                    if len(stateB) > 0:
                        results = batch.merge(stateB,on='key',how='inner',suffixes=('_a','_b'))
                    stateA = pd.concat([stateA, batch])
                elif df_id == 1:
                    if len(stateA) > 0:
                        results = stateA.merge(batch,on='key',how='inner',suffixes=('_a','_b'))
                    stateB = pd.concat([stateB,batch])
                else:
                    raise Exception

            if results is not None:
                temp_results = pd.concat([temp_results, results])
            if temp_results.memory_usage().sum() > WRITE_MEM_LIMIT:
                print(len(temp_results))
                #data.upload_chunk(temp_results, reducer_id)
                #temp_results = pd.DataFrame()
    
    if len(temp_results) > 0:
        print(len(temp_results))
        temp_results.to_csv("result.csv")
        #data.upload_chunk(temp_results, reducer_id)


def join():

    # IN Python implementation, each process is going to have its own copy of the CSVDataset object. This is ok for InputCSV but for OUtputCSV we need to fish out
    # their parts list at the end and do a complete multipart upload.

    quotes = InputCSVDataset("yugan","a.csv", ["key","avalue1", "avalue2"], 0)
    quotes1 = InputCSVDataset("yugan","a.csv", ["key","avalue1", "avalue2"], 0)
    trades = InputCSVDataset('yugan',"b.csv",["key","bvalue1","bvalue2"], 1)

    results = OutputCSVDataset("yugan","test.csv",0)

    quotes.set_num_mappers(1)
    #quotes1.set_num_mappers(2)
    trades.set_num_mappers(1)
    results.set_num_reducer(1)

    p1 = Process(target = mapper_runtime, args=(quotes, 0, mapper, ))
    #p5 = Process(target = mapper_runtime, args=(quotes1, 1, mapper, ))

    p2 = Process(target = mapper_runtime, args=(trades, 0, mapper, ))
    p3 = Process(target = reducer_runtime, args=(results, 0, [0,1]))
    p4 = Process(target = reducer_runtime, args=(results, 1, [0,1]))


    p1.start()
    p2.start()
    p3.start()
    p4.start()
    #p5.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
    #p5.join()

import time
start = time.time()
join()
print(time.time()-start)