from collections import deque
from dataset import * 
import redis
import pyarrow as pa
import pandas as pd
from multiprocessing import Process
import time
'''

In this system, there are two things. The first are task nodes and the second are streams. They can be defined more or less
indepednently from one another. The Task

'''

'''
Since each stream can have only one input, we are just going to use the source node id as the stream id. All ids are thus unique. 
'''

context = pa.default_serialization_context()
WRITE_MEM_LIMIT = 10 * 1024 * 1024

class Stream:

    def __init__(self, source_node_id) -> None:
        
        self.source = source_node_id
        self.targets = []
        self.r = redis.Redis(host='localhost', port=6379, db=0)

        pass    


    def push(self, data):

        if len(self.targets) == 0:
            raise Exception
        
        else:
            # iterate over downstream task nodes, each of which may contain inner parallelism
            # distribution strategy depends on data type as well as partition function
            if type(data) == pd.core.frame.DataFrame:
                data = dict(tuple(data.groupby("key")))

                for target, parallelism in self.targets:
                    messages = {i : [] for i in range(parallelism)}
                    for key in data:
                        # replace with some real partition function
                        channel = int(key) % parallelism
                        payload = data[key]
                        messages[channel].append(payload)
                    for channel in range(parallelism):
                        payload = pd.concat(messages[channel])
                        # don't worry about target being full for now.
                        print("not checking if target is full. This will break with larger joins for sure.")
                        pipeline = self.r.pipeline()
                        pipeline.publish("mailbox-"+str(target) + "-" + str(channel),context.serialize(payload).to_buffer().to_pybytes())
                        pipeline.publish("mailbox-id-"+str(target) + "-" + str(channel),self.source)
                        results = pipeline.execute()
                        if False in results:
                            raise Exception
    def done(self):
        for target, parallelism in self.targets:
            for channel in range(parallelism):
                pipeline = self.r.pipeline()
                pipeline.publish("mailbox-"+str(target) + "-" + str(channel),"done")
                pipeline.publish("mailbox-id-"+str(target) + "-" + str(channel),self.source)
                results = pipeline.execute()
                if False in results:
                    raise Exception


class TaskGraph:
    # this keeps the logical dependency DAG between tasks 
    def __init__(self) -> None:
        self.current_node = 0
        self.nodes = {}
    
    def new_input_csv(self, bucket, key, names, parallelism):
        tasknode = InputCSVNode([],None,self.current_node, parallelism=parallelism)
        tasknode.set_name(bucket,key,names)
        output_stream = Stream(self.current_node)
        tasknode.output_stream = output_stream
        self.nodes[self.current_node] = tasknode
        self.current_node += 1
        return output_stream
    
    def new_stateless_node(self, streams, functionObject, parallelism):
        mapping = {}
        for key in streams:
            stream = streams[key]
            if stream.source not in self.nodes:
                raise Exception("stream source not registered")
            stream.targets.append((self.current_node,parallelism))
            mapping[stream.source] = key
        tasknode = StatelessTaskNode(streams, functionObject, self.current_node, parallelism)
        tasknode.physical_to_logical_stream_mapping = mapping
        output_stream = Stream(self.current_node)
        tasknode.output_stream = output_stream
        self.nodes[self.current_node] = tasknode
        self.current_node += 1
        return output_stream
    
    def new_output_csv(self):

        # this does not return anything
        return 
    
    def initialize(self):
        for node in self.nodes:
            self.nodes[node].initialize()
    
    def run(self):
        processes = []
        for key in self.nodes:
            node = self.nodes[key]
            for replica in range(node.parallelism):
                processes.append(Process(target = node.execute, args=(replica,)))
        for process in processes:
            process.start()
        for process in processes:
            process.join()




class TaskNode:

    def __init__(self, streams, functionObject, id, parallelism) -> None:
        self.functionObjects = [functionObject for i in range(parallelism)]
        self.input_streams = streams
        self.output_stream = None
        self.id = id
        self.parallelism = parallelism
        # this maps what the system's stream_id is to what the user called the stream when they created the task node
        self.physical_to_logical_stream_mapping = {}
        pass

    def initialize(self):
        pass

    def execute(self):
        pass

class JoinExecutor:
    def __init__(self):
        self.state0 = pd.DataFrame()
        self.state1 = pd.DataFrame()
        self.temp_results = pd.DataFrame()

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
            print(len(self.temp_results))
        if self.temp_results.memory_usage().sum() > WRITE_MEM_LIMIT:
            print(len(self.temp_results))


class StatelessTaskNode(TaskNode):

    # this is for one of the parallel threads

    def initialize(self):
        pass

    def execute(self, my_id):

        # this needs to change

        r = redis.Redis(host='localhost', port=6379, db=0)
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe("mailbox-" + str(self.id) + "-" + str(my_id), "mailbox-id-" + str(self.id) + "-" + str(my_id))

        assert my_id < self.parallelism

        mailbox = deque()
        mailbox_id = deque()

        if self.output_stream is None:
            raise Exception
        while len(self.input_streams) > 0:
            message = p.get_message()
            if message is None:
                continue
            if message['channel'].decode('utf-8') == "mailbox-" + str(self.id) + "-" + str(my_id):
                mailbox.append(message['data'])
            elif message['channel'].decode('utf-8') ==  "mailbox-id-" + str(self.id) + "-" + str(my_id):
                mailbox_id.append(int(message['data']))
            
            if len(mailbox) > 0 and len(mailbox_id) > 0:
                first = mailbox.popleft()
                stream_id = mailbox_id.popleft()
                if len(first) < 10 and first.decode("utf-8") == "done":
                    self.input_streams.pop(self.physical_to_logical_stream_mapping[stream_id])
                    print("done", self.physical_to_logical_stream_mapping[stream_id])
                else:
                    batch = context.deserialize(first)
                    results = self.functionObjects[my_id].execute(batch, self.physical_to_logical_stream_mapping[stream_id])
                    if results is not None:
                        self.output_stream.push(results)
                    else:
                        pass
            
            

class InputCSVNode(TaskNode):

    def set_name(self,bucket, key, names):
        self.bucket = bucket
        self.key = key
        self.names = names

    def initialize(self):
        if self.bucket is None:
            raise Exception
        self.input_csv_datasets = [InputCSVDataset(self.bucket, self.key, self.names,0) for i in range(self.parallelism)]
        for dataset in self.input_csv_datasets:
            dataset.set_num_mappers(self.parallelism)
    
    def execute(self, id):
        input_generator = self.input_csv_datasets[id].get_next_batch(id)
        for batch in input_generator:
            self.output_stream.push(batch)
        print("i'm done here")
        self.output_stream.done()




task_graph = TaskGraph()

quotes = task_graph.new_input_csv("yugan","a.csv",["key","avalue1", "avalue2"],1)
trades = task_graph.new_input_csv("yugan","b.csv",["key","avalue1", "avalue2"],1)
join_executor = JoinExecutor()
output_stream = task_graph.new_stateless_node({0:quotes,1:trades},join_executor,1)
task_graph.initialize()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)
#import pdb;pdb.set_trace()
