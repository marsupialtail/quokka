from collections import deque
from dataset import * 
import redis
import pyarrow as pa
import pandas as pd
#from multiprocessing import Process, Value
import time
import ray
ray.init(ignore_reinit_error=True)
#ray.timeline("profile.json")

'''

In this system, there are two things. The first are task nodes and the second are streams. They can be defined more or less
indepednently from one another. The Task

'''

'''
Since each stream can have only one input, we are just going to use the source node id as the stream id. All ids are thus unique. 
'''

# TO DO -- address DeprecationWarning
context = pa.default_serialization_context()
    

class TaskNode:

    def __init__(self, streams, functionObject, id, parallelism, mapping, source_parallelism) -> None:
        self.functionObjects = [functionObject for i in range(parallelism)]
        self.input_streams = streams
        self.id = id
        self.parallelism = parallelism
        self.targets = []
        # this maps what the system's stream_id is to what the user called the stream when they created the task node
        self.physical_to_logical_stream_mapping = mapping
        self.source_parallelism = source_parallelism
        self.r = redis.Redis(host='localhost', port=6800, db=0)
        pass

    def append_to_targets(self,tup):
        self.targets.append(tup)

    def initialize(self):
        pass

    def execute(self):
        pass


    def push(self, data, columns=None):

        print("stream psuh start",time.time())

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
                        pipeline.publish("mailbox-id-"+str(target) + "-" + str(channel),self.id)
                        results = pipeline.execute()
                        if False in results:
                            raise Exception(results)
        
        print("stream psuh end",time.time())

    def done(self):
        for target, parallelism in self.targets:
            for channel in range(parallelism):
                pipeline = self.r.pipeline()
                pipeline.publish("mailbox-"+str(target) + "-" + str(channel),"done")
                pipeline.publish("mailbox-id-"+str(target) + "-" + str(channel),self.id)
                results = pipeline.execute()
                if False in results:
                    raise Exception

@ray.remote
class StatelessTaskNode(TaskNode):

    # this is for one of the parallel threads

    def initialize(self):
        pass

    def execute(self, my_id):

        # this needs to change
        print("task start",time.time())

        p = self.r.pubsub(ignore_subscribe_messages=True)
        p.subscribe("mailbox-" + str(self.id) + "-" + str(my_id), "mailbox-id-" + str(self.id) + "-" + str(my_id))

        assert my_id < self.parallelism

        mailbox = deque()
        mailbox_id = deque()

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

                    # the responsibility for checking how many executors this input stream has is now resting on the consumer.

                    self.source_parallelism[stream_id] -= 1
                    if self.source_parallelism[stream_id] == 0:
                        self.input_streams.pop(self.physical_to_logical_stream_mapping[stream_id])
                        print("done", self.physical_to_logical_stream_mapping[stream_id])
                else:
                    batch = context.deserialize(first)
                    results = self.functionObjects[my_id].execute(batch, self.physical_to_logical_stream_mapping[stream_id])
                    if results is not None:
                        self.push(results)
                    else:
                        pass
        
        self.done()
        print("task end",time.time())
    
            
@ray.remote
class InputCSVNode(TaskNode):

    def __init__(self,id, bucket, key, names, parallelism):
        self.id = id
        self.bucket = bucket
        self.key = key
        self.names = names
        self.parallelism = parallelism
        self.targets= []
        self.r = redis.Redis(host='localhost', port=6800, db=0)


    def initialize(self):
        if self.bucket is None:
            raise Exception
        self.input_csv_datasets = [InputCSVDataset(self.bucket, self.key, self.names,0) for i in range(self.parallelism)]
        for dataset in self.input_csv_datasets:
            dataset.set_num_mappers(self.parallelism)
    
    def execute(self, id):
        print("input_csv start",time.time())
        input_generator = self.input_csv_datasets[id].get_next_batch(id)
        for batch in input_generator:
            self.push(batch)
        self.done()
        print("input_csv end",time.time())


class TaskGraph:
    # this keeps the logical dependency DAG between tasks 
    def __init__(self) -> None:
        self.current_node = 0
        self.nodes = {}
        self.node_parallelism = {}
    
    def new_input_csv(self, bucket, key, names, parallelism):
        tasknode = [InputCSVNode.remote(self.current_node, bucket,key,names, parallelism) for i in range(parallelism)]
        self.nodes[self.current_node] = tasknode
        self.node_parallelism[self.current_node] = parallelism
        self.current_node += 1
        return self.current_node - 1
    
    def new_stateless_node(self, streams, functionObject, parallelism):
        mapping = {}
        source_parallelism = {}
        for key in streams:
            source = streams[key]
            if source not in self.nodes:
                raise Exception("stream source not registered")
            ray.get([i.append_to_targets.remote((self.current_node, parallelism)) for i in self.nodes[source]])
            mapping[source] = key
            source_parallelism[source] = self.node_parallelism[source]
        tasknode = [StatelessTaskNode.remote(streams, functionObject, self.current_node, parallelism, mapping, source_parallelism) for i in range(parallelism)]
        self.nodes[self.current_node] = tasknode
        self.node_parallelism[self.current_node] = parallelism
        self.current_node += 1
        return self.current_node - 1
    
    def new_output_csv(self):

        # this does not return anything
        return 
    
    def initialize(self):
        ray.get([node.initialize.remote() for node_id in self.nodes for node in self.nodes[node_id] ])
    
    def run(self):
        processes = []
        for key in self.nodes:
            node = self.nodes[key]
            for i in range(len(node)):
                replica = node[i]
                processes.append(replica.execute.remote(i))
        ray.get(processes)