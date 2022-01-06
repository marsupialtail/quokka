from collections import deque
from dataset import * 
import redis
import pyarrow as pa
import pandas as pd
from multiprocessing import Process, Value
import time
import ray
ray.init(ignore_reinit_error=True)

'''

In this system, there are two things. The first are task nodes and the second are streams. They can be defined more or less
indepednently from one another. The Task

'''

'''
Since each stream can have only one input, we are just going to use the source node id as the stream id. All ids are thus unique. 
'''

context = pa.default_serialization_context()

@ray.remote
class Stream:

    def __init__(self, source_node_id, source_parallelism) -> None:
        
        self.source = source_node_id
        self.source_parallelism = source_parallelism
        self.done_sources = Value('i',0)
        self.targets = []
        self.r = redis.Redis(host='localhost', port=6379, db=0)

        pass    

    def get_source(self):
        return self.source

    def append_to_targets(self,tup):
        self.targets.append(tup)

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
                        print(results)
                        if False in results:
                            raise Exception
    def done(self):
        with self.done_sources.get_lock():
            self.done_sources.value += 1
        if self.done_sources.value == self.source_parallelism:
            for target, parallelism in self.targets:
                for channel in range(parallelism):
                    pipeline = self.r.pipeline()
                    pipeline.publish("mailbox-"+str(target) + "-" + str(channel),"done")
                    pipeline.publish("mailbox-id-"+str(target) + "-" + str(channel),self.source)
                    results = pipeline.execute()
                    if False in results:
                        raise Exception

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

    # stream is going to be a Ray future
    def set_output_stream(self,stream):
        self.output_stream = stream

    def set_mapping(self, mapping):
        self.physical_to_logical_stream_mapping = mapping

    def get_parallelism(self):
        return self.parallelism

    def initialize(self):
        pass

    def execute(self):
        pass

@ray.remote
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
                        ray.get(self.output_stream.push.remote(results))
                    else:
                        pass
        
        ray.get(self.output_stream.done.remote())
            
            
@ray.remote
class InputCSVNode(TaskNode):

    def __init__(self,bucket, key, names, parallelism):
        self.bucket = bucket
        self.key = key
        self.names = names
        self.parallelism = parallelism

    def initialize(self):
        if self.bucket is None:
            raise Exception
        self.input_csv_datasets = [InputCSVDataset(self.bucket, self.key, self.names,0) for i in range(self.parallelism)]
        for dataset in self.input_csv_datasets:
            dataset.set_num_mappers(self.parallelism)
    
    def execute(self, id):
        input_generator = self.input_csv_datasets[id].get_next_batch(id)
        for batch in input_generator:
            ray.get(self.output_stream.push.remote(batch))
        ray.get(self.output_stream.done.remote())


class TaskGraph:
    # this keeps the logical dependency DAG between tasks 
    def __init__(self) -> None:
        self.current_node = 0
        self.nodes = {}
    
    def new_input_csv(self, bucket, key, names, parallelism):
        tasknode = InputCSVNode.remote(bucket,key,names, parallelism)
        output_stream = Stream.remote(self.current_node, parallelism)
        ray.get(tasknode.set_output_stream.remote(output_stream))
        self.nodes[self.current_node] = tasknode
        self.current_node += 1
        return output_stream
    
    def new_stateless_node(self, streams, functionObject, parallelism):
        mapping = {}
        for key in streams:
            stream = streams[key]
            source = ray.get(stream.get_source.remote())
            if source not in self.nodes:
                raise Exception("stream source not registered")
            ray.get(stream.append_to_targets.remote((self.current_node,parallelism)))
            mapping[source] = key
        tasknode = StatelessTaskNode.remote(streams, functionObject, self.current_node, parallelism)
        ray.get(tasknode.set_mapping.remote(mapping))
        output_stream = Stream.remote(self.current_node, parallelism)
        ray.get(tasknode.set_output_stream.remote(output_stream))
        self.nodes[self.current_node] = tasknode
        self.current_node += 1
        return output_stream
    
    def new_output_csv(self):

        # this does not return anything
        return 
    
    def initialize(self):
        ray.get([self.nodes[node].initialize.remote() for node in self.nodes])
    
    def run(self):
        processes = []
        for key in self.nodes:
            node = self.nodes[key]
            for replica in range(ray.get(node.get_parallelism.remote())):
                processes.append(node.execute.remote(replica))
        ray.get(processes)



