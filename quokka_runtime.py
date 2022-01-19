from collections import deque
from dataset import * 
from sql import JoinExecutor, OutputCSVExecutor
import redis
import pyarrow as pa
import pandas as pd
#from multiprocessing import Process, Value
import time
import ray
import os
import pickle
#parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#os.environ["PYTHONPATH"] = parent_dir + ":" + os.environ.get("PYTHONPATH", "")
ray.init(ignore_reinit_error=True) # do this locally
#ray.init("auto", ignore_reinit_error=True, runtime_env={"working_dir":"/home/ziheng/quokka-dev"})
#ray.timeline("profile.json")

'''

In this system, there are two things. The first are task nodes and the second are streams. They can be defined more or less
indepednently from one another. The Task

'''

'''
Since each stream can have only one input, we are just going to use the source node id as the stream id. All ids are thus unique. 
'''

# TO DO -- address DeprecationWarning
#context = pa.default_serialization_context()
    

class TaskNode:

    def __init__(self, streams, functionObject, id, parallelism, mapping, source_parallelism, ip) -> None:
        self.functionObject = functionObject
        self.input_streams = streams
        self.id = id
        self.parallelism = parallelism
        self.targets = {}
        # this maps what the system's stream_id is to what the user called the stream when they created the task node
        self.physical_to_logical_stream_mapping = mapping
        self.source_parallelism = source_parallelism
        self.r = redis.Redis(host='localhost', port=6800, db=0)
        self.target_rs = {}
        self.target_ps = {}

        # track the targets that are still alive
        self.alive_targets = {}

        # you are only allowed to send a message to a done target once. More than once is unforgivable.
        self.strikes = set()

        pass

    def append_to_targets(self,tup):
        node_id, parallelism, ip, partition_key = tup
        self.targets[node_id] = (parallelism, partition_key)
        self.target_rs[node_id] = redis.Redis(host=ip, port=6800, db=0)
        self.target_ps[node_id] = self.target_rs[node_id].pubsub(ignore_subscribe_messages = True)
        self.target_ps[node_id].subscribe("node-done-"+str(node_id))
        self.alive_targets[node_id] = {i for i in range(parallelism)}
        for i in range(parallelism):
            self.strikes.add((node_id, i))

    def initialize(self):
        pass

    def execute(self):
        pass

    # determines if there are still targets alive, returns True or False. 
    def update_targets(self):

        for target_node in self.target_ps:
            while True:
                message = self.target_ps[target_node].get_message()
                
                if message is not None:
                    print(message['data'])
                    self.alive_targets[target_node].remove(int(message['data']))
                    if len(self.alive_targets[target_node]) == 0:
                        self.alive_targets.pop(target_node)
                else:
                    break 
        if len(self.alive_targets) > 0:
            return True
        else:
            return False

    def push(self, data):

        print("stream psuh start",time.time())

        if len(self.targets) == 0:
            raise Exception
        
        else:
            # iterate over downstream task nodes, each of which may contain inner parallelism
            # distribution strategy depends on data type as well as partition function
            if type(data) == pd.core.frame.DataFrame:
                for target in self.alive_targets:
                    original_parallelism, partition_key = self.targets[target]
                    for channel in self.alive_targets[target]:
                        if partition_key is not None:
                            payload = data[data[partition_key] % original_parallelism == channel]
                        else:
                            payload = data
                        # don't worry about target being full for now.
                        print("not checking if target is full. This will break with larger joins for sure.")
                        pipeline = self.target_rs[target].pipeline()
                        #pipeline.publish("mailbox-"+str(target) + "-" + str(channel),context.serialize(payload).to_buffer().to_pybytes())
                        pipeline.publish("mailbox-"+str(target) + "-" + str(channel),pickle.dumps(payload))

                        pipeline.publish("mailbox-id-"+str(target) + "-" + str(channel),self.id)
                        results = pipeline.execute()
                        if False in results:
                            if (target, channel) not in self.strikes:
                                raise Exception
                            self.strikes.remove((target, channel))
        
        print("stream psuh end",time.time())

    def done(self):
        for target in self.alive_targets:
            for channel in self.alive_targets[target]:
                pipeline = self.target_rs[target].pipeline()
                pipeline.publish("mailbox-"+str(target) + "-" + str(channel),"done")
                pipeline.publish("mailbox-id-"+str(target) + "-" + str(channel),self.id)
                results = pipeline.execute()
                if False in results:
                    if (target, channel) not in self.strikes:
                        raise Exception
                    self.strikes.remove((target, channel))
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
                    batch = pickle.loads(first)
                    results = self.functionObject.execute(batch, self.physical_to_logical_stream_mapping[stream_id], my_id)
                    if hasattr(self.functionObject, 'early_termination') and self.functionObject.early_termination: 
                        break
                    if results is not None and len(self.targets) > 0:
                        break_out = False                        
                        for result in results:
                            if self.update_targets() is False:
                                break_out = True
                                break
                            self.push(result)
                        if break_out:
                            break
                    else:
                        pass
        obj_done =  self.functionObject.done(my_id) 
        if self.update_targets() and obj_done is not None:
            self.push(obj_done)
        self.update_targets()
        self.done()
        self.r.publish("node-done-"+str(self.id),str(my_id))
        print("task end",time.time())
    
            
@ray.remote
class InputCSVNode(TaskNode):

    def __init__(self,id, bucket, key, names, parallelism, batch_func=None, sep = ",", dependent_map = {}):
        self.id = id
        self.bucket = bucket
        self.key = key
        self.names = names
        self.parallelism = parallelism
        self.targets= {}
        self.target_rs = {}
        self.target_ps = {}
        self.alive_targets = {}
        self.strikes = set()

        self.batch_func = batch_func
        self.sep = sep
        self.r = redis.Redis(host='localhost', port=6800, db=0)
        self.dependent_rs = {}
        self.dependent_parallelism = {}
        for key in dependent_map:
            self.dependent_parallelism[key] = dependent_map[key][1]
            r = redis.Redis(host=dependent_map[key][0], port=6800, db=0)
            p = r.pubsub(ignore_subscribe_messages=True)
            p.subscribe("input-done-" + str(key))
            self.dependent_rs[key] = p
        

    def initialize(self):
        if self.bucket is None:
            raise Exception
        self.input_csv_dataset = InputCSVDataset(self.bucket, self.key, self.names,0, sep = self.sep) 
        self.input_csv_dataset.set_num_mappers(self.parallelism)
    
    def execute(self, id):

        undone_dependencies = len(self.dependent_rs)
        while undone_dependencies > 0:
            time.sleep(0.01) # be nice
            for dependent_node in self.dependent_rs:
                message = self.dependent_rs[dependent_node].get_message()
                if message is not None:
                    if message['data'].decode("utf-8") == "done":
                        self.dependent_parallelism[dependent_node] -= 1
                        if self.dependent_parallelism[dependent_node] == 0:
                            undone_dependencies -= 1
                    else:
                        raise Exception(message['data'])

        print("input_csv start",time.time())
        input_generator = self.input_csv_dataset.get_next_batch(id)

        for batch in input_generator:
            if self.update_targets() is False:
                break
            if self.batch_func is not None:
                self.push(self.batch_func(batch))
            else:
                self.push(batch)
        self.update_targets()
        self.done()
        self.r.publish("input-done-" + str(self.id), "done")
        print("input_csv end",time.time())


class TaskGraph:
    # this keeps the logical dependency DAG between tasks 
    def __init__(self) -> None:
        self.current_node = 0
        self.nodes = {}
        self.node_parallelism = {}
        self.node_ips = {}
    
    def new_input_csv(self, bucket, key, names, parallelism, ip='localhost',batch_func=None, sep = ",", dependents = []):
        
        dependent_map = {}
        if len(dependents) > 0:
            for node in dependents:
                dependent_map[node] = (self.node_ips[node], self.node_parallelism[node])
        
        if ip != 'localhost':
            tasknode = [InputCSVNode.options(num_cpus=0.01, resources={"node:" + ip : 0.01}).
            remote(self.current_node, bucket,key,names, parallelism, batch_func = batch_func,sep = sep, dependent_map = dependent_map) for i in range(parallelism)]
        else:
            tasknode = [InputCSVNode.options(num_cpus=0.01,resources={"node:" + ray.worker._global_node.address.split(":")[0] : 0.01}).
            remote(self.current_node, bucket,key,names, parallelism, batch_func = batch_func, sep = sep, dependent_map = dependent_map) for i in range(parallelism)]
        self.nodes[self.current_node] = tasknode
        self.node_parallelism[self.current_node] = parallelism
        self.node_ips[self.current_node] = ip
        self.current_node += 1
        return self.current_node - 1
    
    def new_stateless_node(self, streams, functionObject, parallelism, partition_key, ip='localhost'):
        mapping = {}
        source_parallelism = {}
        for key in streams:
            source = streams[key]
            if source not in self.nodes:
                raise Exception("stream source not registered")
            import sys
            print(sys.path)
            ray.get([i.append_to_targets.remote((self.current_node, parallelism, ip, partition_key[key])) for i in self.nodes[source]])
            mapping[source] = key
            source_parallelism[source] = self.node_parallelism[source]
        if ip != 'localhost':
            tasknode = [StatelessTaskNode.options(resources={"node:" + ip : 0.01}).remote(streams, functionObject, self.current_node, parallelism, mapping, source_parallelism, ip) for i in range(parallelism)]
        else:
            tasknode = [StatelessTaskNode.options(resources={"node:" + ray.worker._global_node.address.split(":")[0]: 0.01}).remote(streams, functionObject, self.current_node, parallelism, mapping, source_parallelism, ip) for i in range(parallelism)]
        self.nodes[self.current_node] = tasknode
        self.node_parallelism[self.current_node] = parallelism
        self.node_ips[self.current_node] = ip
        self.current_node += 1
        return self.current_node - 1

    
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
'''
task_graph = TaskGraph()

quotes = task_graph.new_input_csv("yugan","a-big.csv",["key"] + ["avalue" + str(i) for i in range(100)],2)
trades = task_graph.new_input_csv("yugan","b-big.csv",["key"] + ["bvalue" + str(i) for i in range(100)],2)
join_executor = JoinExecutor("key")
output_stream = task_graph.new_stateless_node({0:quotes,1:trades},join_executor,4)
#output_executor = OutputCSVExecutor(4,"yugan","result")
#wrote = task_graph.new_stateless_node({0:output_stream},output_executor,4)

task_graph.initialize()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)
'''
