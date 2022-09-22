import pandas as pd
import ray
from collections import deque
from pyquokka.dataset import RedisObjectsDataset
import pickle
import redis
from threading import Lock
import time
import gc
import sys
import polars
import pyarrow as pa
import types
import concurrent.futures
import pyarrow.flight
# isolated simplified test bench for different fault tolerance protocols

VERBOSE = False

# above this limit we are going to start flushing things to disk
INPUT_MAILBOX_SIZE_LIMIT = 1024 * 1024 * 1024 * 2 # you can have 2GB in your input mailbox

def convert_to_format(batch, format):
    format = pickle.loads(format.to_pybytes())
    if format == "polars":
        return polars.from_arrow(pa.Table.from_batches([batch]))
    elif format == "pandas":
        return batch.to_pandas()
    elif format == "custom":
        return pickle.loads(batch.to_pandas()['object'][0])
    else:
        raise Exception("don't understand this format", format)

def convert_from_format(payload):
    if type(payload) == pd.core.frame.DataFrame:
        batch = pa.RecordBatch.from_pandas(payload)
        my_format = "pandas"
    elif type(payload) == polars.internals.frame.DataFrame:
        batches = payload.to_arrow().to_batches()
        assert len(batches) == 1
        batch = batches[0]
        my_format = "polars"
    else:
        batch = pa.RecordBatch.from_pydict({"object":[pickle.dumps(payload)]})
        my_format = "custom"
    return batch, my_format

# making a new class of things so we can easily type match when we start processing the input batch
class FlushedMessage:
    def __init__(self, loc) -> None:
        self.loc = loc

class SharedMemMessage:
    def __init__(self, form, name):
        self.format = form
        self.name = name

class Node:

    # will be overridden
    def __init__(self, id, channel) -> None:

        self.ip = ray.util.get_node_ip_address() 
        self.id = id
        self.channel = channel

        self.targets = {}
        self.r = redis.Redis(host='localhost', port=6800, db=0)
        self.head_r = redis.Redis(host=ray.worker._global_node.address.split(":")[0], port=6800, db=0)
        #self.plasma_client = plasma.connect("/tmp/plasma")

        self.target_rs = {}
        self.target_ps = {}

        self.flight_clients = {}
        self.flight_client = pyarrow.flight.connect("grpc://0.0.0.0:5005")

        # track the targets that are still alive
        self.alive_targets = {}
        self.output_lock = Lock()

        self.out_seq = 0

    def initialize(self):
        raise NotImplementedError

    def append_to_targets(self,tup):
        node_id, channel_to_ip, target_info = tup

        unique_ips = set(channel_to_ip.values())
        redis_clients = {i: redis.Redis(host=i, port=6800, db=0) if i != self.ip else redis.Redis(host='localhost', port = 6800, db=0) for i in unique_ips}
        self.targets[node_id] = (channel_to_ip, target_info)
        self.target_rs[node_id] = {}
        self.target_ps[node_id] = {}
        
        self.flight_clients[node_id] = {}

        flight_clients = {i: pyarrow.flight.connect("grpc://" + str(i) + ":5005") if i != self.ip else pyarrow.flight.connect("grpc://0.0.0.0:5005") for i in unique_ips}
        for channel in channel_to_ip:
            self.flight_clients[node_id][channel] = flight_clients[channel_to_ip[channel]]

        for channel in channel_to_ip:
            self.target_rs[node_id][channel] = redis_clients[channel_to_ip[channel]]
        
        for client in redis_clients:
            pubsub = redis_clients[client].pubsub(ignore_subscribe_messages = True)
            pubsub.subscribe("node-done-"+str(node_id))
            self.target_ps[node_id][channel] = pubsub
        
        self.alive_targets[node_id] = {i for i in channel_to_ip}
        # remember the self.strikes stuff? Now we cannot check for that because a downstream target could just die.
        # it's ok if we send stuff to dead people. Fault tolerance is supposed to take care of this.
        
    def update_targets(self):

        for target_node in self.target_ps:
            # there are #-ip locations you need to poll here.
            for channel in self.target_ps[target_node]:
                client = self.target_ps[target_node][channel]
                while True:
                    message = client.get_message()
                    
                    if message is not None:
                        if VERBOSE:
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
            
        self.out_seq += 1

        if type(data) == pa.lib.Table:
            data = polars.from_arrow(data)

        # downstream targets are done. You should be done too then.
        try:    
            if not self.update_targets():
                return False
        except:
            if VERBOSE:
                print("downstream failure detected")

        for target in self.alive_targets:
            original_channel_to_ip, target_info = self.targets[target]

            # target.partitioner will be a FunctionPartitioner with a function that takes in three arguments
            # the data (arbitrary python object), self.channel, channel of this node and number of downstream channels
            # it must return a dictionary of downstream channel number -> arbitrary python object
            # this partition key could be user specified or be supplied by Quokka from default strategies

            assert target_info.lowered

            data = target_info.predicate(data)
            data = data[target_info.projection]

            partitioned_payload = target_info.partitioner.func(data, self.channel, len(original_channel_to_ip))
            assert type(partitioned_payload) == dict and max(partitioned_payload.keys()) < len(original_channel_to_ip)
            
            for channel in self.alive_targets[target]:
                if channel not in partitioned_payload:
                    payload = None
                else:
                    payload = partitioned_payload[channel]
                    for func in target_info.batch_funcs:
                        if len(payload) == 0:
                            payload = None
                            break
                        payload = func(payload)

                client = self.flight_clients[target][channel]
                batch, my_format = convert_from_format(payload)
                
                # format (target id, target channel, source id, source channel, tag, format)
                #print("pushing to target",target,"channel",channel,"from source",self.id,"channel",self.channel,"tag",self.out_seq)
                upload_descriptor = pyarrow.flight.FlightDescriptor.for_command(pickle.dumps((target, channel, self.id, self.channel, self.out_seq, my_format)))

                while True:
                    buf = pyarrow.allocate_buffer(0)
                    action = pyarrow.flight.Action("check_puttable", buf)
                    result = next(client.do_action(action))
                    #print(result.body.to_pybytes().decode("utf-8"))
                    if result.body.to_pybytes().decode("utf-8") != "True":
                        time.sleep(1)
                    else:
                        break

                writer, _ = client.do_put(upload_descriptor, batch.schema)
                writer.write_batch(batch)
                writer.close()
       
        return True

    def done(self):

        self.out_seq += 1
        if VERBOSE:
            print("IM DONE", self.id)

        try:    
            if not self.update_targets():
                if VERBOSE:
                    print("WIERD STUFF IS HAPPENING")
                return False
        except:
            print("downstream failure detected")

        for target in self.alive_targets:
            for channel in self.alive_targets[target]:
                if VERBOSE:
                    print("SAYING IM DONE TO", target, channel, "MY OUT SEQ", self.out_seq)

                client = self.flight_clients[target][channel]
                #print("saying done to target",target,"channel",channel,"from source",self.id,"channel",self.channel,"tag",self.out_seq)
                payload = pickle.dumps((target, channel, self.id, self.channel, self.out_seq, "done"))
                upload_descriptor = pyarrow.flight.FlightDescriptor.for_command(payload)
                writer, _ = client.do_put(upload_descriptor, pa.schema([]))
                writer.close()
                
        return True

class InputNode(Node):
    def __init__(self, id, channel, batch_func = None) -> None:

        super().__init__( id, channel) 

        if VERBOSE:
            print("INPUT ACTOR LAUNCH", self.id)

        self.batch_func = batch_func
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    def initialize(self):
        pass
        
    def execute(self):

        futs = deque()
        futs.append(self.executor.submit(next, self.input_generator))
        while True:
            try:
                pos, batch = futs.popleft().result()
            except StopIteration:
                break
            futs.append(self.executor.submit(next, self.input_generator))
            if self.batch_func is not None:
                result = self.batch_func(batch)
                self.push(result)
            else:
                self.push(batch)
        
        if VERBOSE:
            print("INPUT DONE", self.id, self.channel)
        self.done()

@ray.remote
class InputReaderNode(InputNode):
    def __init__(self, id, channel, accessor, num_channels, batch_func=None) -> None:
        super().__init__(id, channel, batch_func)
        self.accessor = accessor
        self.accessor.set_num_channels(num_channels)
        self.input_generator = self.accessor.get_next_batch(channel, 0)

@ray.remote
class InputRedisDatasetNode(InputNode):
    def __init__(self, id, channel,channel_objects, batch_func=None):
        super().__init__(id, channel, batch_func = batch_func)
        ip_set = set()
        for da in channel_objects:
            for object in channel_objects[da]:
                ip_set.add(object[0])
        self.accessor = RedisObjectsDataset(channel_objects, ip_set)
        self.input_generator = self.accessor.get_next_batch(channel, 0)


class TaskNode(Node):
    def __init__(self, id, channel,  mapping, functionObject, parents) -> None:

        # id: int. Id of the node
        # channel: int. Channel of the node
        # streams: dictionary of logical_id : streams
        # mapping: the mapping between the name you assigned the stream to the actual id of the string.

        super().__init__(id, channel)

        self.id = id 
        self.parents = parents # dict of id -> dict of channel -> actor handles        
        self.functionObject = functionObject
        #assert hasattr(functionObject, "num_states") # for recovery
        self.physical_to_logical_mapping = mapping


    def initialize(self):
        message = pyarrow.py_buffer(pickle.dumps((self.id,self.channel, {i:list(self.parents[i].keys()) for i in self.parents})))
        action = pyarrow.flight.Action("register_channel",message)
        self.flight_client.do_action(action)

    def get_batches(self):

        message = pyarrow.py_buffer(pickle.dumps((self.id, self.channel)))
        action = pyarrow.flight.Action("get_batches_info", message)
        result = next(self.flight_client.do_action(action))
        batch_info, should_terminate = pickle.loads(result.body.to_pybytes())
        assert type(batch_info) == dict

        return batch_info, should_terminate

    def schedule_for_execution(self, batch_info):

        parent, channel = max(batch_info, key=batch_info.get)
        length = batch_info[(parent,channel)]
        if length == 0:
            return None, None

        # now drain that source
        requests = {}
        for daparent, channel in batch_info.keys():
            if daparent != parent:
                continue
            requests[daparent, channel] = batch_info[daparent, channel]
        
        return parent, requests

@ray.remote
class NonBlockingTaskNode(TaskNode):
    def __init__(self, id, channel,  mapping, functionObject, parents) -> None:
        super().__init__(id, channel,  mapping, functionObject, parents)
        print("I'm initialized")
    
    def execute(self):

        while True:

            batch_info, should_terminate = self.get_batches()
            if should_terminate:
                break
            # deque messages from the mailbox in a way that makes sense
            
            stream_id, requests = self.schedule_for_execution(batch_info)
            if stream_id is None:
                continue

            request = ((self.id, self.channel), requests)
            reader = self.flight_client.do_get(pyarrow.flight.Ticket(pickle.dumps(request)))

            batches = []
            while True:
                try:
                    chunk, metadata = reader.read_chunk()
                    batches.append(convert_to_format(chunk, metadata))
                except StopIteration:
                    break

            batches = [i for i in batches if i is not None]
            results = self.functionObject.execute( batches, self.physical_to_logical_mapping[stream_id], self.channel)
            
            # this is a very subtle point. You will only breakout if length of self.target, i.e. the original length of 
            # target list is bigger than 0. So you had somebody to send to but now you don't
            if type(results) == types.GeneratorType:
                for result in results:
                    break_out = False
                    if result is not None and len(self.targets) > 0:
                        if self.push(result) is False:
                            break_out = True
                            break
                    else:
                        pass
            else:
                break_out = False
                if results is not None and len(self.targets) > 0:
                    if self.push(results) is False:
                        break_out = True
                        break
                else:
                    pass
        
            if break_out:
                break
                    
        obj_done =  self.functionObject.done(self.channel) 

        if type(obj_done) == types.GeneratorType:
            for object in obj_done:
                self.push(object)
            del self.functionObject
            gc.collect()
        else:
            del self.functionObject
            gc.collect()
            if obj_done is not None:
                self.push(obj_done)
        if VERBOSE:
            print("TASK NODE DONE", self.id, self.channel)
        self.done()
    
@ray.remote
class BlockingTaskNode(TaskNode):
    def __init__(self, id, channel,  mapping, output_dataset, functionObject, parents) -> None:
        super().__init__(id, channel,  mapping, functionObject, parents)
        self.output_dataset = output_dataset
        self.object_count = 0 
    # explicit override with error. Makes no sense to append to targets for a blocking node. Need to use the dataset instead.
    def append_to_targets(self,tup):
        raise Exception("Trying to stream from a blocking node")
    
    def execute(self):

        add_tasks = []

        while True:
            batch_info, should_terminate = self.get_batches()
            #print(batch_info, should_terminate)
            if should_terminate:
                break
            # deque messages from the mailbox in a way that makes sense
            
            stream_id, requests = self.schedule_for_execution(batch_info)
            if stream_id is None:
                continue
            request = ((self.id, self.channel), requests)

            reader = self.flight_client.do_get(pyarrow.flight.Ticket(pickle.dumps(request)))

            batches = []
            while True:
                try:
                    chunk, metadata = reader.read_chunk()
                    batches.append(convert_to_format(chunk, metadata))
                except StopIteration:
                    break
            
            
            batches = [i for i in batches if i is not None]
            results = self.functionObject.execute( batches,self.physical_to_logical_mapping[stream_id], self.channel)
            
            if results is not None and len(results) > 0:
                cursor = 0
                stride = 1000000
                while cursor < len(results):
                    key = str(self.id) + "-" + str(self.channel) + "-" + str(self.object_count)
                    self.object_count += 1
                    try:
                        self.r.set(key, pickle.dumps(results[cursor : cursor + stride]))
                    except:
                        print(results)
                        raise Exception
                    # we really should be doing sys.getsizeof(result), but that doesn't work for polars dfs
                    add_tasks.append(self.output_dataset.added_object.remote(self.channel, (ray.util.get_node_ip_address(), key, stride)))
                    cursor += stride
            else:
                pass
        
        obj_done =  self.functionObject.done(self.channel) 

        if type(obj_done) == types.GeneratorType:
            for object in obj_done:
                if object is not None:
                    key = str(self.id) + "-" + str(self.channel) + "-" + str(self.object_count)
                    self.object_count += 1
                    self.r.set(key, pickle.dumps(object))
                    if hasattr(object, "__len__"):
                        add_tasks.append(self.output_dataset.added_object.remote(self.channel, (ray.util.get_node_ip_address(), key, len(object))))
                    else:
                        add_tasks.append(self.output_dataset.added_object.remote(self.channel, (ray.util.get_node_ip_address(), key,sys.getsizeof(object))))
            del self.functionObject
            gc.collect()
        else:
            del self.functionObject
            gc.collect()
            
            if obj_done is not None:
                key = str(self.id) + "-" + str(self.channel) + "-" + str(self.object_count)
                self.object_count += 1
                self.r.set(key, pickle.dumps(obj_done))
                if hasattr(obj_done, "__len__"):
                    add_tasks.append(self.output_dataset.added_object.remote(self.channel, (ray.util.get_node_ip_address(), key, len(obj_done))))
                else:
                    add_tasks.append(self.output_dataset.added_object.remote(self.channel, (ray.util.get_node_ip_address(), key, sys.getsizeof(obj_done))))
     
        ray.get(add_tasks)
        ray.get(self.output_dataset.done_channel.remote(self.channel))
        
        self.done()
