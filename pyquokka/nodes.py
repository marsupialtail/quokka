from curses import meta
import numpy as np
import pandas as pd
import ray
from collections import deque, OrderedDict
from pyquokka.dataset import RedisObjectsDataset
import pickle
import os
import redis
from threading import Lock
import time
import boto3
import gc
import sys
import polars
import pyarrow as pa
import types
import concurrent.futures
import pyarrow.flight
# isolated simplified test bench for different fault tolerance protocols

#FT_I = True
#FT =  True
FT_I = True
FT = True
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
    def __init__(self, id, channel, checkpoint_location) -> None:

        self.ip = ray.util.get_node_ip_address() 
        self.id = id
        self.channel = channel
        self.checkpoint_location = checkpoint_location

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

    def initialize(self):
        raise NotImplementedError

    def append_to_targets(self,tup):
        node_id, channel_to_ip, partition_key = tup

        unique_ips = set(channel_to_ip.values())
        redis_clients = {i: redis.Redis(host=i, port=6800, db=0) if i != self.ip else redis.Redis(host='localhost', port = 6800, db=0) for i in unique_ips}
        self.targets[node_id] = (channel_to_ip, partition_key)
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
        
        self.target_output_state[node_id] = {channel:0 for channel in channel_to_ip}


    def update_target_ip_and_help_target_recover(self, target_id, channel, total_channels, target_out_seq_state, new_ip):

        if new_ip != self.targets[target_id][0][channel]: # shouldn't schedule to same IP address ..
            redis_client = redis.Redis(host=new_ip, port = 6800, db=0) 
            pubsub = redis_client.pubsub(ignore_subscribe_messages = True)
            pubsub.subscribe("node-done-"+str(target_id))
            self.target_rs[target_id][channel] = redis_client
            self.target_ps[target_id][channel] = pubsub

        # send logged outputs 
        print("HELP RECOVER",target_id,channel, target_out_seq_state)
        self.output_lock.acquire()
        pipeline = self.target_rs[target_id][channel].pipeline()
        for key in self.logged_outputs:
            if key > target_out_seq_state:
                if type(self.logged_outputs[key]) == str and self.logged_outputs[key] == "done":
                    payload = "done"
                else:
                    partition_key = self.targets[target_id][1]
                    result = partition_key(self.logged_outputs[key], self.channel, total_channels)
                    payload = result[channel] if channel in result else None
                pipeline.publish("mailbox-"+str(target_id) + "-" + str(channel),pickle.dumps(payload))
                pipeline.publish("mailbox-id-"+str(target_id) + "-" + str(channel),pickle.dumps((self.id, self.channel, key)))
        results = pipeline.execute()
        if False in results:
            raise Exception
        self.output_lock.release()   

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

    # reliably log state tag
    def log_state_tag(self):
        assert self.head_r.rpush("state-tag-" + str(self.id) + "-" + str(self.channel), pickle.dumps(self.state_tag))

    def push(self, data):
            
        self.out_seq += 1

        if type(data) == pa.lib.Table:
            data = polars.from_arrow(data)
        
        if FT:
            self.output_lock.acquire()
            self.logged_outputs[self.out_seq] = data
            self.output_lock.release()

        # downstream targets are done. You should be done too then.
        try:    
            if not self.update_targets():
                return False
        except:
            if VERBOSE:
                print("downstream failure detected")

        for target in self.alive_targets:
            original_channel_to_ip, partition_key = self.targets[target]
            # partition key will be a function that takes in three arguments
            # the data (arbitrary python object)
            # self.channel, channel of this node
            # number of downstream channels
            # it must return a dictionary of downstream channel number -> arbitrary python object
            # this partition key could be user specified or be supplied by Quokka from default strategies

            partitioned_payload = partition_key(data, self.channel, len(original_channel_to_ip))
            assert type(partitioned_payload) == dict and max(partitioned_payload.keys()) < len(original_channel_to_ip)
            
            for channel in self.alive_targets[target]:
                if channel not in partitioned_payload:
                    payload = None
                else:
                    payload = partitioned_payload[channel]


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

        self.output_lock.acquire()
        self.logged_outputs[self.out_seq] = "done"
        self.output_lock.release()

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
    def __init__(self, id, channel, checkpoint_location, batch_func = None, dependent_map = {}, checkpoint_interval = 10, ckpt = None) -> None:

        super().__init__( id, channel, checkpoint_location) 

        if VERBOSE:
            print("INPUT ACTOR LAUNCH", self.id)

        self.batch_func = batch_func
        self.dependent_rs = {}
        self.dependent_parallelism = {}
        self.checkpoint_interval = checkpoint_interval
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        self.logged_outputs = {} # we are not going to checkpoint those logged outputs, which could be massive

        for key in dependent_map:
            self.dependent_parallelism[key] = dependent_map[key][1]
            ps = []
            for ip in dependent_map[key][0]:
                r = redis.Redis(host=ip, port=6800, db=0)
                p = r.pubsub(ignore_subscribe_messages=True)
                p.subscribe("input-done-" + str(key))
                ps.append(p)
            self.dependent_rs[key] = ps

        if ckpt is None:
            
            self.target_output_state = {}
            self.out_seq = 0
            self.state_tag = 0
            self.latest_stable_state = None
            self.latest_stable_seq = 0
            self.seq_state_map = {}
        else:
            if ckpt == "s3":
                s3_resource = boto3.resource('s3')
                bucket, key = self.checkpoint_location
                try:
                    recovered_state = pickle.loads(s3_resource.Object(bucket, key).get()['Body'].read())
                except: # failed to load checkpoint!
                    self.target_output_state = {}
                    self.out_seq = 0
                    self.state_tag = 0
                    self.latest_stable_state = None
                    self.latest_stable_seq = 0
                    self.seq_state_map = {}
                    return
                    
            else:
                recovered_state = pickle.load(open(ckpt,"rb"))
            
            # what you do here don't matter, the target_output_state will be set to all zeros
            self.target_output_state = {}
            self.latest_stable_state = recovered_state["state"]
            self.out_seq = recovered_state["out_seq"]
            self.latest_stable_seq = self.out_seq
            self.seq_state_map = {} #recovered_state["seq_state_map"]
            self.state_tag =0# recovered_state["tag"]
            print("INPUT NODE RECOVERED TO STATE", self.latest_stable_state, self.out_seq, self.seq_state_map, self.state_tag)

    def initialize(self):
        pass

    def truncate_logged_outputs(self, target_id, channel, target_ckpt_state):
        if VERBOSE:
            print("STARTING TRUNCATE", target_id, channel, target_ckpt_state, self.target_output_state)
        old_min = min(self.target_output_state[target_id].values())
        self.target_output_state[target_id][channel] = target_ckpt_state
        new_min = min(self.target_output_state[target_id].values())

        self.output_lock.acquire()
        if new_min > old_min:

            self.latest_stable_state = self.seq_state_map[new_min]
            self.latest_stable_seq = new_min
            if VERBOSE:
                print("UPDATING LATEST ", self.latest_stable_state, self.latest_stable_seq, self.seq_state_map)

            for key in range(old_min, new_min):
                if key in self.logged_outputs:
                    if VERBOSE:
                        print("REMOVING KEY",key,"FROM LOGGED OUTPUTS")
                    self.logged_outputs.pop(key)
            gc.collect()
        self.output_lock.release() 
        
    def checkpoint(self, method = "s3"):
        if not FT_I:
            return
        # write logged outputs, state, state_tag to reliable storage
        # for input nodes, log the outputs instead of redownlaoding is probably worth it. since the outputs could be filtered by predicate
        
        state = { "out_seq" : self.latest_stable_seq, "tag":self.state_tag,
        "state":self.latest_stable_state, "seq_state_map": self.seq_state_map}
        state_str = pickle.dumps(state)

        if method == "s3":

            s3_resource = boto3.resource('s3')
            bucket, key = self.checkpoint_location
            # if this fails we are dead, but probability of this failing much smaller than dump failing
            # the lack of rename in S3 is a big problem
            s3_resource.Object(bucket,key).put(Body=state_str)
        
        elif method == "local":
            raise Exception("not supported anymore")
        if VERBOSE:
            print("INPUT NODE CHECKPOINTING")
        
        
    def execute(self):
        
        undone_dependencies = len(self.dependent_rs)
        while undone_dependencies > 0:
            time.sleep(0.001) # be nice
            for dependent_node in self.dependent_rs:
                messages = [i.get_message() for i in self.dependent_rs[dependent_node]]
                for message in messages:
                    if message is not None:
                        if message['data'].decode("utf-8") == "done":
                            self.dependent_parallelism[dependent_node] -= 1
                            if self.dependent_parallelism[dependent_node] == 0:
                                undone_dependencies -= 1
                        else:
                            raise Exception(message['data'])            

        # no need to log the state tag in an input node since we know the expected path...

        futs = deque()
        futs.append(self.executor.submit(next, self.input_generator))
        while True:
            try:
                pos, batch = futs.popleft().result()
            except StopIteration:
                break
            futs.append(self.executor.submit(next, self.input_generator))
            self.seq_state_map[self.out_seq + 1] = pos
            if self.batch_func is not None:
                result = self.batch_func(batch)
                self.push(result)
            else:
                self.push(batch)
            if self.state_tag % self.checkpoint_interval == 0:
                self.checkpoint()
            self.state_tag += 1
        
        if VERBOSE:
            print("INPUT DONE", self.id, self.channel)
        self.done()
        self.r.publish("input-done-" + str(self.id), "done")

@ray.remote
class InputReaderNode(InputNode):
    def __init__(self, id, channel, accessor, num_channels, checkpoint_location, batch_func=None, dependent_map={}, checkpoint_interval=10, ckpt=None) -> None:
        super().__init__(id, channel, checkpoint_location, batch_func, dependent_map, checkpoint_interval, ckpt)
        self.accessor = accessor
        self.accessor.set_num_channels(num_channels)
        self.input_generator = self.accessor.get_next_batch(channel, self.latest_stable_state)

@ray.remote
class InputRedisDatasetNode(InputNode):
    def __init__(self, id, channel,channel_objects, checkpoint_location,batch_func=None, dependent_map={}, ckpt = None):
        super().__init__(id, channel, checkpoint_location, batch_func = batch_func, dependent_map = dependent_map, ckpt = ckpt)
        ip_set = set()
        for da in channel_objects:
            for object in channel_objects[da]:
                ip_set.add(object[0])
        self.accessor = RedisObjectsDataset(channel_objects, ip_set)
        self.input_generator = self.accessor.get_next_batch(channel, self.latest_stable_state)


class TaskNode(Node):
    def __init__(self, id, channel,  mapping, functionObject, parents, checkpoint_location, checkpoint_interval = 10, ckpt = None) -> None:

        # id: int. Id of the node
        # channel: int. Channel of the node
        # streams: dictionary of logical_id : streams
        # mapping: the mapping between the name you assigned the stream to the actual id of the string.

        super().__init__(id, channel, checkpoint_location)

        self.id = id 
        self.parents = parents # dict of id -> dict of channel -> actor handles        
        self.functionObject = functionObject
        #assert hasattr(functionObject, "num_states") # for recovery
        self.physical_to_logical_mapping = mapping
        self.checkpoint_interval = checkpoint_interval

        if ckpt is None:
            self.state_tag =  {(parent,channel): 0 for parent in parents for channel in parents[parent]}
            self.logged_outputs = {}
            self.target_output_state = {}

            self.out_seq = 0
            self.expected_path = deque()

            self.ckpt_counter = -1
            self.ckpt_number = 0
            self.ckpt_files = {}

            # this is used for buffering input mailbox things on disk
            self.disk_fileno = 0

        else:
            if ckpt == "s3":
                s3_resource = boto3.resource('s3')
                bucket, key = self.checkpoint_location
                recovered_state = pickle.loads(s3_resource.Object(bucket, key).get()['Body'].read())
                self.ckpt_number = recovered_state["function_object"]
                self.ckpt_files = recovered_state["ckpt_files"]
                self.disk_fileno = recovered_state["disk_fileno"]
                object_states = []
                for bucket, key in self.ckpt_files:
                    z=  {}
                    for k in self.ckpt_files[bucket, key]:
                        if self.ckpt_files[bucket, key][k] == "polars":
                            z[k] = polars.from_arrow(pa.parquet.read_table("s3://" + bucket + "/" + key + "." + str(k) + ".parquet"))
                        elif self.ckpt_files[bucket, key][k] == "pandas":
                            z[k] = pd.read_parquet("s3://" + bucket + "/" + key + "." + str(k) + ".parquet")
                        elif self.ckpt_files[bucket, key][k] == "pickle":
                            z[k] = pickle.loads(s3_resource.Object(bucket, key + "." + str(k) + ".pkl").get()['Body'].read())
                        else:
                            raise Exception
                    object_states.append(z)

            else:
                raise Exception("not supported anymore")
                recovered_state = pickle.load(open(ckpt,"rb"))

            self.state_tag= recovered_state["tag"]
            print("RECOVERED TO STATE TAG", self.state_tag)
            # you will have to somehow get this information now to the arrow flight server
            # self.latest_input_received = self.state_tag.copy() #recovered_state["latest_input_received"]
            self.functionObject.deserialize(object_states)
            self.out_seq = recovered_state["out_seq"]  
            self.logged_outputs = recovered_state["logged_outputs"]
            self.target_output_state = recovered_state["target_output_state"]

            self.truncate_log() # the process could have failed between checkpoint and truncate log
            self.expected_path = self.get_expected_path()
            print("EXPECTED PATH", self.expected_path)

            self.ckpt_counter = -1
            
            
        
        self.log_state_tag()        

    def initialize(self):
        message = pyarrow.py_buffer(pickle.dumps((self.id,self.channel, {i:list(self.parents[i].keys()) for i in self.parents})))
        action = pyarrow.flight.Action("register_channel",message)
        self.flight_client.do_action(action)


    def truncate_logged_outputs(self, target_id, channel, target_ckpt_state):
        
        if VERBOSE:
            print("STARTING TRUNCATE", target_id, channel, target_ckpt_state, self.target_output_state)
        old_min = min(self.target_output_state[target_id].values())
        self.target_output_state[target_id][channel] = target_ckpt_state
        new_min = min(self.target_output_state[target_id].values())

        self.output_lock.acquire()
        if new_min > old_min:
            for key in range(old_min, new_min):
                if key in self.logged_outputs:
                    if VERBOSE:
                        print("REMOVING KEY",key,"FROM LOGGED OUTPUTS")
                    self.logged_outputs.pop(key)
        self.output_lock.release() 

    def checkpoint(self, method = "s3"):
        if not FT:
            return
        # write logged outputs, state, state_tag to reliable storage

        function_object_state, mode = self.functionObject.serialize()

        if function_object_state is None:
            return

        s3_resource = boto3.resource('s3')
        bucket, key = self.checkpoint_location

        if mode == "all":
            self.ckpt_number = 0
            self.ckpt_files = {}

        key = key + "." + str(self.ckpt_number) 
        self.ckpt_files[(bucket, key)] = {}

        for k in function_object_state:
            if type(function_object_state[k]) == polars.internals.frame.DataFrame:
                pa.parquet.write_table(function_object_state[k].to_arrow(), "s3://" + bucket + "/" + key + "." + str(k) + ".parquet")
                self.ckpt_files[(bucket, key)][k] = "polars"
            elif type(function_object_state[k]) == pd.core.frame.DataFrame:
                function_object_state[k].to_parquet("s3://" + bucket + "/" + key + "." + str(k) + ".parquet")
                self.ckpt_files[(bucket, key)][k] = "pandas"
            else:
                s3_resource.Object(bucket, key + "." + str(k) + ".pkl").put(Body=pickle.dumps(function_object_state[k]))
                self.ckpt_files[(bucket, key)][k] = "pickle"

        self.ckpt_number += 1

        self.output_lock.acquire()
        state = { "logged_outputs": self.logged_outputs, "out_seq" : self.out_seq,
        "function_object": self.ckpt_number, "tag":self.state_tag, "target_output_state": self.target_output_state, "ckpt_files": self.ckpt_files,"disk_fileno":self.disk_fileno}
        state_str = pickle.dumps(state)
        self.output_lock.release()

        if method == "s3":
           
            # if this fails we are dead, but probability of this failing much smaller than dump failing
            # the lack of rename in S3 is a big problem
            bucket, key = self.checkpoint_location
            s3_resource.Object(bucket, key).put(Body=state_str)
        
        elif method == "local":
            raise Exception("not supported anymore")
            # f = open("/home/ubuntu/ckpt-" + str(self.id) + "-" + str(self.channel) + "-temp.pkl","wb")
            # f.write(state_str)
            # f.flush()
            # os.fsync(f.fileno())
            # f.close()
            # # if this fails we are dead, but probability of this failing much smaller than dump failing
            # os.rename("/home/ubuntu/ckpt-" + str(self.id) + "-" + str(self.channel) + "-temp.pkl", "/home/ubuntu/ckpt-" + str(self.id) + "-" + str(self.channel) + ".pkl")
        
        else:
            raise Exception

        self.truncate_log()
        truncate_tasks = []
        for parent in self.parents:
            for channel in self.parents[parent]:
                handler = self.parents[parent][channel]
                truncate_tasks.append(handler.truncate_logged_outputs.remote(self.id, self.channel, self.state_tag[(parent,channel)]))
        try:
            ray.get(truncate_tasks)
        except ray.exceptions.RayActorError:
            print("A PARENT HAS FAILED")
            pass
    
    def ask_upstream_for_help(self, new_ip, total_channels):
        recover_tasks = []
        print("UPSTREAM",self.parents)
        for parent in self.parents:
            for channel in self.parents[parent]:
                handler = self.parents[parent][channel]
                recover_tasks.append(handler.update_target_ip_and_help_target_recover.remote(self.id, self.channel, total_channels, self.state_tag[(parent,channel)], new_ip))
        ray.get(recover_tasks)

    def get_batches(self):

        message = pyarrow.py_buffer(pickle.dumps((self.id, self.channel)))
        action = pyarrow.flight.Action("get_batches_info", message)
        result = next(self.flight_client.do_action(action))
        batch_info, should_terminate = pickle.loads(result.body.to_pybytes())
        assert type(batch_info) == dict

        return batch_info, should_terminate

    
    def get_expected_path(self):
        return deque([pickle.loads(i) for i in self.head_r.lrange("state-tag-" + str(self.id) + "-" + str(self.channel), 0, self.head_r.llen("state-tag-" + str(self.id) + "-" + str(self.channel)))])
    
    # truncate the log to the checkpoint
    def truncate_log(self):
        # you truncated the log right before you failed.
        if self.head_r.llen("state-tag-" + str(self.id) + "-" + str(self.channel)) == 0:
            return
        first_state = pickle.loads(self.head_r.lrange("state-tag-" + str(self.id) + "-" + str(self.channel), 0, 1)[0])
        try:
            diffs = np.array([first_state[i] - self.state_tag[i] for i in first_state])
        except:
            raise Exception(first_state, self.state_tag)
        hmm = np.count_nonzero(diffs > 0)
        huh = np.count_nonzero(diffs < 0)
        if hmm > 0 and huh > 0:
            raise Exception("inconsistent first state in WAL with current state tag", first_state, self.state_tag)
        elif hmm > 0:
            return
        
        while True:
            if self.head_r.llen("state-tag-" + str(self.id) + "-" + str(self.channel)) == 0:
                raise Exception
            tag = pickle.loads(self.head_r.lpop("state-tag-" + str(self.id) + "-" + str(self.channel)))
            if tag == self.state_tag:
                return

    def schedule_for_execution(self, batch_info):

        if len(self.expected_path) == 0:
            # process the source with the most backlog
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
                self.state_tag[(parent,channel)] += batch_info[parent, channel]
            
            self.log_state_tag()
            return parent, requests

        else:
            expected = self.expected_path[0]
            diffs = {i: expected[i] - self.state_tag[i] for i in expected}
            # there could be more than one possible difference, since we now allow scheduling all batches for different channels in a single task node
            to_do = set()
            for key in diffs:
                if diffs[key] > 0:
                    to_do.add(key)
            if len(to_do) == 0:
                raise Exception("there should be some difference..",self.state_tag,expected)
            
            for parent, channel in to_do:
                required_batches = diffs[(parent, channel)]
                if batch_info[parent,channel] < required_batches:
                    # cannot fulfill expectation
                    #print("CANNOT FULFILL EXPECTATION")
                    return None, None

            requests = {}
            for parent, channel in to_do:
                requests[parent,channel] = diffs[(parent,channel)]

            self.state_tag = expected
            self.expected_path.popleft()
            self.log_state_tag()
            return parent, requests

@ray.remote
class NonBlockingTaskNode(TaskNode):
    def __init__(self, id, channel,  mapping, functionObject, parents, checkpoint_location, checkpoint_interval = 10, ckpt = None) -> None:
        super().__init__(id, channel,  mapping, functionObject, parents, checkpoint_location, checkpoint_interval , ckpt )
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

            #print(self.state_tag)

            batches = [i for i in batches if i is not None]
            results = self.functionObject.execute( batches, self.physical_to_logical_mapping[stream_id], self.channel)
            
            # this is a very subtle point. You will only breakout if length of self.target, i.e. the original length of 
            # target list is bigger than 0. So you had somebody to send to but now you don't

            break_out = False
            if results is not None and len(self.targets) > 0:
                if self.push(results) is False:
                    break_out = True
                    break
            else:
                pass
        
            if break_out:
                break
            
            self.ckpt_counter += 1
            if FT and self.ckpt_counter % self.checkpoint_interval == 0:
                if VERBOSE:
                    print(self.id, "CHECKPOINTING")
                self.checkpoint()
        
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
        self.r.publish("node-done-"+str(self.id),str(self.channel))
    
@ray.remote
class BlockingTaskNode(TaskNode):
    def __init__(self, id, channel,  mapping, output_dataset, functionObject, parents, checkpoint_location, checkpoint_interval = 10, ckpt = None) -> None:
        super().__init__(id, channel,  mapping, functionObject, parents, checkpoint_location, checkpoint_interval , ckpt )
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
            
            if VERBOSE:
                print(self.state_tag)
            
            batches = [i for i in batches if i is not None]
            results = self.functionObject.execute( batches,self.physical_to_logical_mapping[stream_id], self.channel)
            
            self.ckpt_counter += 1
            if FT and self.ckpt_counter % self.checkpoint_interval == 0:
                if VERBOSE:
                    print(self.id, "CHECKPOINTING")
                self.checkpoint()

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
        
        #self.done()
        self.r.publish("node-done-"+str(self.id),str(self.channel))
