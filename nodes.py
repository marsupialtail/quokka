import numpy as np
import pandas as pd
import ray
from collections import deque, OrderedDict
from dataset import InputCSVDataset, InputMultiParquetDataset, InputSingleParquetDataset, RedisObjectsDataset, InputMultiCSVDataset
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
# isolated simplified test bench for different fault tolerance protocols

FT_I = True
FT = True

# above this limit we are going to start flushing things to disk
INPUT_MAILBOX_SIZE_LIMIT = 1024 * 1024 * 1024 * 2 # you can have 2GB in your input mailbox

# making a new class of things so we can easily type match when we start processing the input batch
class FlushedMessage:
    def __init__(self, loc) -> None:
        self.loc = loc

class SharedMemMessage:
    def __init__(self, format, name):
        self.format = format
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

        # track the targets that are still alive
        self.alive_targets = {}
        self.output_lock = Lock()

    def initialize(self):
        pass
    def append_to_targets(self,tup):
        node_id, channel_to_ip, partition_key = tup

        unique_ips = set(channel_to_ip.values())
        redis_clients = {i: redis.Redis(host=i, port=6800, db=0) if i != self.ip else redis.Redis(host='localhost', port = 6800, db=0) for i in unique_ips}
        self.targets[node_id] = (channel_to_ip, partition_key)
        self.target_rs[node_id] = {}
        self.target_ps[node_id] = {}

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


    def update_target_ip_and_help_target_recover(self, target_id, channel, target_out_seq_state, new_ip):

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
                    if type(partition_key) == str:
                        payload = self.logged_outputs[key][self.logged_outputs[key][partition_key] % len(self.targets[target_id][0]) == channel]
                    elif callable(partition_key):
                        payload = partition_key(self.logged_outputs[key], self.channel, channel)
                pipeline.publish("mailbox-"+str(target_id) + "-" + str(channel),pickle.dumps(payload))
                pipeline.publish("mailbox-id-"+str(target_id) + "-" + str(channel),pickle.dumps((self.id, self.channel, key)))
        results = pipeline.execute()
        if False in results:
            raise Exception
        self.output_lock.release()

    def truncate_logged_outputs(self, target_id, channel, target_ckpt_state):
        
        print("STARTING TRUNCATE", target_id, channel, target_ckpt_state, self.target_output_state)
        old_min = min(self.target_output_state[target_id].values())
        self.target_output_state[target_id][channel] = target_ckpt_state
        new_min = min(self.target_output_state[target_id].values())

        self.output_lock.acquire()
        if new_min > old_min:
            for key in range(old_min, new_min):
                if key in self.logged_outputs:
                    print("REMOVING KEY",key,"FROM LOGGED OUTPUTS")
                    self.logged_outputs.pop(key)
        self.output_lock.release()    

    def update_targets(self):

        for target_node in self.target_ps:
            # there are #-ip locations you need to poll here.
            for channel in self.target_ps[target_node]:
                client = self.target_ps[target_node][channel]
                while True:
                    message = client.get_message()
                    
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
            print("downstream failure detected")

        if type(data) == pd.core.frame.DataFrame or type(data) == polars.internals.frame.DataFrame:
            for target in self.alive_targets:
                original_channel_to_ip, partition_key = self.targets[target]
                for channel in self.alive_targets[target]:
                    print("PUSHING FROM",str(self.id),str(self.channel)," TO ",str(target),str(channel), " MY TAG ", self.out_seq)
                    if partition_key is not None:

                        if type(partition_key) == str:
                            payload = data[data[partition_key] % len(original_channel_to_ip) == channel]
                        elif callable(partition_key):
                            payload = partition_key(data, self.channel, channel)
                            # you have to send the None, because otherwise the sequence number could be messed up.
                        else:
                            raise Exception("Can't understand partition strategy")
                    else:
                        payload = data

                    # if the target is on the same machine we are just going to use shared memory, and change the payload to the shared memory name!!

                    if original_channel_to_ip[channel] == self.ip:
                        if type(data) == pd.core.frame.DataFrame:
                            batch = data
                            my_format = "pandas"
                        else:
                            batch = data.to_arrow()
                            my_format = "polars"
                        
                        object_id = ray.put(batch)
                        payload = SharedMemMessage(my_format, object_id)

                    pipeline = self.target_rs[target][channel].pipeline()
                    pipeline.publish("mailbox-"+str(target) + "-" + str(channel),pickle.dumps(payload))

                    pipeline.publish("mailbox-id-"+str(target) + "-" + str(channel),pickle.dumps((self.id, self.channel, self.out_seq)))
                    try:    
                        results = pipeline.execute()
                        if False in results:
                            print("Downstream failure detected")
                    except:
                        print("Downstream failure detected")
        else:
            # the data that you gave is a custom thing. so the partition function must be a callable
            for target in self.alive_targets:
                original_channel_to_ip, partition_key = self.targets[target]
                assert callable(partition_key) or partition_key is None
                for channel in self.alive_targets[target]:
                    print("PUSHING FROM",str(self.id),str(self.channel)," TO ",str(target),str(channel), " MY TAG ", self.out_seq)
                    payload = partition_key(data, self.channel, channel) if partition_key is not None else data

                    if original_channel_to_ip[channel] == self.ip:
                        object_id = ray.put(payload)
                        payload = SharedMemMessage("custom", object_id)

                    # don't worry about target being full for now.
                    pipeline = self.target_rs[target][channel].pipeline()
                    pipeline.publish("mailbox-"+str(target) + "-" + str(channel),pickle.dumps(payload))

                    pipeline.publish("mailbox-id-"+str(target) + "-" + str(channel),pickle.dumps((self.id, self.channel, self.out_seq)))
                    try:    
                        results = pipeline.execute()
                        if False in results:
                            print("Downstream failure detected")
                    except:
                        print("Downstream failure detected")

        return True

    def done(self):

        self.out_seq += 1

        print("IM DONE", self.id)

        self.output_lock.acquire()
        self.logged_outputs[self.out_seq] = "done"
        self.output_lock.release()

        try:    
            if not self.update_targets():
                return False
        except:
            print("downstream failure detected")

        for target in self.alive_targets:
            for channel in self.alive_targets[target]:
                pipeline = self.target_rs[target][channel].pipeline()
                pipeline.publish("mailbox-"+str(target) + "-" + str(channel),pickle.dumps("done"))
                pipeline.publish("mailbox-id-"+str(target) + "-" + str(channel),pickle.dumps((self.id, self.channel, self.out_seq)))
                try:
                    results = pipeline.execute()
                    if False in results:
                        print("Downstream failure detected")
                except:
                    print("Downstream failure detected")
        return True

class InputNode(Node):
    def __init__(self, id, channel, checkpoint_location, batch_func = None, dependent_map = {}, checkpoint_interval = 10, ckpt = None) -> None:

        super().__init__( id, channel, checkpoint_location) 

        # track the targets that are still alive
        print("INPUT ACTOR LAUNCH", self.id)

        self.batch_func = batch_func
        self.dependent_rs = {}
        self.dependent_parallelism = {}
        self.checkpoint_interval = checkpoint_interval
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)


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
            self.logged_outputs = OrderedDict()
            self.target_output_state = {}
            self.out_seq = 0
            self.state_tag = 0
            self.state = None
        else:
            if ckpt == "s3":
                s3_resource = boto3.resource('s3')
                bucket, key = self.checkpoint_location
                recovered_state = pickle.loads(s3_resource.Object(bucket, key).get()['Body'].read())
            else:
                recovered_state = pickle.load(open(ckpt,"rb"))
            
            self.logged_outputs = recovered_state["logged_outputs"]
            self.target_output_state = recovered_state["target_output_state"]
            self.state = recovered_state["state"]
            self.out_seq = recovered_state["out_seq"] 
            self.state_tag = recovered_state["tag"]
            print("INPUT NODE RECOVERED TO STATE", self.state)

        
    def checkpoint(self, method = "s3"):
        if not FT_I:
            return
        # write logged outputs, state, state_tag to reliable storage
        # for input nodes, log the outputs instead of redownlaoding is probably worth it. since the outputs could be filtered by predicate
        
        self.output_lock.acquire()
        state = { "logged_outputs": self.logged_outputs, "out_seq" : self.out_seq, "tag":self.state_tag, "target_output_state":self.target_output_state,
        "state":self.state}
        state_str = pickle.dumps(state)
        self.output_lock.release()

        if method == "s3":

            s3_resource = boto3.resource('s3')
            bucket, key = self.checkpoint_location
            # if this fails we are dead, but probability of this failing much smaller than dump failing
            # the lack of rename in S3 is a big problem
            s3_resource.Object(bucket,key).put(Body=state_str)
        
        elif method == "local":
            
            f = open("/home/ubuntu/ckpt-" + str(self.id) + "-" + str(self.channel) + "-temp.pkl","wb")
            f.write(state_str)
            f.flush()
            os.fsync(f.fileno())
            f.close()
            os.rename("/home/ubuntu/ckpt-" + str(self.id) + "-" + str(self.channel) + "-temp.pkl", "/home/ubuntu/ckpt-" + str(self.id) + "-" + str(self.channel) + ".pkl")

        print("INPUT NODE CHECKPOINTING")
        
        # if this fails we are dead, but probability of this failing much smaller than dump failing

        
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
            futs.append(self.executor.submit(next, self.input_generator))
            try:
                pos, batch = futs.popleft().result()
            except StopIteration:
                break
            self.state = pos
            if self.batch_func is not None:
                result = self.batch_func(batch)
                self.push(result)
            else:
                self.push(batch)
            if self.state_tag % self.checkpoint_interval == 0:
                self.checkpoint()
            self.state_tag += 1

        print("INPUT DONE", self.id, self.channel)
        self.done()
        self.r.publish("input-done-" + str(self.id), "done")

@ray.remote
class InputReaderNode(InputNode):
    def __init__(self, id, channel, accessor, num_channels, checkpoint_location, batch_func=None, dependent_map={}, checkpoint_interval=10, ckpt=None) -> None:
        super().__init__(id, channel, checkpoint_location, batch_func, dependent_map, checkpoint_interval, ckpt)
        self.accessor = accessor
        self.accessor.set_num_mappers(num_channels)
        self.input_generator = self.accessor.get_next_batch(channel, self.state)

@ray.remote
class InputRedisDatasetNode(InputNode):
    def __init__(self, id, channel,channel_objects, checkpoint_location,batch_func=None, dependent_map={}, ckpt = None):
        super().__init__(id, channel, checkpoint_location, batch_func = batch_func, dependent_map = dependent_map, ckpt = ckpt)
        ip_set = set()
        for da in channel_objects:
            for object in channel_objects[da]:
                ip_set.add(object[0])
        self.accessor = RedisObjectsDataset(channel_objects, ip_set)
        self.input_generator = self.accessor.get_next_batch(channel, self.state)


class TaskNode(Node):
    def __init__(self, id, channel,  mapping, datasets, functionObject, parents, checkpoint_location, checkpoint_interval = 10, ckpt = None) -> None:

        # id: int. Id of the node
        # channel: int. Channel of the node
        # streams: dictionary of logical_id : streams
        # mapping: the mapping between the name you assigned the stream to the actual id of the string.

        super().__init__(id, channel, checkpoint_location)

        self.p = self.r.pubsub(ignore_subscribe_messages=True)
        self.p.subscribe("mailbox-" + str(id) + "-" + str(channel), "mailbox-id-" + str(id) + "-" + str(channel))
        self.buffered_inputs = {(parent, channel): deque() for parent in parents for channel in parents[parent]}
        self.id = id 
        self.parents = parents # dict of id -> dict of channel -> actor handles        
        self.datasets = datasets
        self.functionObject = functionObject
        assert hasattr(functionObject, "num_states") # for recovery
        if self.datasets is not None:
            self.functionObject.initialize(self.datasets, self.channel)
        self.physical_to_logical_mapping = mapping
        self.checkpoint_interval = checkpoint_interval

        if ckpt is None:
            self.state_tag =  {(parent,channel): 0 for parent in parents for channel in parents[parent]}
            self.latest_input_received = {(parent,channel): 0 for parent in parents for channel in parents[parent]}
            self.logged_outputs = OrderedDict()
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
            self.latest_input_received = self.state_tag.copy() #recovered_state["latest_input_received"]
            self.functionObject.deserialize(object_states)
            self.out_seq = recovered_state["out_seq"]  
            self.logged_outputs = recovered_state["logged_outputs"]
            self.target_output_state = recovered_state["target_output_state"]

            self.truncate_log() # the process could have failed between checkpoint and truncate log
            self.expected_path = self.get_expected_path()
            print("EXPECTED PATH", self.expected_path)

            self.ckpt_counter = -1
            
            
        
        self.log_state_tag()        

    def checkpoint(self, method = "s3"):
        if not FT:
            return
        # write logged outputs, state, state_tag to reliable storage

        function_object_state, mode = self.functionObject.serialize()
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
        state = {"latest_input_received": self.latest_input_received, "logged_outputs": self.logged_outputs, "out_seq" : self.out_seq,
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
    
    def ask_upstream_for_help(self, new_ip):
        recover_tasks = []
        print("UPSTREAM",self.parents)
        for parent in self.parents:
            for channel in self.parents[parent]:
                handler = self.parents[parent][channel]
                recover_tasks.append(handler.update_target_ip_and_help_target_recover.remote(self.id, self.channel, self.state_tag[(parent,channel)], new_ip))
        ray.get(recover_tasks)
    
    def get_buffered_inputs_mem_usage(self):
        m = 0
        for key in self.buffered_inputs:
            m += sum([sys.getsizeof(i) for i in self.buffered_inputs[key]])
        return m

    def get_batches(self, mailbox, mailbox_id):
        while True:
            #print("dead spinning here", len(mailbox), len(mailbox_id))
            message = self.p.get_message()
            if message is None:
                break
            if message['channel'].decode('utf-8') == "mailbox-" + str(self.id) + "-" + str(self.channel):
                mailbox.append(message['data'])
            elif message['channel'].decode('utf-8') ==  "mailbox-id-" + str(self.id)+ "-" + str(self.channel):
                # this should be a tuple (source_id, source_tag)
                mailbox_id.append(pickle.loads(message['data']))
        
        batches_returned = 0
        while len(mailbox) > 0 and len(mailbox_id) > 0:
            #print("dead spinning here 1", len(mailbox), len(mailbox_id))
            first = mailbox.popleft()
            stream_id, channel,  tag = mailbox_id.popleft()

            if stream_id not in self.parents or channel not in self.parents[stream_id]:
                print("this channel has already received the done signal. stop wasting your breath.")
                continue

            if tag <= self.latest_input_received[(stream_id,channel)]:
                print("rejected an input stream's tag smaller than or equal to latest input received. input tag", tag, "current latest input received", self.latest_input_received[(stream_id, channel)])
                continue
            if tag > self.latest_input_received[(stream_id,channel)] + 1:
                print("DROPPING INPUT. THIS IS A FUTURE INPUT THAT WILL BE RESENT (hopefully)", tag, stream_id, channel, "current tag", self.latest_input_received[(stream_id,channel)])
                continue

            batches_returned += 1
            self.latest_input_received[(stream_id,channel)] = tag
            if len(first) < 20 and pickle.loads(first) == "done":
                # the responsibility for checking how many executors this input stream has is now resting on the consumer.
                self.parents[stream_id].pop(channel)
                #raise Exception
                if len(self.parents[stream_id]) == 0:
                    self.parents.pop(stream_id)
                
                print("done", stream_id)
            else:
                # check current size of the buffered inputs deque. This is rather primitive

                #curr_mem = self.get_buffered_inputs_mem_usage()
                if False: #curr_mem > INPUT_MAILBOX_SIZE_LIMIT:
                    f = open("/data/input-mailbox-" + str(self.id) + "-" + str(self.channel) + "-" + str(self.disk_fileno) + ".pkl","wb")
                    f.write(first)
                    f.flush()
                    self.buffered_inputs[(stream_id,channel)].append(FlushedMessage("/data/input-mailbox-" + str(self.id) + "-" + str(self.channel) + "-" + str(self.disk_fileno) + ".pkl"))
                    self.disk_fileno += 1
                else:
                    print("start", self.id, self.channel, time.time())
                    self.buffered_inputs[(stream_id,channel)].append(pickle.loads(first))
                    print("end", self.id, self.channel, time.time())
            
        return batches_returned
    
    def get_expected_path(self):
        return deque([pickle.loads(i) for i in self.head_r.lrange("state-tag-" + str(self.id) + "-" + str(self.channel), 0, self.head_r.llen("state-tag-" + str(self.id) + "-" + str(self.channel)))])
    
    # truncate the log to the checkpoint
    def truncate_log(self):
        # you truncated the log right before you failed.
        if self.head_r.llen("state-tag-" + str(self.id) + "-" + str(self.channel)) == 0:
            return
        first_state = pickle.loads(self.head_r.lrange("state-tag-" + str(self.id) + "-" + str(self.channel), 0, 1)[0])
        diffs = np.array([first_state[i] - self.state_tag[i] for i in first_state])
        hmm = np.count_nonzero(diffs > 0)
        if hmm > 1:
            raise Exception("I think you truncated something you shouldn't have", first_state, self.state_tag)
        if hmm == 1:
            return
        
        while True:
            if self.head_r.llen("state-tag-" + str(self.id) + "-" + str(self.channel)) == 0:
                raise Exception
            tag = pickle.loads(self.head_r.lpop("state-tag-" + str(self.id) + "-" + str(self.channel)))
            if tag == self.state_tag:
                return

    def schedule_for_execution(self):
        if len(self.expected_path) == 0:
            # process the source with the most backlog
            lengths = {i: len(self.buffered_inputs[i]) for i in self.buffered_inputs}
            parent, channel = max(lengths, key=lengths.get)
            length = lengths[(parent,channel)]
            if length == 0:
                return None, None

            # now drain that source
            batches = []
            for message in self.buffered_inputs[parent, channel]:
                if type(message) == FlushedMessage:
                    batches.append(pickle.load(open(message.loc, "rb")))
                    os.remove(message.loc)
                elif type(message) == SharedMemMessage:
                    message_format = message.format
                    object_id = message.name
                    batch = ray.get(object_id)
                    if message_format == "pandas" or message_format == "custom":
                        batches.append(batch)
                    elif message_format == "polars":
                        batches.append(polars.from_arrow(batch))
                    else:
                        raise Exception
                    ray.internal.internal_api.free(object_id)
                else:
                    batches.append(message)
            self.state_tag[(parent,channel)] += length
            self.buffered_inputs[parent,channel].clear()
            
            self.log_state_tag()
            return parent, batches

        else:
            expected = self.expected_path[0]
            diffs = {i: expected[i] - self.state_tag[i] for i in expected}
            # there should only be one nonzero value in diffs. we need to figure out which one that is.
            to_do = None
            for key in diffs:
                if diffs[key] > 0:
                    if to_do is None:
                        to_do = key
                    else:
                        raise Exception("shouldn't have more than one source > 0")
            if to_do is None:
                raise Exception("there should be some difference..",self.state_tag,expected)
            parent, channel = to_do
            required_batches = diffs[(parent, channel)]
            if len(self.buffered_inputs[parent,channel]) < required_batches:
                # cannot fulfill expectation
                #print("CANNOT FULFILL EXPECTATION")
                return None, None
            else:
                batches = []
                for i in range(required_batches):
                    message = self.buffered_inputs[parent,channel].popleft()
                    if type(message) == FlushedMessage:
                        batches.append(pickle.load(open(message.loc, "rb")))
                        os.remove(message.loc)
                    elif type(message) == SharedMemMessage:
                        message_format = message.format
                        object_id = message.name
                        batch = ray.get(object_id)
                        if message_format == "pandas" or message_format == "custom":
                            batches.append(batch)
                        elif message_format == "polars":
                            batches.append(polars.from_arrow(batch))
                        else:
                            raise Exception
                        ray.internal.internal_api.free(object_id)
                    else:
                        batches.append(message)
            self.state_tag = expected
            self.expected_path.popleft()
            self.log_state_tag()
            return parent, batches

    def input_buffers_drained(self):
        for key in self.buffered_inputs:
            if len(self.buffered_inputs[key]) > 0:
                return False
        return True
    
@ray.remote
class NonBlockingTaskNode(TaskNode):
    def __init__(self, id, channel,  mapping, datasets, functionObject, parents, checkpoint_location, checkpoint_interval = 10, ckpt = None) -> None:
        super().__init__(id, channel,  mapping, datasets, functionObject, parents, checkpoint_location, checkpoint_interval , ckpt )
    
    def execute(self):
        
        mailbox = deque()
        mailbox_meta = deque()

        while not (len(self.parents) == 0 and self.input_buffers_drained()):

            # append messages to the mailbox
            batches_returned = self.get_batches(mailbox, mailbox_meta)
            # deque messages from the mailbox in a way that makes sense

            stream_id, batches = self.schedule_for_execution()
            if stream_id is None:
                continue

            #print("BUFFERED INPUT LENGTHS",{i:len(i) for i in self.buffered_inputs})
            #print(self.state_tag)
            #print(self.latest_input_received)
            for key in self.state_tag:
                assert self.state_tag[key] <= self.latest_input_received[key]

            results = self.functionObject.execute( batches, self.physical_to_logical_mapping[stream_id], self.channel)
            
            # this is a very subtle point. You will only breakout if length of self.target, i.e. the original length of 
            # target list is bigger than 0. So you had somebody to send to but now you don't

            if results is not None and len(self.targets) > 0:
                break_out = False
                if self.push(results) is False:
                    break_out = True
                    break
                if break_out:
                    break
            else:
                pass
            
            self.ckpt_counter += 1
            if FT and self.ckpt_counter % self.checkpoint_interval == 0:
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
        
        print("TASK NODE DONE", self.id, self.channel)
        self.done()
        self.r.publish("node-done-"+str(self.id),str(self.channel))
    
@ray.remote
class BlockingTaskNode(TaskNode):
    def __init__(self, id, channel,  mapping, datasets, output_dataset, functionObject, parents, checkpoint_location, checkpoint_interval = 10, ckpt = None) -> None:
        super().__init__(id, channel,  mapping, datasets, functionObject, parents, checkpoint_location, checkpoint_interval , ckpt )
        self.output_dataset = output_dataset
        self.object_count = 0 
    # explicit override with error. Makes no sense to append to targets for a blocking node. Need to use the dataset instead.
    def append_to_targets(self,tup):
        raise Exception("Trying to stream from a blocking node")
    
    def execute(self):
        
        mailbox = deque()
        mailbox_meta = deque()

        while not (len(self.parents) == 0 and self.input_buffers_drained()):

            # append messages to the mailbox
            batches_returned = self.get_batches(mailbox, mailbox_meta)
            #print(batches_returned)
            # deque messages from the mailbox in a way that makes sense
            stream_id, batches = self.schedule_for_execution()
            #print(stream_id)
            if stream_id is None:
                continue

            print(self.state_tag)

            results = self.functionObject.execute( batches,self.physical_to_logical_mapping[stream_id], self.channel)
            
            self.ckpt_counter += 1
            if FT and self.ckpt_counter % self.checkpoint_interval == 0:
                print(self.id, "CHECKPOINTING")
                self.checkpoint()

            # this is a very subtle point. You will only breakout if length of self.target, i.e. the original length of 
            # target list is bigger than 0. So you had somebody to send to but now you don't

            if results is not None and len(results) > 0:
                key = str(self.id) + "-" + str(self.channel) + "-" + str(self.object_count)
                self.object_count += 1
                self.r.set(key, pickle.dumps(results))
                # we really should be doing sys.getsizeof(result), but that doesn't work for polars dfs
                self.output_dataset.added_object.remote(self.channel, (ray.util.get_node_ip_address(), key, len(results)))                    
            else:
                pass
        
        obj_done =  self.functionObject.done(self.channel) 

        if type(obj_done) == types.GeneratorType:
            for object in obj_done:
                if object is not None:
                    key = str(self.id) + "-" + str(self.channel) + "-" + str(self.object_count)
                    self.object_count += 1
                    self.r.set(key, pickle.dumps(object))
                    self.output_dataset.added_object.remote(self.channel, (ray.util.get_node_ip_address(), key, len(object))) 
            del self.functionObject
            gc.collect()
        else:
            del self.functionObject
            gc.collect()
            
            if obj_done is not None:
                key = str(self.id) + "-" + str(self.channel) + "-" + str(self.object_count)
                self.object_count += 1
                self.r.set(key, pickle.dumps(obj_done))
                self.output_dataset.added_object.remote(self.channel, (ray.util.get_node_ip_address(), key, len(obj_done))) 
                           
        self.output_dataset.done_channel.remote(self.channel)
        
        #self.done()
        self.r.publish("node-done-"+str(self.id),str(self.channel))
