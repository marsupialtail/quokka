from collections import deque
from dataset import * 
import redis
import sys
import pyarrow as pa
import polars
import numpy as np
import pandas as pd
import time
import ray
import gc
import pickle

ray.init("auto", ignore_reinit_error=True, runtime_env={"working_dir":"/home/ubuntu/quokka","excludes":["*.csv","*.tbl","*.parquet"]})

#ray.init(ignore_reinit_error=True) # do this locally

#ray.timeline("profile.json")

    
@ray.remote
class Dataset:

    def __init__(self, num_channels) -> None:
        self.num_channels = num_channels
        self.objects = {i: [] for i in range(self.num_channels)}
        self.metadata = {}
        self.remaining_channels = {i for i in range(self.num_channels)}
        self.done = False

    # only one person will ever call this, and that is the master node
    # def change_num_channels(self, num_channels):
    #     if num_channels > self.num_channels:
    #         for i in range(self.num_channels, num_channels):
    #             self.objects[i] = {}
    #     self.num_channels = num_channels

    def added_object(self, channel, object_handle):

        if channel not in self.objects or channel not in self.remaining_channels:
            raise Exception
        self.objects[channel].append(object_handle)
    
    def add_metadata(self, channel, object_handle):
        if channel in self.metadata or channel not in self.remaining_channels:
            raise Exception("Cannot add metadata for the same channel twice")
        self.metadata[channel] = object_handle
    
    def done_channel(self, channel):
        self.remaining_channels.remove(channel)
        if len(self.remaining_channels) == 0:
            self.done = True

    def is_complete(self):
        return self.done

    # debugging method
    def print_all(self):
        for channel in self.objects:
            for object in self.objects[channel]:
                r = redis.Redis(host=object[0], port=6800, db=0)
                print(pickle.loads(r.get(object[1])))
    
    def get_objects(self):
        assert self.is_complete()
        return self.objects

    def to_pandas(self):
        assert self.is_complete()
        dfs = []
        for channel in self.objects:
            for object in self.objects[channel]:
                r = redis.Redis(host=object[0], port=6800, db=0)
                dfs.append(pickle.loads(r.get(object[1])))
        return pd.concat(dfs)


class TaskNode:

    # parallelism is going to be a dict of channel_id : ip
    def __init__(self, id, channel_to_ip):
        self.id = id
        self.channel_to_ip = channel_to_ip
        self.targets = {}
        self.r = redis.Redis(host='localhost', port=6800, db=0)
        self.target_rs = {}
        self.target_ps = {}

        # track the targets that are still alive
        self.alive_targets = {}
        # you are only allowed to send a message to a done target once. More than once is unforgivable. This is because the current mechanism
        # checks if something is done, and then sends a message. The target can be done while the message is sending. But then come next message,
        # the check must tell you that the target is done.
        self.strikes = set()
    
    def append_to_targets(self,tup):
        node_id, channel_to_ip, partition_key = tup

        unique_ips = set(channel_to_ip.values())
        redis_clients = {i: redis.Redis(host=i, port=6800, db=0) if i != ray.util.get_node_ip_address() else redis.Redis(host='localhost', port = 6800, db=0) for i in unique_ips}
        self.targets[node_id] = (channel_to_ip, partition_key)
        self.target_rs[node_id] = {}
        self.target_ps[node_id] = []
        for channel in channel_to_ip:
            self.target_rs[node_id][channel] = redis_clients[channel_to_ip[channel]]
        
        for client in redis_clients:
            pubsub = redis_clients[client].pubsub(ignore_subscribe_messages = True)
            pubsub.subscribe("node-done-"+str(node_id))
            self.target_ps[node_id].append(pubsub)
        
        self.alive_targets[node_id] = {i for i in channel_to_ip}
        for i in channel_to_ip:
            self.strikes.add((node_id,i))

    def initialize(self):
        # child classes must override this method
        raise NotImplementedError

    def execute(self):
        # child classes must override this method
        raise NotImplementedError

    # determines if there are still targets alive, returns True or False. 
    def update_targets(self):

        for target_node in self.target_ps:
            # there are #-ip locations you need to poll here.
            for client in self.target_ps[target_node]:
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

    def get_batches(self, mailbox, mailbox_id, p, my_id):
        while True:
            message = p.get_message()
            if message is None:
                break
            if message['channel'].decode('utf-8') == "mailbox-" + str(self.id) + "-" + str(my_id):
                mailbox.append(message['data'])
            elif message['channel'].decode('utf-8') ==  "mailbox-id-" + str(self.id) + "-" + str(my_id):
                mailbox_id.append(int(message['data']))
        
        my_batches = {}
        while len(mailbox) > 0 and len(mailbox_id) > 0:
            first = mailbox.popleft()
            stream_id = mailbox_id.popleft()

            if len(first) < 10 and first.decode("utf-8") == "done":

                # the responsibility for checking how many executors this input stream has is now resting on the consumer.

                self.source_parallelism[stream_id] -= 1
                if self.source_parallelism[stream_id] == 0:
                    self.input_streams.pop(self.physical_to_logical_stream_mapping[stream_id])
                    print("done", self.physical_to_logical_stream_mapping[stream_id])
            else:
                if stream_id in my_batches:
                    my_batches[stream_id].append(pickle.loads(first))
                else:
                    my_batches[stream_id] = [pickle.loads(first)]
        return my_batches

    def push(self, data):

        print("stream psuh start",time.time())

        if not self.update_targets():
            print("stream psuh end",time.time())
            return False
        
        for target in self.alive_targets:
            original_channel_to_ip, partition_key = self.targets[target]
            for channel in self.alive_targets[target]:
                if partition_key is not None:
                    if type(data) == pd.core.frame.DataFrame:
                        if type(partition_key) == str:
                            payload = data[data[partition_key] % len(original_channel_to_ip) == channel]
                            print("payload size ",payload.memory_usage().sum(), channel)
                        elif callable(partition_key):
                            payload = partition_key(data, channel)
                        else:
                            raise Exception("Can't understand partition strategy")
                    if type(data) == polars.internals.frame.DataFrame:
                        if type(partition_key) == str:
                            payload = data[data[partition_key] % len(original_channel_to_ip) == channel]
                        elif callable(partition_key):
                            payload = partition_key(data, channel)
                        else:
                            raise Exception("Can't understand partition strategy")
                       
                else:
                    payload = data
                # don't worry about target being full for now.
                print("not checking if target is full. This will break with larger joins for sure.")
                pipeline = self.target_rs[target][channel].pipeline()
                #pipeline.publish("mailbox-"+str(target) + "-" + str(channel),context.serialize(payload).to_buffer().to_pybytes())
                pipeline.publish("mailbox-"+str(target) + "-" + str(channel),pickle.dumps(payload))

                pipeline.publish("mailbox-id-"+str(target) + "-" + str(channel),self.id)
                results = pipeline.execute()
                if False in results:
                    if (target, channel) not in self.strikes:
                        raise Exception
                    self.strikes.remove((target, channel))

        print("stream psuh end",time.time())
        return True

    def done(self):
        if not self.update_targets():
            return False
        for target in self.alive_targets:
            for channel in self.alive_targets[target]:
                pipeline = self.target_rs[target][channel].pipeline()
                pipeline.publish("mailbox-"+str(target) + "-" + str(channel),"done")
                pipeline.publish("mailbox-id-"+str(target) + "-" + str(channel),self.id)
                results = pipeline.execute()
                if False in results:
                    if (target, channel) not in self.strikes:
                        print(target,channel)
                        raise Exception
                    self.strikes.remove((target, channel))
        return True

@ray.remote
class NonBlockingTaskNode(TaskNode):

    # this is for one of the parallel threads

    def __init__(self, streams, datasets, functionObject, id, channel_to_ip, mapping, source_parallelism, ip) -> None:

        super().__init__(id, channel_to_ip)

        self.functionObject = functionObject
        self.input_streams = streams
        self.datasets = datasets
        
        # this maps what the system's stream_id is to what the user called the stream when they created the task node
        self.physical_to_logical_stream_mapping = mapping
        self.source_parallelism = source_parallelism
        
        self.ip = ip

    def initialize(self, my_id):
        if self.datasets is not None:
            self.functionObject.initialize(self.datasets, my_id)

    def execute(self, my_id):

        print("task start",time.time())

        p = self.r.pubsub(ignore_subscribe_messages=True)
        p.subscribe("mailbox-" + str(self.id) + "-" + str(my_id), "mailbox-id-" + str(self.id) + "-" + str(my_id))

        assert my_id in self.channel_to_ip

        mailbox = deque()
        mailbox_id = deque()

        while len(self.input_streams) > 0:

            my_batches = self.get_batches(mailbox, mailbox_id, p, my_id)

            for stream_id in my_batches:

                results = self.functionObject.execute(my_batches[stream_id], self.physical_to_logical_stream_mapping[stream_id], my_id)
                if hasattr(self.functionObject, 'early_termination') and self.functionObject.early_termination: 
                    break

                # this is a very subtle point. You will only breakout if length of self.target, i.e. the original length of 
                # target list is bigger than 0. So you had somebody to send to but now you don't

                if results is not None and len(self.targets) > 0:
                    break_out = False
                    assert type(results) == list                    
                    for result in results:
                        if self.push(result) is False:
                            break_out = True
                            break
                    if break_out:
                        break
                else:
                    pass
    
        obj_done =  self.functionObject.done(my_id) 
        del self.functionObject
        gc.collect()
        if obj_done is not None:
            self.push(obj_done)
        
        self.done()
        self.r.publish("node-done-"+str(self.id),str(my_id))
        print("task end",time.time())

@ray.remote
class BlockingTaskNode(TaskNode):

    # this is for one of the parallel threads

    def __init__(self, streams, datasets, output_dataset, functionObject, id, channel_to_ip, mapping, source_parallelism, ip) -> None:

        super().__init__(id, channel_to_ip)

        self.functionObject = functionObject
        self.input_streams = streams
        self.datasets = datasets
        self.output_dataset = output_dataset
        
        # this maps what the system's stream_id is to what the user called the stream when they created the task node
        self.physical_to_logical_stream_mapping = mapping
        self.source_parallelism = source_parallelism
        
        self.ip = ip

    def initialize(self, my_id):
        if self.datasets is not None:
            self.functionObject.initialize(self.datasets, my_id)
    
    # explicit override with error. Makes no sense to append to targets for a blocking node. Need to use the dataset instead.
    def append_to_targets(self,tup):
        raise Exception("Trying to stream from a blocking node")

    def execute(self, my_id):

        # this needs to change
        print("task start",time.time())

        p = self.r.pubsub(ignore_subscribe_messages=True)
        p.subscribe("mailbox-" + str(self.id) + "-" + str(my_id), "mailbox-id-" + str(self.id) + "-" + str(my_id))

        assert my_id in self.channel_to_ip

        mailbox = deque()
        mailbox_id = deque()

        self.object_count = 0

        while len(self.input_streams) > 0:

            my_batches = self.get_batches( mailbox, mailbox_id, p, my_id)

            for stream_id in my_batches:
                results = self.functionObject.execute(my_batches[stream_id], self.physical_to_logical_stream_mapping[stream_id], my_id)
                if hasattr(self.functionObject, 'early_termination') and self.functionObject.early_termination: 
                    break

                if results is not None and len(results) > 0:
                    assert type(results) == list
                    for result in results:
                        key = str(self.id) + "-" + str(my_id) + "-" + str(self.object_count)
                        self.object_count += 1
                        self.r.set(key, pickle.dumps(result))
                        self.output_dataset.added_object.remote(my_id, (ray.util.get_node_ip_address(), key, sys.getsizeof(result)))                    
                else:
                    pass
    
        obj_done =  self.functionObject.done(my_id) 
        del self.functionObject
        gc.collect()
        if obj_done is not None:
            key = str(self.id) + "-" + str(my_id) + "-" + str(self.object_count)
            self.object_count += 1
            self.r.set(key, pickle.dumps(obj_done))
            self.output_dataset.added_object.remote(my_id, (ray.util.get_node_ip_address(), key, sys.getsizeof(obj_done)))
        
        self.output_dataset.done_channel.remote(my_id)
        self.done()
        self.r.publish("node-done-"+str(self.id),str(my_id))
        print("task end",time.time())

class InputNode(TaskNode):

    def __init__(self, id, channel_to_ip, dependent_map = {}):
        super().__init__(id, channel_to_ip)
        self.dependent_rs = {}
        self.dependent_parallelism = {}
        for key in dependent_map:
            self.dependent_parallelism[key] = dependent_map[key][1]
            r = redis.Redis(host=dependent_map[key][0], port=6800, db=0)
            p = r.pubsub(ignore_subscribe_messages=True)
            p.subscribe("input-done-" + str(key))
            self.dependent_rs[key] = p
    
    def execute(self, id):

        undone_dependencies = len(self.dependent_rs)
        while undone_dependencies > 0:
            time.sleep(0.001) # be nice
            for dependent_node in self.dependent_rs:
                message = self.dependent_rs[dependent_node].get_message()
                if message is not None:
                    if message['data'].decode("utf-8") == "done":
                        self.dependent_parallelism[dependent_node] -= 1
                        if self.dependent_parallelism[dependent_node] == 0:
                            undone_dependencies -= 1
                    else:
                        raise Exception(message['data'])

        print("input node start",time.time())
        input_generator = self.accessor.get_next_batch(id)

        for batch in input_generator:
            
            if self.batch_func is not None:
                print("batch func start",time.time())
                result = self.batch_func(batch)
                print("batch func end",time.time())
                self.push(result)
            else:
                self.push(batch)

        self.done()
        self.r.publish("input-done-" + str(self.id), "done")
        print("input node end",time.time())

@ray.remote
class InputS3CSVNode(InputNode):

    def __init__(self,id, bucket, key, names, channel_to_ip, batch_func=None, sep = ",", stride = 64 * 1024 * 1024, dependent_map = {}):
        super().__init__(id, channel_to_ip, dependent_map)
        
        self.bucket = bucket
        self.key = key
        self.names = names

        self.batch_func = batch_func
        self.sep = sep
        self.stride = stride

    def initialize(self, my_id):
        if self.bucket is None:
            raise Exception
        self.accessor = InputCSVDataset(self.bucket, self.key, self.names,0, sep = self.sep, stride = self.stride) 
        self.accessor.set_num_mappers(len(self.channel_to_ip))

@ray.remote
class InputS3MultiParquetNode(InputNode):

    def __init__(self, id, bucket, key, channel_to_ip, columns = None, filters = None, batch_func=None, dependent_map={}):
        super().__init__(id, channel_to_ip, dependent_map)

        self.bucket = bucket
        self.key = key
        self.columns = columns
        self.batch_func = batch_func
        self.filters = filters
    
    def initialize(self, my_id):
        if self.bucket is None:
            raise Exception
        self.accessor = InputMultiParquetDataset(self.bucket, self.key, columns = self.columns)
        self.accessor.set_num_mappers(len(self.channel_to_ip))

@ray.remote
class InputRedisDatasetNode(InputNode):

    def __init__(self, id, channel_objects, channel_to_ip, batch_func=None, dependent_map={}):
        super().__init__(id, channel_to_ip, dependent_map)
        self.channel_objects = channel_objects
        self.batch_func = batch_func
    
    def initialize(self, my_id):
        ip_set = set()
        for channel in self.channel_objects:
            for object in self.channel_objects[channel]:
                ip_set.add(object[0])
        self.accessor = RedisObjectsDataset(self.channel_objects, ip_set)

class TaskGraph:
    # this keeps the logical dependency DAG between tasks 
    def __init__(self) -> None:
        self.current_node = 0
        self.nodes = {}
        self.node_channel_to_ip = {}
        self.node_ips = {}
        self.datasets = {}
    
    def flip_ip_channels(self, ip_to_num_channel):
        ips = list(ip_to_num_channel.keys())
        starts = np.cumsum([0] + [ip_to_num_channel[ip] for ip in ips])
        start_dict = {ips[k]: starts[k] for k in range(len(ips))}
        lists_to_merge =  [ {i: ip for i in range(start_dict[ip], start_dict[ip] + ip_to_num_channel[ip])} for ip in ips ]
        channel_to_ip = {k: v for d in lists_to_merge for k, v in d.items()}
        for key in channel_to_ip:
            if channel_to_ip[key] == 'localhost':
                channel_to_ip[key] = ray.worker._global_node.address.split(":")[0] 

        return channel_to_ip

    def return_dependent_map(self, dependents):
        dependent_map = {}
        if len(dependents) > 0:
            for node in dependents:
                dependent_map[node] = (self.node_ips[node], len(self.node_channel_to_ip[node]))
        return dependent_map

    def new_input_redis(self, dataset, ip_to_num_channel, policy = "default", batch_func=None, dependents = []):
        
        dependent_map = self.return_dependent_map(dependents)
        channel_to_ip = self.flip_ip_channels(ip_to_num_channel)

        # this will assert that the dataset is complete. You can only call this API on a completed dataset
        objects = ray.get(dataset.get_objects.remote())

        ip_to_channel_sets = {}
        for channel in channel_to_ip:
            ip = channel_to_ip[channel]
            if ip not in ip_to_channel_sets:
                ip_to_channel_sets[ip] = {channel}
            else:
                ip_to_channel_sets[ip].add(channel)

        # current heuristics for scheduling objects to reader channels:
        # if an object can be streamed out locally from someone, always do that
        # try to balance the amounts of things that people have to stream out locally
        # if an object cannot be streamed out locally, assign it to anyone
        # try to balance the amounts of things that people have to fetch over the network.
        
        channel_objects = {channel: [] for channel in channel_to_ip}

        if policy == "default":
            local_read_sizes = {channel: 0 for channel in channel_to_ip}
            remote_read_sizes = {channel: 0 for channel in channel_to_ip}

            for writer_channel in objects:
                for object in objects[writer_channel]:
                    ip, key, size = object
                    # the object is on a machine that is not part of this task node, will have to remote fetch
                    if ip not in ip_to_channel_sets:
                        # find the channel with the least amount of remote read
                        my_channel = min(remote_read_sizes, key = remote_read_sizes.get)
                        channel_objects[my_channel].append(object)
                        remote_read_sizes[my_channel] += size
                    else:
                        eligible_sizes = {reader_channel : local_read_sizes[reader_channel] for reader_channel in ip_to_channel_sets[ip]}
                        my_channel = min(eligible_sizes, key = eligible_sizes.get)
                        channel_objects[my_channel].append(object)
                        local_read_sizes[my_channel] += size
        
        else:
            raise Exception("other distribution policies not implemented yet.")

        print(channel_objects)

        tasknode = []
        for ip in ip_to_num_channel:
            if ip != 'localhost':
                tasknode.extend([InputRedisDatasetNode.options(num_cpus=0.001, resources={"node:" + ip : 0.001}).
                remote(self.current_node, channel_objects, channel_to_ip, batch_func=batch_func, dependent_map=dependent_map) for i in range(ip_to_num_channel[ip])])
            else:
                tasknode.extend([InputRedisDatasetNode.options(num_cpus=0.001,resources={"node:" + ray.worker._global_node.address.split(":")[0] : 0.001}).
                remote(self.current_node, channel_objects, channel_to_ip, batch_func=batch_func, dependent_map=dependent_map) for i in range(ip_to_num_channel[ip])])
        
        self.nodes[self.current_node] = tasknode
        self.node_channel_to_ip[self.current_node] = channel_to_ip
        self.node_ips[self.current_node] = ip
        self.current_node += 1
        return self.current_node - 1

    def new_input_csv(self, bucket, key, names, ip_to_num_channel, batch_func=None, sep = ",", dependents = [], stride= 64 * 1024 * 1024):
        
        dependent_map = self.return_dependent_map(dependents)
        channel_to_ip = self.flip_ip_channels(ip_to_num_channel)

        tasknode = []
        for ip in ip_to_num_channel:
            if ip != 'localhost':
                tasknode.extend([InputS3CSVNode.options(num_cpus=0.001, resources={"node:" + ip : 0.001}).
                remote(self.current_node, bucket,key,names, channel_to_ip, batch_func = batch_func,sep = sep, 
                stride= stride, dependent_map = dependent_map, ) for i in range(ip_to_num_channel[ip])])
            else:
                tasknode.extend([InputS3CSVNode.options(num_cpus=0.001,resources={"node:" + ray.worker._global_node.address.split(":")[0] : 0.001}).
                remote(self.current_node, bucket,key,names, channel_to_ip, batch_func = batch_func, sep = sep,
                stride = stride, dependent_map = dependent_map, ) for i in range(ip_to_num_channel[ip])])
        
        self.nodes[self.current_node] = tasknode
        self.node_channel_to_ip[self.current_node] = channel_to_ip
        self.node_ips[self.current_node] = ip
        self.current_node += 1
        return self.current_node - 1
    

    def new_input_multiparquet(self, bucket, key,  ip_to_num_channel, batch_func=None, columns = None, filters = None, dependents = []):
        
        dependent_map = self.return_dependent_map(dependents)
        channel_to_ip = self.flip_ip_channels(ip_to_num_channel)

        tasknode = []
        for ip in ip_to_num_channel:
            if ip != 'localhost':
                tasknode.extend([InputS3MultiParquetNode.options(num_cpus=0.001, resources={"node:" + ip : 0.001}).
                remote(self.current_node, bucket,key,channel_to_ip, columns = columns,filters= filters,
                 batch_func = batch_func,dependent_map = dependent_map) for i in range(ip_to_num_channel[ip])])
            else:
                tasknode.extend([InputS3MultiParquetNode.options(num_cpus=0.001,resources={"node:" + ray.worker._global_node.address.split(":")[0] : 0.001}).
                remote(self.current_node, bucket,key,channel_to_ip, columns = columns,filters= filters,
                 batch_func = batch_func, dependent_map = dependent_map) for i in range(ip_to_num_channel[ip])])
        
        self.nodes[self.current_node] = tasknode
        self.node_channel_to_ip[self.current_node] = channel_to_ip
        self.node_ips[self.current_node] = ip
        self.current_node += 1
        return self.current_node - 1
    
    def new_non_blocking_node(self, streams, datasets, functionObject, ip_to_num_channel, partition_key):
        
        channel_to_ip = self.flip_ip_channels(ip_to_num_channel)
        # this is the mapping of physical node id to the key the user called in streams. i.e. if you made a node, task graph assigns it an internal id #
        # then if you set this node as the input of this new non blocking task node and do streams = {0: node}, then mapping will be {0: the internal id of that node}
        mapping = {}
        # this is a dictionary of stream_id to the number of channels in that stream
        source_parallelism = {}
        for key in streams:
            source = streams[key]
            if source not in self.nodes:
                raise Exception("stream source not registered")
            ray.get([i.append_to_targets.remote((self.current_node, channel_to_ip, partition_key[key])) for i in self.nodes[source]])
            mapping[source] = key
            source_parallelism[source] = len(self.node_channel_to_ip[source]) # this only cares about how many channels the source has
        
        tasknode = []
        for ip in ip_to_num_channel:
            if ip != 'localhost':
                tasknode.extend([NonBlockingTaskNode.options(num_cpus = 0.001, resources={"node:" + ip : 0.001}).remote(streams, datasets, functionObject, self.current_node, 
                channel_to_ip, mapping, source_parallelism, ip) for i in range(ip_to_num_channel[ip])])
            else:
                tasknode.extend([NonBlockingTaskNode.options(num_cpus = 0.001, resources={"node:" + ray.worker._global_node.address.split(":")[0]: 0.001}).remote(streams, 
                datasets, functionObject, self.current_node, channel_to_ip, mapping, source_parallelism, ip) for i in range(ip_to_num_channel[ip])])

        self.nodes[self.current_node] = tasknode
        self.node_channel_to_ip[self.current_node] = channel_to_ip
        self.node_ips[self.current_node] = ip
        self.current_node += 1
        return self.current_node - 1

    def new_blocking_node(self, streams, datasets, functionObject, ip_to_num_channel, partition_key):
        
        channel_to_ip = self.flip_ip_channels(ip_to_num_channel)
        mapping = {}
        source_parallelism = {}
        for key in streams:
            source = streams[key]
            if source not in self.nodes:
                raise Exception("stream source not registered")
            ray.get([i.append_to_targets.remote((self.current_node, channel_to_ip, partition_key[key])) for i in self.nodes[source]])
            mapping[source] = key
            source_parallelism[source] = len(self.node_channel_to_ip[source]) # this only cares about how many channels the source has
        
        # the datasets will all be managed on the head node. Note that they are not in charge of actually storing the objects, they just 
        # track the ids.
        output_dataset = Dataset.options(num_cpus = 0.001, resources={"node:" + ray.worker._global_node.address.split(":")[0]: 0.001}).remote(len(channel_to_ip))

        tasknode = []
        for ip in ip_to_num_channel:
            if ip != 'localhost':
                tasknode.extend([BlockingTaskNode.options(num_cpus = 0.001, resources={"node:" + ip : 0.001}).remote(streams, datasets, output_dataset, functionObject, self.current_node, 
                channel_to_ip, mapping, source_parallelism, ip) for i in range(ip_to_num_channel[ip])])
            else:
                tasknode.extend([BlockingTaskNode.options(num_cpus = 0.001, resources={"node:" + ray.worker._global_node.address.split(":")[0]: 0.001}).remote(streams, 
                datasets, output_dataset, functionObject, self.current_node, channel_to_ip, mapping, source_parallelism, ip) for i in range(ip_to_num_channel[ip])])
        
        self.nodes[self.current_node] = tasknode
        self.node_channel_to_ip[self.current_node] = channel_to_ip
        self.node_ips[self.current_node] = ip
        self.current_node += 1
        return output_dataset

    
    def initialize(self):

        processes = []
        for key in self.nodes:
            node = self.nodes[key]
            for i in range(len(node)):
                replica = node[i]
                processes.append(replica.initialize.remote(i))
        ray.get(processes)
    
    def run(self):
        processes = []
        for key in self.nodes:
            node = self.nodes[key]
            for i in range(len(node)):
                replica = node[i]
                processes.append(replica.execute.remote(i))
        ray.get(processes)
