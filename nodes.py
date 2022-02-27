from dataset import * 
import redis
import ray
from collections import deque
import gc
import sys

class Node:

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
        
        if type(data) == pd.core.frame.DataFrame:
            for target in self.alive_targets:
                original_channel_to_ip, partition_key = self.targets[target]
                for channel in self.alive_targets[target]:
                    if partition_key is not None:

                        if type(partition_key) == str:
                            payload = data[data[partition_key] % len(original_channel_to_ip) == channel]
                            print("payload size ",payload.memory_usage().sum(), channel)
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
        else:
            raise Exception

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

class TaskNode(Node):
    def __init__(self, streams, datasets, functionObject, id, channel_to_ip, mapping, source_parallelism, ip, checkpoint_location) -> None:

        super().__init__(id, channel_to_ip)

        self.functionObject = functionObject
        self.input_streams = streams
        self.datasets = datasets
        
        # this maps what the system's stream_id is to what the user called the stream when they created the task node
        self.physical_to_logical_stream_mapping = mapping
        self.source_parallelism = source_parallelism
        
        self.ip = ip
        self.checkpoint_location = checkpoint_location

    def __init__(self, checkpoint_location):

        # we cannot pickle redis clients so we have to rebuild all of them.
        bucket, key = checkpoint_location
        s3_resource = boto3.resource('s3')
        body = s3_resource.Object(bucket, key).get()['Body']
        state = pickle.loads(body.read())
        super().__init__(state.id, state.channel_to_ip)
        for tup in state.targets:
            self.append_to_targets(tup)
        
        self.input_streams = state.streams
        self.datasets = state.datasets
        
        # this maps what the system's stream_id is to what the user called the stream when they created the task node
        self.physical_to_logical_stream_mapping = state.mapping
        self.source_parallelism = state.source_parallelism
        
        self.ip = state.ip
        assert checkpoint_location == state.checkpoint_location
        self.checkpoint_location = checkpoint_location

        self.functionObject = state.functionObject

    def initialize(self, my_id):
        if self.datasets is not None:
            self.functionObject.initialize(self.datasets, my_id)
    
    # this is not intended to be called remotely
    def checkpoint(self):
        # this will be horribly inefficient atm.

        # rely on pickle to serailize the state of the functionObject. probably reasonable.

        # note that we are serializing ray object handlers in datasets. This is assuming that those things will still be around after the failure
        # this is a fair assumption as all the dataset handlers are on master node and we assume that thing doesn't die.
        state_dict = {"id": self.id, "channel_to_ip": self.channel_to_ip, "targets": self.targets, "alive_targets": self.alive_targets,
        "strikes":self.strikes, "functionObject": self.functionObject, "input_streams": self.input_streams, "datasets": self.datasets, 
        "mapping": self.physical_to_logical_stream_mapping, "source_parallelism": self.source_parallelism, "ip": self.ip,
        "checkpoint_location": self.checkpoint_location}

        state_str = pickle.dumps(state_dict)
        s3_resource = boto3.resource('s3')
        bucket, key = self.checkpoint_location
        s3_resource.Object(bucket, key).put(state_str)

@ray.remote
class NonBlockingTaskNode(TaskNode):

    # this is for one of the parallel threads

    def __init__(self, streams, datasets, functionObject, id, channel_to_ip, mapping, source_parallelism, ip, checkpoint_location) -> None:

        super().__init__( streams, datasets, functionObject, id, channel_to_ip, mapping, source_parallelism, ip, checkpoint_location)

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

    def __init__(self, streams, datasets, output_dataset, functionObject, id, channel_to_ip, mapping, source_parallelism, ip, checkpoint_location) -> None:

        super().__init__( streams, datasets, functionObject, id, channel_to_ip, mapping, source_parallelism, ip, checkpoint_location)
        self.output_dataset = output_dataset
    
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

class InputNode(Node):

    def __init__(self, id, channel_to_ip, dependent_map = {}):
        super().__init__(id, channel_to_ip)
        self.dependent_rs = {}
        self.dependent_parallelism = {}
        for key in dependent_map:
            self.dependent_parallelism[key] = dependent_map[key][1]
            ps = []
            for ip in dependent_map[key][0]:
                r = redis.Redis(host=ip, port=6800, db=0)
                p = r.pubsub(ignore_subscribe_messages=True)
                p.subscribe("input-done-" + str(key))
                ps.append(p)
            self.dependent_rs[key] = ps
    
    def execute(self, id):

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

    def __init__(self, id, bucket, key, channel_to_ip, columns = None, batch_func=None, dependent_map={}):
        super().__init__(id, channel_to_ip, dependent_map)

        self.bucket = bucket
        self.key = key
        self.columns = columns
        self.batch_func = batch_func
    
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