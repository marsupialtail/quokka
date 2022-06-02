import polars
from pyquokka.nodes import * 
from pyquokka.utils import * 
import numpy as np
import pandas as pd
import time
import random
import pickle
from functools import partial
import random
from pyquokka.flight import * 

#ray.init("auto", _system_config={"worker_register_timeout_seconds": 60}, ignore_reinit_error=True, runtime_env={"working_dir":"/home/ubuntu/quokka","excludes":["*.csv","*.tbl","*.parquet"]})
#ray.init("auto", ignore_reinit_error=True, runtime_env={"working_dir":"/home/ziheng/.local/lib/python3.8/site-packages/pyquokka/"})

#ray.init(ignore_reinit_error=True) # do this locally
#ray.timeline("profile.json")


NONBLOCKING_NODE = 1
BLOCKING_NODE = 2
INPUT_REDIS_DATASET = 3
INPUT_READER_DATASET = 6

class Dataset:

    def __init__(self, wrapped_dataset) -> None:
        self.wrapped_dataset = wrapped_dataset
    
    def to_list(self):
        return ray.get(self.wrapped_dataset.to_list.remote())
    
    def to_pandas(self):
        return ray.get(self.wrapped_dataset.to_pandas.remote())
    
    def to_dict(self):
        return ray.get(self.wrapped_dataset.to_dict.remote())

@ray.remote
class WrappedDataset:

    def __init__(self, num_channels) -> None:
        self.num_channels = num_channels
        self.objects = {i: [] for i in range(self.num_channels)}
        self.metadata = {}
        self.remaining_channels = {i for i in range(self.num_channels)}
        self.done = False

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
        try:
            return polars.concat(dfs)
        except:
            return pd.concat(dfs)
    
    def to_list(self):
        assert self.is_complete()
        ret = []
        for channel in self.objects:
            for object in self.objects[channel]:
                r = redis.Redis(host=object[0], port=6800, db=0)
                ret.append(pickle.loads(r.get(object[1])))
        return ret

    def to_dict(self):
        assert self.is_complete()
        d = {channel:[] for channel in self.objects}
        for channel in self.objects:
            for object in self.objects[channel]:
                r = redis.Redis(host=object[0], port=6800, db=0)
                thing = pickle.loads(r.get(object[1]))
                if type(thing) == list:
                    d[channel].extend(thing)
                else:
                    d[channel].append(thing)
        return d

class TaskGraph:
    # this keeps the logical dependency DAG between tasks 
    def __init__(self, cluster, checkpoint_bucket = "quokka-checkpoint") -> None:
        self.cluster = cluster
        self.current_node = 0
        self.nodes = {}
        self.node_channel_to_ip = {}
        self.node_ips = {}
        self.node_type = {}
        self.node_parents = {}
        self.node_args = {}
        self.checkpoint_bucket = checkpoint_bucket
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(checkpoint_bucket)
        bucket.objects.all().delete()
        r = redis.Redis(host=str(self.cluster.leader_public_ip), port=6800, db=0)
        state_tags = [i.decode("utf-8") for i in r.keys() if "state-tag" in i.decode("utf-8")] 
        for state_tag in state_tags:
            r.delete(state_tag)
    
    def flip_ip_channels(self, ip_to_num_channel):
        ips = sorted(list(ip_to_num_channel.keys()))
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

    def epilogue(self,tasknode, channel_to_ip, ips):
        self.nodes[self.current_node] = tasknode
        self.node_channel_to_ip[self.current_node] = channel_to_ip
        self.node_ips[self.current_node] = ips
        self.current_node += 1
        return self.current_node - 1

    def new_input_redis(self, dataset, ip_to_num_channel = None, policy = "default", batch_func=None, dependents = []):
        
        dependent_map = self.return_dependent_map(dependents)
        if ip_to_num_channel is None:
            # automatically come up with some policy
            ip_to_num_channel = {ip: self.cluster.cpu_count for ip in list(self.cluster.private_ips.values())}
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

        print("CHANNEL_OBJECTS",channel_objects)

        tasknode = {}
        for channel in channel_to_ip:
            ip = channel_to_ip[channel]
            if ip != 'localhost':
                tasknode[channel] = InputRedisDatasetNode.options(max_concurrency = 2, num_cpus=0.001, resources={"node:" + ip : 0.001}
                ).remote(self.current_node, channel, channel_objects, (self.checkpoint_bucket, str(self.current_node) + "-" + str(channel)), batch_func=batch_func, dependent_map=dependent_map)
            else:
                tasknode[channel] = InputRedisDatasetNode.options(max_concurrency = 2, num_cpus=0.001,resources={"node:" + ray.worker._global_node.address.split(":")[0] : 0.001}
                ).remote(self.current_node, channel, channel_objects, (self.checkpoint_bucket, str(self.current_node) + "-" + str(channel)), batch_func=batch_func, dependent_map=dependent_map)
        
        self.node_type[self.current_node] = INPUT_REDIS_DATASET
        self.node_args[self.current_node] = {"channel_objects":channel_objects, "batch_func": batch_func, "dependent_map":dependent_map}
        return self.epilogue(tasknode,channel_to_ip, tuple(ip_to_num_channel.keys()))

    def new_input_reader_node(self, reader, ip_to_num_channel = None, batch_func = None, dependents = [], ckpt_interval = 10):

        dependent_map = self.return_dependent_map(dependents)

        if ip_to_num_channel is None:
            # automatically come up with some policy
            ip_to_num_channel = {ip: self.cluster.cpu_count for ip in list(self.cluster.private_ips.values())}

        channel_to_ip = self.flip_ip_channels(ip_to_num_channel)

        # this is some state that is associated with the number of channels. typically for reading csvs or text files where you have to decide the split points.
        
        if hasattr(reader, "get_own_state"):
            reader.get_own_state(len(channel_to_ip))
        
        # set num channels is still needed later to initialize the self.s3 object, which can't be pickled!

        tasknode = {}
        for channel in channel_to_ip:
            ip = channel_to_ip[channel]
            if ip != 'localhost':
                tasknode[channel] = InputReaderNode.options(max_concurrency = 2, num_cpus=0.001, resources={"node:" + ip : 0.001}
                ).remote(self.current_node, channel, reader, len(channel_to_ip), (self.checkpoint_bucket, str(self.current_node) + "-" + str(channel)),
                batch_func = batch_func,dependent_map = dependent_map, checkpoint_interval = ckpt_interval)
            else:
                tasknode[channel] = InputReaderNode.options(max_concurrency = 2, num_cpus=0.001,resources={"node:" + ray.worker._global_node.address.split(":")[0] : 0.001}
                ).remote(self.current_node, channel, reader, len(channel_to_ip), (self.checkpoint_bucket, str(self.current_node) + "-" + str(channel)), 
                batch_func = batch_func, dependent_map = dependent_map, checkpoint_interval = ckpt_interval) 
        
        self.node_type[self.current_node] = INPUT_READER_DATASET
        self.node_args[self.current_node] = {"reader": reader, "batch_func":batch_func, "dependent_map" : dependent_map, "ckpt_interval": ckpt_interval}
        return self.epilogue(tasknode,channel_to_ip, tuple(ip_to_num_channel.keys()))
    
    def flip_channels_ip(self, channel_to_ip):
        ips = channel_to_ip.values()
        result = {ip: 0 for ip in ips}
        for channel in channel_to_ip:
            result[channel_to_ip[channel]] += 1
        return result
    
    def get_default_partition(self, source_ip_to_num_channel, target_ip_to_num_channel):
        # this can get more sophisticated in the future. For now it's super dumb.
        
        def partition_key_0(ratio, data, source_channel, num_target_channels):
            target_channel = source_channel // ratio
            return {target_channel: data}
        def partition_key_1(ratio, data, source_channel, num_target_channels):
            # return entirety of data to a random channel between source_channel * ratio and source_channel * ratio + ratio
            target_channel = int(random.random() * ratio) + source_channel * ratio
            return {target_channel: data}
        assert(set(source_ip_to_num_channel) == set(target_ip_to_num_channel))
        ratio = None
        direction = None
        for ip in source_ip_to_num_channel:
            if ratio is None:
                if source_ip_to_num_channel[ip] % target_ip_to_num_channel[ip] == 0:
                    ratio = source_ip_to_num_channel[ip] // target_ip_to_num_channel[ip]
                    direction = 0
                elif target_ip_to_num_channel[ip] % source_ip_to_num_channel[ip] == 0:
                    ratio = target_ip_to_num_channel[ip] // source_ip_to_num_channel[ip]
                    direction = 1
                else:
                    raise Exception("Can't support automated partition function generation since number of channels not a whole multiple of each other between source and target")
            elif ratio is not None:
                if direction == 0:
                    assert target_ip_to_num_channel[ip] * ratio == source_ip_to_num_channel[ip], "ratio of number of channels between source and target must be same for every ip right now"
                elif direction == 1:
                    assert source_ip_to_num_channel[ip] * ratio == target_ip_to_num_channel[ip], "ratio of number of channels between source and target must be same for every ip right now"
        
        print("RATIO", ratio)
        if direction == 0:
            return partial(partition_key_0, ratio)
        elif direction == 1:
            return partial(partition_key_1, ratio)
        else:
            return "Something is wrong"

    def prologue(self, streams, ip_to_num_channel, channel_to_ip, partition_key_supplied):
        partition_key = {}
        def partition_key_str(key, data, source_channel, num_target_channels):
            result = {}
            for channel in range(num_target_channels):
                if type(data) == pd.core.frame.DataFrame:
                    if "int" in str(data.dtypes[key]).lower() or "float" in str(data.dtypes[key]).lower():
                        payload = data[data[key] % num_target_channels == channel]
                    elif data.dtypes[key] == 'object': # str
                        # this is not the fastest. we rehashing this everytime.
                        payload = data[pd.util.hash_array(data[key].to_numpy()) % num_target_channels == channel]
                    else:
                        raise Exception("invalid partition column type, supports ints, floats and strs")
                elif type(data) == polars.internals.frame.DataFrame:
                    if "int" in str(data[key].dtype).lower() or "float" in str(data[key].dtype).lower():
                        payload = data[data[key] % num_target_channels == channel]
                    elif data[key].dtype == polars.datatypes.Utf8:
                        payload = data[data[key].hash() % num_target_channels == channel]
                    else:
                        raise Exception("invalid partition column type, supports ints, floats and strs")
                result[channel] = payload
            return result
        
        def broadcast(data, source_channel, num_target_channels):
            return {i: data for i in range(num_target_channels)}

        if ip_to_num_channel is None and channel_to_ip is None: 
            # automatically come up with some policy, user supplied no information
            ip_to_num_channel = {ip: 1 for ip in list(self.cluster.private_ips.values())}
            channel_to_ip = self.flip_ip_channels(ip_to_num_channel)
        elif channel_to_ip is None:
            channel_to_ip = self.flip_ip_channels(ip_to_num_channel)
        elif ip_to_num_channel is None:
            ip_to_num_channel = self.flip_channels_ip(channel_to_ip)
        else:
            raise Exception("Cannot specify both ip_to_num_channel and channel_to_ip")
        
        
        for key in streams:
            if key not in partition_key_supplied:
                source = streams[key]
                source_ip_to_num_channel = self.flip_channels_ip(self.node_channel_to_ip[source])
                partition_key[key] = self.get_default_partition(source_ip_to_num_channel, ip_to_num_channel)
            else:
                # this has been provided
                if type(partition_key_supplied[key]) == str:
                    print("Inferring hash partitioning strategy for column ", partition_key_supplied[key], "the source node must produce pyarrow, polars or pandas dataframes, and the dataframes must have this column!")
                    partition_key[key] = partial(partition_key_str, partition_key_supplied[key])
                elif partition_key_supplied[key] == None:
                    partition_key[key] = broadcast
                elif callable(partition_key_supplied[key]):
                    from inspect import signature
                    assert len(signature(partition_key_supplied[key]).parameters) == 3, "custom partition function must accept three arguments: data object, source channel id, and the number of target channels"
                    partition_key[key] = partition_key_supplied[key]
                else:
                    raise Exception("Can't understand user defined partition strategy")

        # this is the mapping of physical node id to the key the user called in streams. i.e. if you made a node, task graph assigns it an internal id #
        # then if you set this node as the input of this new non blocking task node and do streams = {0: node}, then mapping will be {0: the internal id of that node}
        mapping = {}
        parents = {}
        for key in streams:
            source = streams[key]
            if source not in self.nodes:
                raise Exception("stream source not registered")
            ray.get([self.nodes[source][i].append_to_targets.remote((self.current_node, channel_to_ip, partition_key[key])) for i in self.nodes[source]])
            mapping[source] = key
            parents[source] = self.nodes[source]
        self.node_parents[self.current_node] = parents
        
        return ip_to_num_channel, channel_to_ip, partition_key, mapping, parents

    def new_non_blocking_node(self, streams, functionObject, ip_to_num_channel = None, channel_to_ip = None, partition_key_supplied = {}, ckpt_interval = 10):
        
        ip_to_num_channel, channel_to_ip, partition_key, mapping, parents = self.prologue(streams, ip_to_num_channel, channel_to_ip, partition_key_supplied)
        #print("MAPPING", mapping)
        tasknode = {}
        for channel in channel_to_ip:
            ip = channel_to_ip[channel]
            if ip != 'localhost':
                tasknode[channel] = NonBlockingTaskNode.options(max_concurrency = 2, num_cpus = 0.001, resources={"node:" + ip : 0.001}).remote(self.current_node, channel, mapping,  functionObject, 
                parents, (self.checkpoint_bucket , str(self.current_node) + "-" + str(channel)), checkpoint_interval = ckpt_interval)
            else:
                tasknode[channel] = NonBlockingTaskNode.options(max_concurrency = 2, num_cpus = 0.001, resources={"node:" + ray.worker._global_node.address.split(":")[0]: 0.001}).remote(self.current_node,
                 channel, mapping, functionObject, parents, (self.checkpoint_bucket , str(self.current_node) + "-" + str(channel)), checkpoint_interval = ckpt_interval)
        
        self.node_type[self.current_node] = NONBLOCKING_NODE
        self.node_args[self.current_node] = {"mapping":mapping,  "functionObject":functionObject, "ckpt_interval": ckpt_interval, "partition_key":partition_key}
        return self.epilogue(tasknode,channel_to_ip, tuple(ip_to_num_channel.keys()))

    def new_blocking_node(self, streams, functionObject,ip_to_num_channel = None, channel_to_ip = None, partition_key_supplied = {},ckpt_interval = 10):
        
        ip_to_num_channel, channel_to_ip, partition_key, mapping, parents = self.prologue(streams, ip_to_num_channel, channel_to_ip, partition_key_supplied)

        # the datasets will all be managed on the head node. Note that they are not in charge of actually storing the objects, they just 
        # track the ids.
        output_dataset = WrappedDataset.options(num_cpus = 0.001, resources={"node:" + str(self.cluster.leader_private_ip): 0.001}).remote(len(channel_to_ip))

        tasknode = {}
        for channel in channel_to_ip:
            ip = channel_to_ip[channel]
            if ip != 'localhost':
                tasknode[channel] = BlockingTaskNode.options(max_concurrency = 2, num_cpus = 0.001, resources={"node:" + ip : 0.001}).remote(self.current_node, channel, mapping, output_dataset, functionObject, 
                parents, (self.checkpoint_bucket , str(self.current_node) + "-" + str(channel)), checkpoint_interval = ckpt_interval)
            else:
                tasknode[channel] = BlockingTaskNode.options(max_concurrency = 2, num_cpus = 0.001, resources={"node:" + ray.worker._global_node.address.split(":")[0]: 0.001}).remote(self.current_node, 
                channel, mapping, output_dataset, functionObject, parents, (self.checkpoint_bucket , str(self.current_node) + "-" + str(channel)), checkpoint_interval = ckpt_interval)
            
        self.node_type[self.current_node] = BLOCKING_NODE
        self.node_args[self.current_node] = {"mapping":mapping, "output_dataset": output_dataset, "functionObject":functionObject, "ckpt_interval": ckpt_interval,"partition_key":partition_key}
        self.epilogue(tasknode,channel_to_ip, tuple(ip_to_num_channel.keys()))
        return Dataset(output_dataset)
    
    def create(self):

        # launch a flight server actor on each machine
        launches = []
        private_ips = list(self.cluster.private_ips.values())
        for ip in private_ips:
            server = FlightServerWrapper.options(max_concurrency = 2, num_cpus = 0.001, resources = {"node:" + ip : 0.001}).remote("0.0.0.0", location = "grpc+tcp://0.0.0.0:5005")
            server.start_server.remote()

        launches = []
        for key in self.nodes:
            node = self.nodes[key]
            for channel in node:
                replica = node[channel]
                launches.append(replica.initialize.remote())
        ray.get(launches)
        
    def run(self):
        processes = []
        for key in self.nodes:
            node = self.nodes[key]
            for channel in node:
                replica = node[channel]
                processes.append(replica.execute.remote())
        unfinished = processes
        while True:
            finished, unfinished = ray.wait(unfinished, timeout = 1)
            #print(unfinished)
            if len(unfinished) == 0:
                break

    def run_with_fault_tolerance(self):
        processes_by_ip = {}
        process_to_actor = {}
        what_is_the_process = {}
        ip_set = set()
        for node_id in self.nodes:
            node = self.nodes[node_id]
            for channel in node:
                replica = node[channel]
                ip = self.node_channel_to_ip[node_id][channel]
                ip_set.add(ip)
                if ip not in processes_by_ip:
                    processes_by_ip[ip] = []
                bump = replica.execute.remote()
                processes_by_ip[ip].append(bump)
                process_to_actor[bump] = replica
                what_is_the_process[bump] = (node_id, channel)

        for ip in processes_by_ip:
            for process in processes_by_ip[ip]:
                print(ip, what_is_the_process[process])
        
        all_processes_by_ip = processes_by_ip.copy()

        cpu_time = {ip: time.time() for ip in processes_by_ip}

        all_ips = ip_set.copy()
        while len(ip_set) > 0:
            time.sleep(0.01) # be nice
            to_remove = set()
            to_add = set()
            for ip in ip_set:
                try:
                    finished, unfinished = ray.wait(processes_by_ip[ip], timeout = 1)
                    #print(finished, unfinished)
                    if len(unfinished) == 0:
                        to_remove.add(ip)
                        cpu_time[ip] = time.time() - cpu_time[ip]
                        print("ADDING ", ip , " TO TOREMOVE")
                    ray.get(finished)
                    # this ordering is very important. if we do this line before ray.get(finished), we might discard the actor who triggered the failure! If there is an actor failure, then we don't update the processes.
                    processes_by_ip[ip] = unfinished
                except ray.exceptions.RayActorError:
                    # let's assume that this entire machine is dead and there are no good things left on this machine, which is probably the case 
                    # if your spot instance got preempted
                    # reschedule all the channels stuck on this ip on a new machine
                    
                    # check if the machine is still reachable
                    # utils.check_instance_alive(ip -- redis works with prviate ips, just try to send it a redis set)
                    #new_private_ip = utils.launch_new_instance()

                    # now go ahead and try to kill all the actors on the old instance
                    print("WORKER AT ", ip, " HAS DIED. At least, an actor on it failed.")
                    print("=======================================================")
                    print("ATTEMPTING TO RESCHEDULE ITS WORK")
                    print("=======================================================")
                    cpu_time[ip] = time.time() - cpu_time[ip]
                    restarted_actors = {}
                    # this is a very subtle point. you need to reschedule all the actors, even the ones who are done. Because they might have to replay some of their logged outputs downstream
                    for old_process in all_processes_by_ip[ip]:
                        node, channel = what_is_the_process[old_process]
                        try:
                            restarted_actors[node].append(channel)
                        except:
                            restarted_actors[node] = [channel]                           
                        try:
                            ray.kill(process_to_actor[old_process])
                        except:
                            pass # don't get too mad if you can't kill something that's already dead

                    to_remove.add(ip)
                    all_ips.remove(ip)

                    # redistribute the dead stuff on the alive machines
                    # now that we have killed stuff, relaunch them on the new one.    
                    # go in increasing node order, which is by the way how the task graph must have been generated in the construction phase
                    # 
                    
                    helps = []
                    for node in sorted(restarted_actors.keys()):

                        affected_channels = restarted_actors[node]
                        node_type = self.node_type[node]
                        new_channel_to_ip = self.node_channel_to_ip[node].copy()
                        for channel in affected_channels:
                            new_ip = random.sample(all_ips,1)[0]
                            
                            # schedule everything on the same one
                            #new_ip = min(all_ips)

                            new_channel_to_ip[channel] = new_ip
                            to_add.add(new_ip)
                        self.node_channel_to_ip[node] = new_channel_to_ip
                        print("RESTARTING NODE",node, " NODE TYPE ", node_type, " ", new_channel_to_ip)
                        print("AFFECTED CHANNELS", affected_channels, ip_set)
                        if node_type == NONBLOCKING_NODE:
                            # this should have the new actor info, since node_parents refer to self.nodes, and the input nodes must have been updated since node number smaller
                            my_parents = self.node_parents[node] # all the channels should have the same parents
                            mapping = self.node_args[node]["mapping"]
                            partition_key = self.node_args[node]["partition_key"]
                            functionObject = self.node_args[node]["functionObject"]
                            ckpt_interval = self.node_args[node]["ckpt_interval"]
                            for source in my_parents:
                                ray.get([self.nodes[source][channel].append_to_targets.remote((node, new_channel_to_ip, partition_key[mapping[source]])) for channel in restarted_actors[source]])

                            for channel in affected_channels:
                                self.nodes[node][channel] = NonBlockingTaskNode.options(max_concurrency = 2, num_cpus = 0.001, resources={"node:" + new_channel_to_ip[channel] : 0.001}).remote(node, channel, 
                                        mapping,  functionObject, my_parents, (self.checkpoint_bucket , str(node) + "-" + str(channel)), checkpoint_interval = ckpt_interval, ckpt = 's3')
                                helps.append(self.nodes[node][channel].ask_upstream_for_help.remote(new_channel_to_ip[channel], len(new_channel_to_ip)))

                            for bump in self.nodes:
                                if bump in self.node_parents and node in self.node_parents[bump] and bump not in restarted_actors: # this node was not restarted. noone will apped_targets to you, do it yourself
                                    bump_partition_key = self.node_args[bump]["partition_key"][self.node_args[bump]["mapping"][node]]
                                    ray.get([self.nodes[node][channel].append_to_targets.remote((bump, self.node_channel_to_ip[bump], bump_partition_key)) for channel in affected_channels])
                        
                        elif node_type == BLOCKING_NODE:
                            # this should have the new actor info, since node_parents refer to self.nodes, and the input nodes must have been updated since node number smaller
                            my_parents = self.node_parents[node] # all the channels should have the same parents
                            mapping = self.node_args[node]["mapping"]
                            partition_key = self.node_args[node]["partition_key"]
                            output_dataset = self.node_args[node]["output_dataset"]
                            functionObject = self.node_args[node]["functionObject"]
                            ckpt_interval = self.node_args[node]["ckpt_interval"]
                            for source in my_parents:
                                ray.get([self.nodes[source][channel].append_to_targets.remote((node, new_channel_to_ip, partition_key[mapping[source]])) for channel in restarted_actors[source]])
                            for channel in affected_channels:
                                self.nodes[node][channel] = NonBlockingTaskNode.options(max_concurrency = 2, num_cpus = 0.001, resources={"node:" +new_channel_to_ip[channel] : 0.001}).remote(node, channel, 
                                    mapping, output_dataset, functionObject, my_parents, (self.checkpoint_bucket , str(node) + "-" + str(channel)), checkpoint_interval = ckpt_interval, ckpt = 's3')
                                helps.append(self.nodes[node][channel].ask_upstream_for_help.remote(new_channel_to_ip[channel], len(new_channel_to_ip)))
                            
                            for bump in self.nodes:
                                if bump in self.node_parents and node in self.node_parents[bump] and bump not in restarted_actors: # this node was not restarted. noone will apped_targets to you, do it yourself
                                    bump_partition_key = self.node_args[bump]["partition_key"][self.node_args[bump]["mapping"][node]]
                                    ray.get([self.nodes[node][channel].append_to_targets.remote((bump, self.node_channel_to_ip[bump], bump_partition_key)) for channel in affected_channels])
                        
                        elif node_type == INPUT_REDIS_DATASET:
                            channel_objects = self.node_args[node]["channel_objects"]
                            batch_func = self.node_args[node]["batch_func"]
                            dependent_map = self.node_args[node]["dependent_map"]
                            for channel in affected_channels:
                                self.nodes[node][channel] =  InputRedisDatasetNode.options(max_concurrency = 2, num_cpus=0.001, resources={"node:" + new_channel_to_ip[channel] : 0.001}
                                ).remote(node, channel, channel_objects, (self.checkpoint_bucket, str(node) + "-" + str(channel)), 
                                batch_func=batch_func, dependent_map=dependent_map, ckpt="s3")
                        
                        elif node_type == INPUT_READER_DATASET:
                            reader = self.node_args[node]["reader"]
                            batch_func = self.node_args[node]["batch_func"]
                            dependent_map = self.node_args[node]["dependent_map"]
                            ckpt_interval = self.node_args[node]["ckpt_interval"]
                            for channel in affected_channels:
                                self.nodes[node][channel] = InputReaderNode.options(max_concurrency = 2, num_cpus=0.001, resources={"node:" +new_channel_to_ip[channel] : 0.001}
                                    ).remote(node, channel, reader, len(new_channel_to_ip), (self.checkpoint_bucket, str(node) + "-" + str(channel)),
                                    batch_func = batch_func,dependent_map = dependent_map, checkpoint_interval = ckpt_interval, ckpt = "s3")

                        else:
                            raise Exception("what is this node? Can't do that yet")


                    ray.get(helps)
                    
                    for node in sorted(restarted_actors.keys()):
                        affected_channels = restarted_actors[node]
                        for channel in affected_channels:
                            new_ip = self.node_channel_to_ip[node][channel]
                            processes_by_ip[new_ip].append(self.nodes[node][channel].execute.remote())
                    
            for ip in to_remove:
                ip_set.remove(ip)
            for ip in to_add:
                ip_set.add(ip)
        
        print("TOTAL CPU TIME", sum(cpu_time.values()), cpu_time)
