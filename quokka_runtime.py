from nodes import * 
import numpy as np
import pandas as pd
import time
import pickle
import utils as utils

ray.init(ignore_reinit_error=True) # do this locally
#ray.init("auto", ignore_reinit_error=True, runtime_env={"working_dir":"/home/ubuntu/quokka","excludes":["*.csv","*.tbl","*.parquet"]})
#ray.timeline("profile.json")

    
@ray.remote
class Dataset:

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
        return pd.concat(dfs)

class TaskGraph:
    # this keeps the logical dependency DAG between tasks 
    def __init__(self, checkpoint_bucket = "temp") -> None:
        self.current_node = 0
        self.nodes = {}
        self.node_channel_to_ip = {}
        self.node_ips = {}
        self.datasets = {}
        self.non_blocking_nodes = set()
        self.blocking_nodes = set()
        self.checkpoint_bucket = checkpoint_bucket
    
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

    def epilogue(self,tasknode, channel_to_ip, ips):
        self.nodes[self.current_node] = tasknode
        self.node_channel_to_ip[self.current_node] = channel_to_ip
        self.node_ips[self.current_node] = ips
        self.current_node += 1
        return self.current_node - 1

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

        tasknode = {}
        for channel in channel_to_ip:
            ip = channel_to_ip[channel]
            if ip != 'localhost':
                tasknode[channel] = InputRedisDatasetNode.options(num_cpus=0.001, resources={"node:" + ip : 0.001}
                ).remote(self.current_node, channel, channel_objects, (self.checkpoint_bucket, self.current_node + "-" + channel), batch_func=batch_func, dependent_map=dependent_map)
            else:
                tasknode[channel] = InputRedisDatasetNode.options(num_cpus=0.001,resources={"node:" + ray.worker._global_node.address.split(":")[0] : 0.001}
                ).remote(self.current_node, channel, channel_objects, (self.checkpoint_bucket, self.current_node + "-" + channel), batch_func=batch_func, dependent_map=dependent_map)
        
        return self.epilogue(tasknode,channel_to_ip, tuple(ip_to_num_channel.keys()))

    def new_input_csv(self, bucket, key, names, ip_to_num_channel, batch_func=None, sep = ",", dependents = [], stride= 1024 * 1024):
        
        dependent_map = self.return_dependent_map(dependents)
        channel_to_ip = self.flip_ip_channels(ip_to_num_channel)

        tasknode = {}
        for channel in channel_to_ip:
            ip = channel_to_ip[channel]
            if ip != 'localhost':
                tasknode[channel] = InputS3CSVNode.options(num_cpus=0.001, resources={"node:" + ip : 0.001}
                ).remote(self.current_node, channel, bucket,key,names, len(channel_to_ip), (self.checkpoint_bucket, self.current_node + "-" + channel),batch_func = batch_func,sep = sep, 
                stride= stride, dependent_map = dependent_map, )
            else:
                tasknode[channel] = InputS3CSVNode.options(num_cpus=0.001,resources={"node:" + ray.worker._global_node.address.split(":")[0] : 0.001}
                ).remote(self.current_node, channel, bucket,key,names, len(channel_to_ip), (self.checkpoint_bucket, self.current_node + "-" + channel), batch_func = batch_func, sep = sep,
                stride = stride, dependent_map = dependent_map, ) 
        
        return self.epilogue(tasknode,channel_to_ip, tuple(ip_to_num_channel.keys()))
    

    def new_input_multiparquet(self, bucket, key,  ip_to_num_channel, batch_func=None, columns = None, dependents = []):
        
        dependent_map = self.return_dependent_map(dependents)
        channel_to_ip = self.flip_ip_channels(ip_to_num_channel)

        tasknode = {}
        for channel in channel_to_ip:
            ip = channel_to_ip[channel]
            if ip != 'localhost':
                tasknode[channel] = InputS3MultiParquetNode.options(num_cpus=0.001, resources={"node:" + ip : 0.001}
                ).remote(self.current_node, channel, bucket,key,len(channel_to_ip), (self.checkpoint_bucket, self.current_node + "-" + channel), columns = columns,
                 batch_func = batch_func,dependent_map = dependent_map)
            else:
                tasknode[channel] = InputS3MultiParquetNode.options(num_cpus=0.001,resources={"node:" + ray.worker._global_node.address.split(":")[0] : 0.001}
                ).remote(self.current_node, channel, bucket,key,len(channel_to_ip), (self.checkpoint_bucket, self.current_node + "-" + channel),columns = columns,
                 batch_func = batch_func, dependent_map = dependent_map)
        
        return self.epilogue(tasknode,channel_to_ip, tuple(ip_to_num_channel.keys()))
    
    def new_non_blocking_node(self, streams, datasets, functionObject, ip_to_num_channel, partition_key, ckpt_interval = 10):
        
        channel_to_ip = self.flip_ip_channels(ip_to_num_channel)
        # this is the mapping of physical node id to the key the user called in streams. i.e. if you made a node, task graph assigns it an internal id #
        # then if you set this node as the input of this new non blocking task node and do streams = {0: node}, then mapping will be {0: the internal id of that node}
        mapping = {}
        # this is a dictionary of stream_id to the number of channels in that stream
        parents = {}
        for key in streams:
            source = streams[key]
            if source not in self.nodes:
                raise Exception("stream source not registered")
            ray.get([self.nodes[source][i].append_to_targets.remote((self.current_node, channel_to_ip, partition_key[key])) for i in self.nodes[source]])
            mapping[source] = key
            parents[source] = self.nodes[source]
        
        tasknode = {}
        for channel in channel_to_ip:
            ip = channel_to_ip[channel]
            if ip != 'localhost':
                tasknode[channel] = NonBlockingTaskNode.options(num_cpus = 0.001, resources={"node:" + ip : 0.001}).remote(self.current_node, channel, mapping, datasets, functionObject, 
                parents, (self.checkpoint_bucket , str(self.current_node) + "-" + str(channel)), checkpoint_interval = ckpt_interval)
            else:
                tasknode[channel] = NonBlockingTaskNode.options(num_cpus = 0.001, resources={"node:" + ray.worker._global_node.address.split(":")[0]: 0.001}).remote(self.current_node,
                 channel, mapping, datasets, functionObject, parents, (self.checkpoint_bucket , str(self.current_node) + "-" + str(channel)), checkpoint_interval = ckpt_interval)

        return self.epilogue(tasknode,channel_to_ip, tuple(ip_to_num_channel.keys()))

    def new_blocking_node(self, streams, datasets, functionObject, ip_to_num_channel, partition_key, ckpt_interval = 10):
        
        channel_to_ip = self.flip_ip_channels(ip_to_num_channel)
        mapping = {}
        parents = {}
        for key in streams:
            source = streams[key]
            if source not in self.nodes:
                raise Exception("stream source not registered")
            ray.get([self.nodes[source][i].append_to_targets.remote((self.current_node, channel_to_ip, partition_key[key])) for i in self.nodes[source]])
            mapping[source] = key
            parents[source] = self.nodes[source]
        
        # the datasets will all be managed on the head node. Note that they are not in charge of actually storing the objects, they just 
        # track the ids.
        output_dataset = Dataset.options(num_cpus = 0.001, resources={"node:" + ray.worker._global_node.address.split(":")[0]: 0.001}).remote(len(channel_to_ip))

        tasknode = {}
        for channel in channel_to_ip:
            ip = channel_to_ip[channel]
            if ip != 'localhost':
                tasknode[channel] = BlockingTaskNode.options(num_cpus = 0.001, resources={"node:" + ip : 0.001}).remote(self.current_node, channel, mapping, datasets, output_dataset, functionObject, 
                parents, (self.checkpoint_bucket , str(self.current_node) + "-" + str(channel)), checkpoint_interval = ckpt_interval)
            else:
                tasknode[channel] = BlockingTaskNode.options(num_cpus = 0.001, resources={"node:" + ray.worker._global_node.address.split(":")[0]: 0.001}).remote(self.current_node, 
                channel, mapping, datasets, output_dataset, functionObject, parents, (self.checkpoint_bucket , str(self.current_node) + "-" + str(channel)), checkpoint_interval = ckpt_interval)
        
        return self.epilogue(tasknode,channel_to_ip, tuple(ip_to_num_channel.keys()))
    
    def run(self):
        processes = []
        for key in self.nodes:
            node = self.nodes[key]
            for channel in node:
                replica = node[channel]
                processes.append(replica.execute.remote())
        ray.get(processes)

    def run_with_fault_tolerance(self):
        processes_by_ip = {}
        process_to_actor = {}
        what_is_the_process = {}
        for key in self.nodes:
            node = self.nodes[key]
            for channel in node:
                replica = node[channel]
                ip = self.node_channel_to_ip[node][channel]
                if ip not in processes_by_ip:
                    processes_by_ip[ip] = []
                bump = replica.execute.remote(channel)
                processes_by_ip[ip].append(bump)
                process_to_actor[bump] = replica
                what_is_the_process[bump] = (node, channel)

        done_ips = set()
        while True:
            time.sleep(0.001) # be nice
            for ip in processes_by_ip:
                if ip in done_ips:
                    continue
                try:
                    finished, unfinished = ray.wait(processes_by_ip[ip])
                    if len(unfinished) == 0:
                        done_ips.add(ip)
                    processes_by_ip[ip] = unfinished
                    for bump in finished:
                        ray.kill(process_to_actor[bump])
                except ray.exceptions.RayActorError:
                    # let's assume that this entire machine is dead and there are no good things left on this machine, which is probably the case 
                    # if your spot instance got preempted
                    # reschedule all the channels stuck on this ip on a new machine
                    
                    # in the future when we don't hand-input private IPs and can get a mapping between public and private ips
                    # check if the machine is still reachable
                    # utils.check_instance_alive(private_ip_to_public_ip(ip))

                    new_private_ip = utils.launch_new_instance()

                    # now go ahead and try to kill all the actors on the old instance

                    for old_process in processes_by_ip[ip]:
                        try:
                            ray.kill(process_to_actor[old_process])
                        except:
                            pass # don't get too mad if you can't kill something that's already dead

                    # now that we have killed stuff, relaunch them on the new one.      
                    new_processes = []  
                    for old_process in processes_by_ip[ip]:
                        node, channel = what_is_the_process
                        if node in self.non_blocking_nodes:
                            actor = NonBlockingTaskNode.options(num_cpus = 0.001, resources={"node:" + new_private_ip : 0.001}).remote((self.checkpoint_bucket,str(self.current_node) + "-" + str(channel)))                      
                        elif node in self.blocking_nodes:
                            actor = BlockingTaskNode.options(num_cpus = 0.001, resources={"node:" + new_private_ip : 0.001}).remote((self.checkpoint_bucket,str(self.current_node) + "-" + str(channel)))
                        else:
                            raise Exception("what is this node, an input node? Can't do that yet")
                        
                        ray.get(actor.initialize.remote(channel))
                        new_processes.append(actor.execute.remote(channel))
                        processes_by_ip[ip] = new_processes
                        # now you have to modify all of their original upstream and downstream

