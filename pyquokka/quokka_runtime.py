import redis
import ray
import polars
import time
from coordinator import Coordinator
from placement_strategy import CustomChannelsStrategy, PlacementStrategy, SingleChannelStrategy
from quokka_dataset import * 
from placement_strategy import * 
from target_info import * 
from core import *
from tables import * 
import sql_utils
from functools import partial

class TaskGraph:
    # this keeps the logical dependency DAG between tasks 
    def __init__(self, cluster, io_per_node = 2, exec_per_node = 1) -> None:

        self.io_per_node = io_per_node
        self.exec_per_node = exec_per_node
        self.cluster = cluster
        self.current_actor = 0
        self.actors = {}
        self.actor_placement_strategy = {}
        
        self.r = redis.Redis(host=str(self.cluster.leader_public_ip), port=6800, db=0)
        while True:
            try:
                _ = self.r.keys()
                break
            except redis.exceptions.BusyLoadingError:
                time.sleep(0.01)
        
        self.r.flushall()

        self.coordinator = Coordinator.remote()

        self.nodes = {}
        # for topological ordering
        self.actor_types = {}

        self.node_locs= {}
        self.io_nodes = set()
        self.compute_nodes = set()
        count = 0

        self.leader_compute_nodes = []

        # default strategy launches two IO nodes and one compute node per machine
        private_ips = list(self.cluster.private_ips.values())
        for ip in private_ips:
            
            for k in range(io_per_node):
                self.nodes[count] = IOTaskManager.options(num_cpus = 0.001, max_concurrency = 2, resources={"node:" + ip : 0.001}).remote(count, cluster.leader_private_ip, list(cluster.private_ips.values()))
                self.io_nodes.add(count)
                self.node_locs[count] = ip
                count += 1
            for k in range(exec_per_node):
                self.nodes[count] = ExecTaskManager.options(num_cpus = 0.001, max_concurrency = 2, resources={"node:" + ip : 0.001}).remote(count, cluster.leader_private_ip, list(cluster.private_ips.values()))
                
                if ip == self.cluster.leader_private_ip:
                    self.leader_compute_nodes.append(count)
                
                self.compute_nodes.add(count)
                self.node_locs[count] = ip
                count += 1        

        ray.get(self.coordinator.register_nodes.remote(io_nodes = {k: self.nodes[k] for k in self.io_nodes}, compute_nodes = {k: self.nodes[k] for k in self.compute_nodes}))
        ray.get(self.coordinator.register_node_ips.remote( self.node_locs ))

        self.FOT = FunctionObjectTable()
        self.CLT = ChannelLocationTable()
        self.NTT = NodeTaskTable()

    def get_total_channels_from_placement_strategy(self, placement_strategy, node_type):

        if type(placement_strategy) == SingleChannelStrategy:
            return 1
        elif type(placement_strategy) == CustomChannelsStrategy:
            return self.cluster.num_node * placement_strategy.channels_per_node * (self.io_per_node if node_type == 'input' else self.exec_per_node)
        else:
            raise Exception("strategy not supported")

    def epilogue(self, placement_strategy):
        self.actor_placement_strategy[self.current_actor] = placement_strategy
        self.current_actor += 1
        return self.current_actor - 1

    def new_input_reader_node(self, reader, placement_strategy = CustomChannelsStrategy(1)):

        self.actor_types[self.current_actor] = 'input'

        assert type(placement_strategy) == CustomChannelsStrategy
        
        if hasattr(reader, "get_own_state"):
            reader.get_own_state(self.get_total_channels_from_placement_strategy(placement_strategy, 'input'))
                
        self.FOT.set(self.r, self.current_actor, ray.cloudpickle.dumps(reader))

        count = 0
        channel_locs = {}
        for node in self.io_nodes:
            for channel in range(placement_strategy.channels_per_node):
                input_task = InputTask(self.current_actor, count, 0, None)
                channel_locs[count] = node
                count += 1
                self.NTT.rpush(self.r, node, input_task.reduce())
        
        ray.get(self.coordinator.register_actor_location.remote(self.current_actor, channel_locs))
        
        start = time.time()
        print("actor spin up took ", time.time() -start)

        return self.epilogue(placement_strategy)
    
    def get_default_partition(self, source_placement_strategy, target_placement_strategy):
        # this can get more sophisticated in the future. For now it's super dumb.
        
        def partition_key_0(ratio, data, source_channel, num_target_channels):
            target_channel = source_channel // ratio
            return {target_channel: data}
        def partition_key_1(ratio, data, source_channel, num_target_channels):
            # return entirety of data to a random channel between source_channel * ratio and source_channel * ratio + ratio
            target_channel = int(random.random() * ratio) + source_channel * ratio
            return {target_channel: data}
        
        assert type(source_placement_strategy) == CustomChannelsStrategy and type(target_placement_strategy) == CustomChannelsStrategy
        if source_placement_strategy.channels_per_node >= target_placement_strategy.channels_per_node:
            assert source_placement_strategy.channels_per_node % target_placement_strategy.channels_per_node == 0
            return partial(partition_key_0, source_placement_strategy.channels_per_node // target_placement_strategy.channels_per_node )
        else:
            assert target_placement_strategy.channels_per_node % source_placement_strategy.channels_per_node == 0
            return partial(partition_key_1, target_placement_strategy.channels_per_node // source_placement_strategy.channels_per_node)

    def prologue(self, streams, placement_strategy, source_target_info):

        def partition_key_str(key, data, source_channel, num_target_channels):
            result = {}
            for channel in range(num_target_channels):
                if type(data) == polars.internals.DataFrame:
                    if "int" in str(data[key].dtype).lower() or "float" in str(data[key].dtype).lower():
                        payload = data.filter(data[key] % num_target_channels == channel)
                    elif data[key].dtype == polars.datatypes.Utf8:
                        payload = data.filter(data[key].hash() % num_target_channels == channel)
                    else:
                        raise Exception("invalid partition column type, supports ints, floats and strs")
                else:
                    raise Exception("only support polars format")
                result[channel] = payload
            return result
        
        def broadcast(data, source_channel, num_target_channels):
            return {i: data for i in range(num_target_channels)}
        
        def partition_fn(predicate_fn, partitioner_fn, batch_funcs, projection, num_target_channels, x, source_channel):

            if predicate_fn != True:
                x = x.filter(predicate_fn(x))
            partitioned = partitioner_fn(x, source_channel, num_target_channels)
            results = {}
            for channel in partitioned:
                payload = partitioned[channel]
                for func in batch_funcs:
                    if payload is None or len(payload) == 0:
                        payload = None
                        break
                    payload = func(payload)

                if payload is None or len(payload) == 0:
                    continue
                if projection is not None:
                    results[channel] =  payload[sorted(list(projection))]
                else:
                    results[channel] = payload
            return results
        
        mapping = {}

        for key in streams:
            assert key in source_target_info
            source = streams[key]
            mapping[source] = key

            target_info = source_target_info[key]     
            target_info.predicate = sql_utils.evaluate(target_info.predicate)
            target_info.projection = target_info.projection

            # this has been provided
            if type(target_info.partitioner)== HashPartitioner:
                #print("Inferring hash partitioning strategy for column ", target_info.partitioner.key, "the source node must produce pyarrow, polars or pandas dataframes, and the dataframes must have this column!")
                target_info.partitioner = partial(partition_key_str, target_info.partitioner.key)
            elif type(target_info.partitioner) == BroadcastPartitioner:
                target_info.partitioner = broadcast
            elif type(target_info.partitioner) == FunctionPartitioner:
                from inspect import signature
                assert len(signature(target_info.partitioner.func).parameters) == 3, "custom partition function must accept three arguments: data object, source channel id, and the number of target channels"
                target_info.partitioner = target_info.partitioner.func
            elif type(target_info.partitioner) == PassThroughPartitioner:
                source = streams[key]
                target_info.partitioner = self.get_default_partition(self.actor_placement_strategy[source], placement_strategy)
            else:
                raise Exception("Partitioner not supported")
            
            target_info.lowered = True

            func = partial(partition_fn, target_info.predicate, target_info.partitioner, target_info.batch_funcs, target_info.projection, self.get_total_channels_from_placement_strategy(placement_strategy, 'exec'))

            registered = ray.get([node.register_partition_function.remote(source, self.current_actor, self.get_total_channels_from_placement_strategy(placement_strategy, 'exec'), func) for node in (self.nodes.values())])
            assert all(registered)

        
        registered = ray.get([node.register_mapping.remote(self.current_actor, mapping) for node in (self.nodes.values())])
        assert all(registered)

        source_actor_ids = []
        source_channel_ids = []
        min_seqs = []
        for source in mapping:
            num_channels = self.get_total_channels_from_placement_strategy(self.actor_placement_strategy[source], self.actor_types[source])
            source_actor_ids.extend([source] * num_channels)
            source_channel_ids.extend(range(num_channels))
            min_seqs.extend([0] * num_channels)
        input_reqs = polars.from_dict({"source_actor_id":source_actor_ids, "source_channel_id":source_channel_ids, "min_seq":min_seqs})

        return input_reqs

    def new_non_blocking_node(self, streams, functionObject, placement_strategy = CustomChannelsStrategy(1), source_target_info = {}):

        assert len(source_target_info) == len(streams)
        self.actor_types[self.current_actor] = 'exec'

        self.FOT.set(self.r, self.current_actor, ray.cloudpickle.dumps(functionObject))

        input_reqs = self.prologue(streams, placement_strategy, source_target_info)
        print("input_reqs",input_reqs)
        
        channel_locs = {}
        if type(placement_strategy) == SingleChannelStrategy:

            node = self.leader_compute_nodes[0]
            exec_task = ExecutorTask(self.current_actor, 0, 0, 0, input_reqs)
            channel_locs[0] = node
            self.NTT.rpush(self.r, node, exec_task.reduce())
            self.CLT.set(self.r, pickle.dumps((self.current_actor, 0)), self.node_locs[node])

        elif type(placement_strategy) == CustomChannelsStrategy:

            count = 0
            for node in self.compute_nodes:
                for channel in range(placement_strategy.channels_per_node):
                    exec_task = ExecutorTask(self.current_actor, count, 0, 0, input_reqs)
                    channel_locs[count] = node
                    self.NTT.rpush(self.r, node, exec_task.reduce())
                    self.CLT.set(self.r, pickle.dumps((self.current_actor, count)), self.node_locs[node])
                    count += 1
        else:
            raise Exception("placement strategy not supported")
        
        ray.get(self.coordinator.register_actor_location.remote(self.current_actor, channel_locs))
        
        return self.epilogue(placement_strategy)

    def new_blocking_node(self, streams, functionObject, placement_strategy = CustomChannelsStrategy(1), source_target_info = {}):
        
        current_actor = self.new_non_blocking_node(streams, functionObject, placement_strategy, source_target_info)
        self.actor_types[current_actor] = 'exec'
        total_channels = self.get_total_channels_from_placement_strategy(placement_strategy, 'exec')

        output_dataset = ArrowDataset.options(num_cpus = 0.001, resources={"node:" + str(self.cluster.leader_private_ip): 0.001}).remote(total_channels)

        registered = ray.get([node.register_blocking.remote(current_actor , output_dataset) for node in list(self.nodes.values())])
        assert all(registered)
        return Dataset(output_dataset)
    
    def create(self):

        initted = ray.get([node.init.remote() for node in list(self.nodes.values())])
        assert all(initted)

        # galaxy brain shit here -- the topological order is implicit given the way the API works. 
        topological_order = list(range(self.current_actor))[::-1]
        topological_order = [k for k in topological_order if self.actor_types[k] != 'input']
        print("topological_order", topological_order)
        ray.get(self.coordinator.register_actor_topo.remote(topological_order))
        
    def run(self):
        ray.get(self.coordinator.execute.remote())
