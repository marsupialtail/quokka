import redis
import ray
import polars
import time
from pyquokka.coordinator import Coordinator
from pyquokka.placement_strategy import *
from pyquokka.quokka_dataset import * 
from pyquokka.placement_strategy import * 
from pyquokka.target_info import * 
from pyquokka.core import *
from pyquokka.tables import * 
from pyquokka.utils import LocalCluster, EC2Cluster
import pyquokka.sql_utils as sql_utils
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

        self.coordinator = Coordinator.options(num_cpus=0.001, max_concurrency = 2,resources={"node:" + str(self.cluster.leader_private_ip): 0.001}).remote()

        self.nodes = {}
        # for topological ordering
        self.actor_types = {}

        self.node_locs= {}
        self.io_nodes = set()
        self.compute_nodes = set()
        self.replay_nodes = set()
        count = 0

        self.leader_compute_nodes = []

        # default strategy launches two IO nodes and one compute node per machine
        private_ips = list(self.cluster.private_ips.values())
        for ip in private_ips:
            
            for k in range(1):
                self.nodes[count] = ReplayTaskManager.options(num_cpus = 0.001, max_concurrency = 2, resources={"node:" + ip : 0.001}).remote(count, cluster.leader_private_ip, list(cluster.private_ips.values()))
                self.replay_nodes.add(count)
                self.node_locs[count] = ip
                count += 1
            for k in range(io_per_node):
                self.nodes[count] = IOTaskManager.options(num_cpus = 0.001, max_concurrency = 2, resources={"node:" + ip : 0.001}).remote(count, cluster.leader_private_ip, list(cluster.private_ips.values()))
                self.io_nodes.add(count)
                self.node_locs[count] = ip
                count += 1
            for k in range(exec_per_node):
                if type(self.cluster) == LocalCluster:
                    self.nodes[count] = ExecTaskManager.options(num_cpus = 0.001, max_concurrency = 2, resources={"node:" + ip : 0.001}).remote(count, cluster.leader_private_ip, list(cluster.private_ips.values()), None)
                elif type(self.cluster) == EC2Cluster:
                    self.nodes[count] = ExecTaskManager.options(num_cpus = 0.001, max_concurrency = 2, resources={"node:" + ip : 0.001}).remote(count, cluster.leader_private_ip, list(cluster.private_ips.values()), "quokka-checkpoint") 
                else:
                    raise Exception

                if ip == self.cluster.leader_private_ip:
                    self.leader_compute_nodes.append(count)
                
                self.compute_nodes.add(count)
                self.node_locs[count] = ip
                count += 1        

        ray.get(self.coordinator.register_nodes.remote(replay_nodes = {k: self.nodes[k] for k in self.replay_nodes}, io_nodes = {k: self.nodes[k] for k in self.io_nodes}, compute_nodes = {k: self.nodes[k] for k in self.compute_nodes}))
        ray.get(self.coordinator.register_node_ips.remote( self.node_locs ))

        self.FOT = FunctionObjectTable()
        self.CLT = ChannelLocationTable()
        self.NTT = NodeTaskTable()
        self.IRT = InputRequirementsTable()
        self.LT = LineageTable()
        self.DST = DoneSeqTable()

        # progress tracking stuff
        self.input_partitions = {}

    def get_total_channels_from_placement_strategy(self, placement_strategy, node_type):

        if type(placement_strategy) == SingleChannelStrategy:
            return 1
        elif type(placement_strategy) == CustomChannelsStrategy:
            return self.cluster.num_node * placement_strategy.channels_per_node * (self.io_per_node if node_type == 'input' else self.exec_per_node)
        else:
            print(placement_strategy)
            import pdb; pdb.set_trace()
            raise Exception("strategy not supported")

    def epilogue(self, placement_strategy):
        self.actor_placement_strategy[self.current_actor] = placement_strategy
        self.current_actor += 1
        return self.current_actor - 1

    def new_input_reader_node(self, reader, placement_strategy = None):

        self.actor_types[self.current_actor] = 'input'
        if placement_strategy is None:
            placement_strategy = CustomChannelsStrategy(1)
        assert type(placement_strategy) == CustomChannelsStrategy
        
        channel_info = reader.get_own_state(self.get_total_channels_from_placement_strategy(placement_strategy, 'input'))
        # print(channel_info)
        self.input_partitions[self.current_actor] = sum([len(channel_info[k]) for k in channel_info])

        pipe = self.r.pipeline()
        start = time.time()
        self.FOT.set(pipe, self.current_actor, ray.cloudpickle.dumps(reader))

        count = 0
        channel_locs = {}
        
        for node in sorted(self.io_nodes):
            for channel in range(placement_strategy.channels_per_node):

                if count in channel_info:
                    lineages = channel_info[count]
                else:
                    lineages = []

                if len(lineages) > 0:
                    vals = {pickle.dumps((self.current_actor, count, seq)) : pickle.dumps(lineages[seq]) for seq in range(len(lineages))}

                    input_task = TapedInputTask(self.current_actor, count, [i for i in range(len(lineages))])
                    self.LT.mset(pipe, vals)
                    self.NTT.rpush(pipe, node, input_task.reduce())

                self.DST.set(pipe, pickle.dumps((self.current_actor, count)), len(lineages) - 1)
                channel_locs[count] = node
                count += 1
        pipe.execute()
        ray.get(self.coordinator.register_actor_location.remote(self.current_actor, channel_locs))

        return self.epilogue(placement_strategy)
    
    def get_default_partition(self, source_node_id, target_placement_strategy):
        # this can get more sophisticated in the future. For now it's super dumb.
        
        def partition_key_0(ratio, data, source_channel, num_target_channels):
            target_channel = source_channel // ratio
            return {target_channel: data}
        def partition_key_1(ratio, data, source_channel, num_target_channels):
            # return entirety of data to a random channel between source_channel * ratio and source_channel * ratio + ratio
            target_channel = int(random.random() * ratio) + source_channel * ratio
            return {target_channel: data}

        source_placement_strategy = self.actor_placement_strategy[source_node_id]
        
        assert type(source_placement_strategy) == CustomChannelsStrategy and type(target_placement_strategy) == CustomChannelsStrategy
        source_total_channels = self.get_total_channels_from_placement_strategy(source_placement_strategy, self.actor_types[source_node_id])
        target_total_channels = self.get_total_channels_from_placement_strategy(target_placement_strategy, "exec")
        
        if source_total_channels >= target_total_channels:
            assert source_total_channels % target_total_channels == 0
            return partial(partition_key_0, source_total_channels // target_total_channels )
        else:
            assert target_total_channels % source_total_channels == 0
            return partial(partition_key_1, target_total_channels // source_total_channels)

    def prologue(self, streams, placement_strategy, source_target_info):

        def partition_key_str(key, data, source_channel, num_target_channels):

            result = {}
            assert type(data) == polars.internals.DataFrame
            if "int" in str(data[key].dtype).lower():
                partitions = data.with_column(polars.Series(name="__partition__", values=(data[key] % num_target_channels))).partition_by("__partition__")
            elif data[key].dtype == polars.datatypes.Utf8:
                partitions = data.with_column(polars.Series(name="__partition__", values=(data[key].hash() % num_target_channels))).partition_by("__partition__")
            else:
                raise Exception("partition key type not supported")
            for partition in partitions:
                target = partition["__partition__"][0]
                result[target] = partition.drop("__partition__")   
            return result
        

        def partition_key_range(key, total_range, data, source_channel, num_target_channels):

            per_channel_range = total_range // num_target_channels
            result = {}
            assert type(data) == polars.internals.DataFrame
            partitions = data.with_column(polars.Series(name="__partition__", values=((data[key] - 1) // per_channel_range))).partition_by("__partition__")
            for partition in partitions:
                target = partition["__partition__"][0]
                result[target] = partition.drop("__partition__")   
            return result 

        def broadcast(data, source_channel, num_target_channels):
            return {i: data for i in range(num_target_channels)}
        
        def partition_fn(predicate_fn, partitioner_fn, batch_funcs, projection, num_target_channels, x, source_channel):

            start = time.time()
            if predicate_fn != sqlglot.exp.TRUE:
                x = x.filter(predicate_fn(x))
            # print("filter time", time.time() - start)

            start = time.time()
            partitioned = partitioner_fn(x, source_channel, num_target_channels)
            # print("partition fn time", time.time() - start)

            results = {}
            for channel in partitioned:
                payload = partitioned[channel]
                for func in batch_funcs:
                    if payload is None:
                        break
                    payload = func(payload)

                if payload is None:
                    continue

                start = time.time()
                if projection is not None:
                    results[channel] =  payload[sorted(list(projection))]
                else:
                    results[channel] = payload
                # print("selection time", time.time() - start)
            return results
        
        mapping = {}

        for key in streams:
            assert key in source_target_info
            source = streams[key]
            mapping[source] = key

            target_info = source_target_info[key] 
            if target_info.predicate != sqlglot.exp.TRUE:
                target_info.predicate = sql_utils.evaluate(target_info.predicate)
            target_info.projection = target_info.projection

            # this has been provided
            if type(target_info.partitioner)== HashPartitioner:
                #print("Inferring hash partitioning strategy for column ", target_info.partitioner.key, "the source node must produce pyarrow, polars or pandas dataframes, and the dataframes must have this column!")
                target_info.partitioner = partial(partition_key_str, target_info.partitioner.key)
            elif type(target_info.partitioner) == RangePartitioner:
                target_info.partitioner = partial(partition_key_range, target_info.partitioner.key, target_info.partitioner.total_range)
            elif type(target_info.partitioner) == BroadcastPartitioner:
                target_info.partitioner = broadcast
            elif type(target_info.partitioner) == PassThroughPartitioner:
                source = streams[key]
                target_info.partitioner = self.get_default_partition(source, placement_strategy)
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

        if placement_strategy is None:
            placement_strategy = CustomChannelsStrategy(1)
        
        pipe = self.r.pipeline()

        self.FOT.set(pipe, self.current_actor, ray.cloudpickle.dumps(functionObject))

        input_reqs = self.prologue(streams, placement_strategy, source_target_info)
        # print("input_reqs",input_reqs)
        
        channel_locs = {}
        if type(placement_strategy) == SingleChannelStrategy:

            node = self.leader_compute_nodes[0]
            exec_task = ExecutorTask(self.current_actor, 0, 0, 0, input_reqs)
            channel_locs[0] = node
            self.NTT.rpush(pipe, node, exec_task.reduce())
            self.CLT.set(pipe, pickle.dumps((self.current_actor, 0)), self.node_locs[node])
            self.IRT.set(pipe, pickle.dumps((self.current_actor, 0, -1)), pickle.dumps(input_reqs))

        elif type(placement_strategy) == CustomChannelsStrategy:

            count = 0
            for node in sorted(self.compute_nodes):
                for channel in range(placement_strategy.channels_per_node):
                    exec_task = ExecutorTask(self.current_actor, count, 0, 0, input_reqs)
                    channel_locs[count] = node
                    self.NTT.rpush(pipe, node, exec_task.reduce())
                    self.CLT.set(pipe, pickle.dumps((self.current_actor, count)), self.node_locs[node])
                    self.IRT.set(pipe, pickle.dumps((self.current_actor, count, -1)), pickle.dumps(input_reqs))
                    count += 1
        else:
            raise Exception("placement strategy not supported")
        
        pipe.execute()
        ray.get(self.coordinator.register_actor_location.remote(self.current_actor, channel_locs))
        
        return self.epilogue(placement_strategy)

    def new_blocking_node(self, streams, functionObject, placement_strategy = CustomChannelsStrategy(1), source_target_info = {}, transform_fn = None):

        if placement_strategy is None:
            placement_strategy = CustomChannelsStrategy(1)

        current_actor = self.new_non_blocking_node(streams, functionObject, placement_strategy, source_target_info)
        self.actor_types[current_actor] = 'exec'
        total_channels = self.get_total_channels_from_placement_strategy(placement_strategy, 'exec')

        output_dataset = ArrowDataset.options(num_cpus = 0.001, resources={"node:" + str(self.cluster.leader_private_ip): 0.001}).remote(total_channels)
        ray.get(output_dataset.ping.remote())

        registered = ray.get([node.register_blocking.remote(current_actor , transform_fn, output_dataset) for node in list(self.nodes.values())])
        assert all(registered)
        return Dataset(output_dataset)
    
    def create(self):

        initted = ray.get([node.init.remote() for node in list(self.nodes.values())])
        assert all(initted)

        # galaxy brain shit here -- the topological order is implicit given the way the API works. 
        topological_order = list(range(self.current_actor))[::-1]
        topological_order = [k for k in topological_order if self.actor_types[k] != 'input']
        # print("topological_order", topological_order)
        ray.get(self.coordinator.register_actor_topo.remote(topological_order))
        
    def run(self):

        def progress():
            from tqdm import tqdm

            input_partitions = {}
            pbars = {k: tqdm(total = self.input_partitions[k]) for k in self.input_partitions}
            curr_source = 0
            for k in pbars:
                pbars[k].set_description("Processing input source " + str(curr_source))
                input_partitions[k] = self.input_partitions[k]
                curr_source += 1

            while True:
                time.sleep(0.5)
                current_partitions = {k : 0 for k in input_partitions}
                ntt = self.NTT.to_dict(self.r)
                ios = {k: ntt[k] for k in ntt if 'input' in str(ntt[k])}
                for node in ios:
                    for task in ios[node]:
                        actor_id = task[1][0]
                        current_partitions[actor_id] += len(task[1][2])
                for k in current_partitions:
                    pbars[k].update(input_partitions[k] - current_partitions[k])
                input_partitions = current_partitions
                if not any(input_partitions.values()):
                    break


        import threading
        if not PROFILE:
            new_thread = threading.Thread(target = progress)
            new_thread.start()
        ray.get(self.coordinator.execute.remote())
        if not PROFILE:
            new_thread.join()