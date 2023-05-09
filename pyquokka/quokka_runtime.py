import redis
import ray
import polars
import time
from pyquokka.dataset import InputRayDataset
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
import ray.cloudpickle as pickle

class TaskGraph:
    # this keeps the logical dependency DAG between tasks 
    def __init__(self, context) -> None:
        
        self.context = context
        self.current_actor = 0
        self.actors = {}
        self.actor_placement_strategy = {}
        
        self.r = redis.Redis(host=str(self.context.cluster.leader_public_ip), port=6800, db=0)
        while True:
            try:
                _ = self.r.keys()
                break
            except:
                time.sleep(0.01)
        
        self.r.flushall()

        # for topological ordering
        self.actor_types = {}

        self.FOT = FunctionObjectTable()
        self.CLT = ChannelLocationTable()
        self.NTT = NodeTaskTable()
        self.IRT = InputRequirementsTable()
        self.LT = LineageTable()
        self.DST = DoneSeqTable()
        self.SAT = SortedActorsTable()
        self.PFT = PartitionFunctionTable()
        self.AST = ActorStageTable()
        self.LIT = LastInputTable()

        # progress tracking stuff
        self.input_partitions = {}

    def get_total_channels_from_placement_strategy(self, placement_strategy, node_type):

        if type(placement_strategy) == SingleChannelStrategy:
            return 1
        elif type(placement_strategy) == CustomChannelsStrategy:
            return self.context.cluster.num_node * placement_strategy.channels_per_node * (self.context.io_per_node if node_type == 'input' else self.context.exec_per_node)
        elif type(placement_strategy) == DatasetStrategy:
            return placement_strategy.total_channels
        else:
            print(placement_strategy)
            raise Exception("strategy not supported")

    def epilogue(self, stage, placement_strategy):
        self.AST.set(self.r, self.current_actor, stage)
        self.actor_placement_strategy[self.current_actor] = placement_strategy
        self.current_actor += 1
        return self.current_actor - 1

    def new_input_dataset_node(self, dataset, stage = 0):

        start = time.time()
        self.actor_types[self.current_actor] = 'input'

        # this will be a dictionary of ip addresses to ray refs
        objects_dict = dataset.to_dict()        
        # print(channel_info)
        self.input_partitions[self.current_actor] = sum([len(objects_dict[k]) for k in objects_dict])

        reader = InputRayDataset(objects_dict)

        pipe = self.r.pipeline()
        self.FOT.set(pipe, self.current_actor, ray.cloudpickle.dumps(reader))

        count = 0
        channel_locs = {}

        # print(objects_dict)

        for ip in objects_dict:
            assert ip in self.context.node_locs.values()
            nodes = [k for k in self.context.io_nodes if self.context.node_locs[k] == ip]
            objects_per_node = (len(objects_dict[ip]) - 1) // len(nodes) + 1
            for i in range(len(nodes)):
                node = nodes[i]
                objects = list(range(i * objects_per_node , min((i + 1) * objects_per_node, len(objects_dict[ip]))))
                if len(objects) == 0:
                    break
                # print(objects)
                lineages = [(ip, i) for i in objects]
                vals = {pickle.dumps((self.current_actor, count, seq)) : pickle.dumps(lineages[seq]) for seq in range(len(lineages))}
                input_task = TapedInputTask(self.current_actor, count, [i for i in range(len(lineages))])
                self.LIT.set(pipe, pickle.dumps((self.current_actor,count)), len(lineages) - 1)
                self.LT.mset(pipe, vals)
                self.NTT.rpush(pipe, node, input_task.reduce())
                channel_locs[count] = node
                count += 1
        
        pipe.execute()
        ray.get(self.context.coordinator.register_actor_location.remote(self.current_actor, channel_locs))

        return self.epilogue(stage, DatasetStrategy(len(channel_locs)))

    def new_input_reader_node(self, reader, stage = 0, placement_strategy = None):

        start = time.time()
        self.actor_types[self.current_actor] = 'input'
        if placement_strategy is None:
            placement_strategy = CustomChannelsStrategy(1)
        
        assert type(placement_strategy) in [SingleChannelStrategy, CustomChannelsStrategy]
        
        channel_info = reader.get_own_state(self.get_total_channels_from_placement_strategy(placement_strategy, 'input'))
        # print(channel_info)
        self.input_partitions[self.current_actor] = sum([len(channel_info[k]) for k in channel_info])

        pipe = self.r.pipeline()
        start = time.time()
        self.FOT.set(pipe, self.current_actor, ray.cloudpickle.dumps(reader))

        count = 0
        channel_locs = {}

        if type(placement_strategy) == SingleChannelStrategy:

            node = self.context.leader_io_nodes[0]
            lineages = channel_info[0]
            vals = {pickle.dumps((self.current_actor, count, seq)) : pickle.dumps(lineages[seq]) for seq in range(len(lineages))}
            input_task = TapedInputTask(self.current_actor, count, [i for i in range(len(lineages))])
            self.LIT.set(pipe, pickle.dumps((self.current_actor,count)), len(lineages) - 1)
            self.LT.mset(pipe, vals)
            self.NTT.rpush(pipe, node, input_task.reduce())
            channel_locs[count] = node
        
        elif type(placement_strategy) == CustomChannelsStrategy:
        
            for node in sorted(self.context.io_nodes):
                for channel in range(placement_strategy.channels_per_node):

                    if count in channel_info:
                        lineages = channel_info[count]
                    else:
                        lineages = []

                    if len(lineages) > 0:
                        vals = {pickle.dumps((self.current_actor, count, seq)) : pickle.dumps(lineages[seq]) for seq in range(len(lineages))}
                        input_task = TapedInputTask(self.current_actor, count, [i for i in range(len(lineages))])
                        self.LIT.set(pipe, pickle.dumps((self.current_actor,count)), len(lineages) - 1)
                        self.LT.mset(pipe, vals)
                        self.NTT.rpush(pipe, node, input_task.reduce())
                    
                    else:
                        # this channel doesn't have anything to do, mark it as done. DST for it must be set for the executors to terminate
                        self.DST.set(pipe, pickle.dumps((self.current_actor, count)), -1)

                    channel_locs[count] = node
                    count += 1
        pipe.execute()
        ray.get(self.context.coordinator.register_actor_location.remote(self.current_actor, channel_locs))

        return self.epilogue(stage, placement_strategy)
    
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
        

        if type(source_placement_strategy) == CustomChannelsStrategy:
            source_total_channels = self.get_total_channels_from_placement_strategy(source_placement_strategy, self.actor_types[source_node_id])
        elif type(source_placement_strategy) == DatasetStrategy:
            source_total_channels = source_placement_strategy.total_channels
        elif type(source_placement_strategy) == SingleChannelStrategy:
            source_total_channels = 1
        else:
            raise Exception("source strategy not supported")

        if type(target_placement_strategy) == CustomChannelsStrategy:
            target_total_channels = self.get_total_channels_from_placement_strategy(target_placement_strategy, "exec")
        elif type(target_placement_strategy) == SingleChannelStrategy:
            target_total_channels = 1
        else:
            raise Exception("target strategy not supported")
        
        if source_total_channels >= target_total_channels:
            return partial(partition_key_0, source_total_channels // target_total_channels )
        else:
            assert target_total_channels % source_total_channels == 0
            return partial(partition_key_1, target_total_channels // source_total_channels)

    def prologue(self, streams, placement_strategy, source_target_info):

        def partition_key_str(key, data, source_channel, num_target_channels):

            result = {}
            assert type(data) == polars.DataFrame
            if "int" in str(data[key].dtype).lower():
                partitions = data.with_columns(polars.Series(name="__partition__", values=(data[key] % num_target_channels))).partition_by("__partition__")
            elif data[key].dtype == polars.datatypes.Utf8 or data[key].dtype == polars.datatypes.Float32 or data[key].dtype == polars.datatypes.Float64:
                partitions = data.with_columns(polars.Series(name="__partition__", values=(data[key].hash() % num_target_channels))).partition_by("__partition__")
            else:
                print(data[key])
                raise Exception("partition key type not supported")
            for partition in partitions:
                target = partition["__partition__"][0]
                result[target] = partition.drop("__partition__")   
            return result
        

        def partition_key_range(key, total_range, data, source_channel, num_target_channels):

            per_channel_range = total_range // num_target_channels
            result = {}
            assert type(data) == polars.DataFrame
            partitions = data.with_columns(polars.Series(name="__partition__", values=((data[key] - 1) // per_channel_range))).partition_by("__partition__")
            for partition in partitions:
                target = partition["__partition__"][0]
                result[target] = partition.drop("__partition__")   
            return result 

        def broadcast(data, source_channel, num_target_channels):
            return {i: data for i in range(num_target_channels)}
        
        
        mapping = {}
        sources = []

        for key in streams:
            assert key in source_target_info
            source = streams[key]
            sources.append(source)
            mapping[source] = key

            target_info = source_target_info[key] 
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
            self.PFT.set(self.r, pickle.dumps((source, self.current_actor)), ray.cloudpickle.dumps(target_info))

            # TODO: figure out why you need to do this to make it work
            self.PFT.get(self.r, pickle.dumps((source, self.current_actor)))
        
        registered = ray.get([node.register_partition_function.remote(sources, self.current_actor, self.get_total_channels_from_placement_strategy(placement_strategy, 'exec'), mapping) for node in (self.context.task_managers.values())])
        assert all(registered)

        # consult the AST to figure out source stages
        # the result should be a list of dictionaries by stage

        sources = list(mapping.keys())
        stages = self.AST.mget(self.r, sources)
        assert all([i is not None for i in stages]), "source stages not found"
        stage_sources = {}
        for i in range(len(stages)):
            stage = int(stages[i])
            source = sources[i]
            if stage not in stage_sources:
                stage_sources[stage] = []
            stage_sources[stage].append(source)

        input_reqs = []

        for stage in sorted(stage_sources.keys()):
            # print(self.current_actor, stage)
            source_actor_ids = []
            source_channel_ids = []
            min_seqs = []
            for source in stage_sources[stage]:
                num_channels = self.get_total_channels_from_placement_strategy(self.actor_placement_strategy[source], self.actor_types[source])
                source_actor_ids.extend([source] * num_channels)
                source_channel_ids.extend(range(num_channels))
                min_seqs.extend([0] * num_channels)
            input_reqs.append(polars.from_dict({"source_actor_id":source_actor_ids, "source_channel_id":source_channel_ids, "min_seq":min_seqs}))

        return input_reqs

    def new_non_blocking_node(self, streams, functionObject, stage = 0, placement_strategy = CustomChannelsStrategy(1), source_target_info = {}, assume_sorted = {}):

        start = time.time()
        assert len(source_target_info) == len(streams)
        self.actor_types[self.current_actor] = 'exec'

        if placement_strategy is None:
            placement_strategy = CustomChannelsStrategy(1)
        
        pipe = self.r.pipeline()

        self.FOT.set(pipe, self.current_actor, ray.cloudpickle.dumps(functionObject))

        start = time.time()
        input_reqs = self.prologue(streams, placement_strategy, source_target_info)
        # print("input_reqs",self.current_actor, input_reqs)

        for key in assume_sorted:
            # first find how many channels that key has in input_reqs
            sorted_source_channels = len(polars.concat(input_reqs).filter(polars.col("source_actor_id") == key))
            assume_sorted[key] = sorted_source_channels
        
        if len(assume_sorted) > 0:
            self.SAT.set(pipe, self.current_actor, pickle.dumps(assume_sorted))

        channel_locs = {}
        if type(placement_strategy) == SingleChannelStrategy:

            node = self.context.leader_compute_nodes[0]
            exec_task = ExecutorTask(self.current_actor, 0, 0, 0, input_reqs)
            channel_locs[0] = node
            self.NTT.rpush(pipe, node, exec_task.reduce())
            self.CLT.set(pipe, pickle.dumps((self.current_actor, 0)), self.context.node_locs[node])
            self.IRT.set(pipe, pickle.dumps((self.current_actor, 0, -1)), pickle.dumps(input_reqs))

        elif type(placement_strategy) == CustomChannelsStrategy:

            count = 0
            for node in sorted(self.context.compute_nodes):
                for channel in range(placement_strategy.channels_per_node):
                    exec_task = ExecutorTask(self.current_actor, count, 0, 0, input_reqs)
                    channel_locs[count] = node
                    self.NTT.rpush(pipe, node, exec_task.reduce())
                    self.CLT.set(pipe, pickle.dumps((self.current_actor, count)), self.context.node_locs[node])
                    self.IRT.set(pipe, pickle.dumps((self.current_actor, count, -1)), pickle.dumps(input_reqs))
                    count += 1
        else:
            raise Exception("placement strategy not supported")
        
        pipe.execute()
        ray.get(self.context.coordinator.register_actor_location.remote(self.current_actor, channel_locs))

        return self.epilogue(stage, placement_strategy)

    def new_blocking_node(self, streams, functionObject, stage = 0, placement_strategy = CustomChannelsStrategy(1), source_target_info = {}, transform_fn = None, assume_sorted = {}):

        if placement_strategy is None:
            placement_strategy = CustomChannelsStrategy(1)

        current_actor = self.new_non_blocking_node(streams, functionObject, stage, placement_strategy, source_target_info, assume_sorted)
        self.actor_types[current_actor] = 'exec'
        dataset_id = ray.get(self.context.dataset_manager.create_dataset.remote())

        registered = ray.get([node.register_blocking.remote(current_actor , transform_fn, self.context.dataset_manager, dataset_id) for node in list(self.context.task_managers.values())])
        assert all(registered)
        return dataset_id
    
    def create(self):

        initted = ray.get([node.init.remote() for node in list(self.context.task_managers.values())])
        assert all(initted)

        # galaxy brain shit here -- the topological order is implicit given the way the API works. 
        topological_order = list(range(self.current_actor))[::-1]
        topological_order = [k for k in topological_order if self.actor_types[k] != 'input']
        # print("topological_order", topological_order)
        ray.get(self.context.coordinator.register_actor_topo.remote(topological_order))
        
    def run(self):

        from tqdm import tqdm

        execute_handle = self.context.coordinator.execute.remote()          

        input_partitions = {}
        pbars = {k: tqdm(total = self.input_partitions[k]) for k in self.input_partitions}
        curr_source = 0
        for k in pbars:
            pbars[k].set_description("Processing input source " + str(curr_source))
            input_partitions[k] = self.input_partitions[k]
            curr_source += 1

        while True:
            time.sleep(0.1)
            try:
                finished, unfinished = ray.wait([execute_handle], timeout= 0.01)
                if len(finished) > 0:
                    ray.get(execute_handle)
                    break
            except:
                raise Exception("Error in execution")
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
            # if not any(input_partitions.values()):
            #     break
