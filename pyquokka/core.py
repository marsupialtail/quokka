import ray
import pyarrow
import pyarrow.flight
import pickle
import redis
from pyquokka.hbq import * 
from pyquokka.task import * 
from pyquokka.tables import * 
import polars
import time
import boto3
import types

CHECKPOINT_INTERVAL = None
MAX_SEQ = 1000000000
DEBUG = False
PROFILE = False
FT = False
MEM_LIMIT = 0.25
MAX_BATCHES = 5

def print_if_debug(*x):
    if DEBUG:
        print(*x)

def print_if_profile(*x):
    if PROFILE:
        print(*x, time.time())

def record_batches_to_polars(batches):

    aligned_batches = [pyarrow.record_batch([pyarrow.concat_arrays([arr]) for arr in batch], schema=batch.schema) for batch in batches]
    return polars.from_arrow(pyarrow.Table.from_batches(aligned_batches))

class ConnectionError(Exception):
    pass

'''

The names of objects are (source_actor_id, source_channel_id, seq, target_actor_id, target_actor_id, partition_fn, target_channel_id)
Each object can have only one producing task and one consuming task.
i.e. only one consuming task can be present on the cluster'''

'''
What is in the local cache, this is organized as primary key nodeid, channel -> set of values
This reflects how objects are actually stored in the cache.
An example: 
{(0,0) -> ((0,0,0),(1,0,0),(2,0,0)), (0,1) -> }
'''


class TaskManager:
    def __init__(self, node_id : int, coordinator_ip : str, worker_ips:list, hbq_path = "/data/") -> None:

        self.node_id = node_id
        self.mappings = {}
        self.function_objects = {}

        # key is source_actor_id, value is another dict of target_actor_id: fn
        self.partition_fns = {}
        self.target_count = {}

        self.blocking_nodes = {}

        self.dst = None

        # this defines how this object is going to push outputs
        self.flight_client = pyarrow.flight.connect("grpc://0.0.0.0:5005")
        self.clear_flights(self.flight_client)
        self.set_flight_configs(self.flight_client)
        self.flight_clients = {i: pyarrow.flight.connect("grpc://" + str(i) + ":5005") for i in worker_ips}

        #  Bytedance can write this.
        self.HBQ = HBQ(hbq_path)

        '''
        - Node Task Table (NTT): track the current tasks for each node. Key- node IP. Value - Set of tasks on that node. 
        - Lineage Table (LT): track the lineage of each object
        - PT delete lock: every time a node wants to delete from the PT (engage in GC), they need to check this lock is not acquired but the coordinator. It's like a shared lock. Two nodes can hold this lock at the same time, but no node can hold this lock if the coordinator has it. 
                
        '''

        if coordinator_ip == "localhost":
            print("Warning: coordinator_ip set to localhost. This only makes sense for local deployment.")

        self.r = redis.Redis(coordinator_ip, 6800, db = 0)
        self.CT = CemetaryTable()
        self.NOT = NodeObjectTable()
        self.PT = PresentObjectTable()
        self.NTT = NodeTaskTable()
        self.LT = LineageTable()
        self.DST = DoneSeqTable()
        self.CLT = ChannelLocationTable()
        self.FOT = FunctionObjectTable()

        # populate this dictionary from the initial assignment 
            
        self.self_flight_client = pyarrow.flight.connect("grpc://0.0.0.0:5005")

        # self.tasks = deque()

    def init(self):
        self.actor_flight_clients = {}
        keys = self.CLT.keys(self.r)
        values = self.CLT.mget(self.r, keys)
        for key, value in zip(keys, values):
            actor, channel = pickle.loads(key)
            ip = value.decode('utf-8')
            if actor in self.actor_flight_clients:
                self.actor_flight_clients[actor][channel] = self.flight_clients[ip]
            else:
                self.actor_flight_clients[actor] = {channel : self.flight_clients[ip]}
        return True

    def alive(self):
        return True

    def update_dst(self):
        # you only ever need the actor, channel pairs that have been registered in self.actor_flight_clients
        
        keys = self.DST.keys(self.r)
        seqs = [int(i) for i in self.DST.mget(self.r, keys)]
        actor_ids = [pickle.loads(k)[0] for k in keys]
        channel_ids = [pickle.loads(k)[1] for k in keys]

        if len(seqs) > 0:
            self.dst = polars.from_dict({"source_actor_id": actor_ids, "source_channel_id": channel_ids, "done_seq": seqs})

    
    def register_partition_function(self, source_actor_id, target_actor_id, number_target_channels, partition_function):

        # this will include the predicate and the projection before the actual partition function, and all the batch functions afterwards.
        # for the moment let's assume that no autoscaling happens and this doesn't change.

        if source_actor_id not in self.partition_fns:
            self.partition_fns[source_actor_id] = {target_actor_id: partition_function}
        else:
            self.partition_fns[source_actor_id][target_actor_id] = partition_function
        
        if source_actor_id not in self.target_count:
            self.target_count[source_actor_id] = number_target_channels
        else:
            self.target_count[source_actor_id] += number_target_channels
        
        return True
    
    def register_mapping(self, actor_id, mapping):
        self.mappings[actor_id] = mapping
        return True
    
    def register_blocking(self, actor_id, transform_fn, dataset_object):
        self.blocking_nodes[actor_id] = (transform_fn, dataset_object)
        return True
    
    def check_in_recovery(self):
        if self.r.get("recovery-lock") == b'1':
            print("Recovery request detected, I am going to wait ", self.node_id)
            self.r.sadd("waiting-workers",  self.node_id)
            while True:
                time.sleep(0.01)
                if self.r.get("recovery-lock") == b'0':
                    self.actor_flight_clients = {}

                    # update your routing table first!
                    keys = self.CLT.keys(self.r)
                    values = self.CLT.mget(self.r, keys)
                    for key, value in zip(keys, values):
                        actor, channel = pickle.loads(key)
                        ip = value.decode('utf-8')
                        # print("reset", actor, channel, ip)
                        if actor in self.actor_flight_clients:
                            self.actor_flight_clients[actor][channel] = self.flight_clients[ip]
                        else:
                            self.actor_flight_clients[actor] = {channel : self.flight_clients[ip]}
                    break
            print("exitted recovery loop", self.node_id)
    
    def clear_flights(self, client):
        buf = pyarrow.allocate_buffer(0)
        action = pyarrow.flight.Action("clear", buf)
        result = next(client.do_action(action))
        #print(result.body.to_pybytes().decode("utf-8"))
        if result.body.to_pybytes().decode("utf-8") != "True":
            print(result.body.to_pybytes().decode("utf-8"))
            raise Exception
    
    def set_flight_configs(self, client):
        message = pyarrow.py_buffer(pickle.dumps({"mem_limit" : MEM_LIMIT, "max_batches" : MAX_BATCHES}))
        action = pyarrow.flight.Action("set_configs", message)
        result = next(client.do_action(action))
        if result.body.to_pybytes().decode("utf-8") != "True":
            print(result.body.to_pybytes().decode("utf-8"))
            raise Exception

    def push(self, source_actor_id: int, source_channel_id: int, seq: int, output: polars.internals.DataFrame, target_mask = None, from_local = False):
        
        start_push = time.time()

        my_format = "polars" # let's not worry about different formats for now though that will be needed eventually
        
        if target_mask is not None:
            print_if_debug("TARGET_MASK", target_mask)
        
        partition_fns = self.partition_fns[source_actor_id]

        for target_actor_id in partition_fns:

            if target_mask is not None and target_actor_id not in target_mask:
                continue

            partition_fn = partition_fns[target_actor_id]
            # this will be a dict of channel -> Polars DataFrame

            if from_local:
                outputs = self.HBQ.get(source_actor_id, source_channel_id, seq, target_actor_id)
            else:

                start_part = time.time()
                outputs = partition_fn(output, source_channel_id)
                print_if_profile("partitioner time", time.time() - start_part)
                start_spill = time.time()
                if FT:
                    self.HBQ.put(source_actor_id, source_channel_id, seq, target_actor_id, outputs)
                print_if_profile("disk spill time", time.time() - start_spill)
            # wrap all the outputs with their actual names and by default persist all outputs

            # print(outputs)
            fake_row = outputs[list(outputs.keys())[0]][:0]
            expected_schema = outputs[list(outputs.keys())[0]].to_arrow().schema

            for target_channel_id in self.actor_flight_clients[target_actor_id]:

                start = time.time()

                if target_mask is not None and target_channel_id not in target_mask[target_actor_id]:
                    continue

                if target_channel_id in outputs and len(outputs[target_channel_id]) > 0:
                    data = outputs[target_channel_id]
                    # set the partition function number to 0 for now because we won't ever be changing the partition function.
                    name = (source_actor_id, source_channel_id, seq, target_actor_id, 0, target_channel_id)
                    batches = data.to_arrow().to_batches()
                    
                else:
                    name = (source_actor_id, source_channel_id, seq, target_actor_id, 0, target_channel_id)
                   
                    batches = [pyarrow.RecordBatch.from_pandas(fake_row.to_pandas(), schema = expected_schema)]
                    # print(fake_row.schema, fake_row.to_pandas(), batches[0].schema)
                    # print(name, batches, fake_row.to_pandas())

                assert len(batches) == 1, batches
                # spin here until you can finally push it. If you die while spinning, well yourself will be recovered.
                # this is fine since you don't have the recovery lock.
                client = self.actor_flight_clients[target_actor_id][target_channel_id]

                print_if_debug("pushing", source_actor_id, source_channel_id, seq, target_actor_id, target_channel_id, len(batches[0]))

                try:
                    if not self.check_puttable(client):
                        return False
                    upload_descriptor = pyarrow.flight.FlightDescriptor.for_command(pickle.dumps((True, name, my_format)))
                    batch = batches[0]
                    writer, _ = client.do_put(upload_descriptor, batch.schema)
                    # print("Attempting to write ", batch.nbytes, "bytes")
                    if batch.nbytes >= 2e9:
                        for k in range(0, len(batch), len(batch)//10):
                            writer.write_batch(batch[k : k + len(batch) // 10])
                    else:
                        writer.write_batch(batch)
                    writer.close()

                except pyarrow._flight.FlightUnavailableError:
                    # failed to push to cache, probably because downstream node failed. update actor_flight_clients
                    print("downstream unavailable")
                    return False
                
                print_if_debug("finished pushing", source_actor_id, source_channel_id, seq, target_actor_id, target_channel_id, len(batches[0]))

                print_if_profile("pushing to one channel time", time.time() - start)

        print_if_profile("push time", time.time() - start_push)
        return True
        
    
    def execute(self):
        raise NotImplementedError

    #The task node periodically will run a garbage collection process for its local cache as well as its HBQ. 
    # this can be run every X iterations in the main loop
    def garbage_collect(self):

        gcable = []
        
        transaction = self.r.pipeline()
        for source_actor_id, source_channel_id, seq, target_actor_id in self.HBQ.get_objects():

            # refer to the comment of the cemetary table in tables.py to understand this logic.
            # basically count is the number of objects in the flight server with the right name prefix, which should be total number of target object slices
            if self.CT.scard(self.r, pickle.dumps(source_actor_id, source_channel_id, seq)) == self.target_count[source_actor_id]:
                
                gcable.append((source_actor_id, source_channel_id, seq, target_actor_id))
                self.NOT.srem(transaction, self.node_id, pickle.dumps((source_actor_id, source_channel_id, seq)))
                self.PT.delete(transaction, pickle.dumps((source_actor_id, source_channel_id, seq)))
                        
        assert all(transaction.execute())
        self.HBQ.gc(gcable)

    def task_commit(self, transaction, candidate_task, next_task):

        if next_task is not None:
            # now we need to actually reflect this information in the global data structures
            # note when we put task info in the NTT, the info needs to include the entire task, including all the future tasks it could launch. i.e. the tape in taped tasks.
            self.NTT.lrem(transaction, str(self.node_id), 1, candidate_task.reduce())
            self.NTT.rpush(transaction, str(self.node_id), next_task.reduce())
            
        else:
            # this task is not spawning off more tasks, it's considered done
            # most likely it got done from all its input sources
            self.NTT.lrem(transaction, str(self.node_id), 1, candidate_task.reduce())
        

@ray.remote
class ExecTaskManager(TaskManager):
    def __init__(self, node_id: int, coordinator_ip: str, worker_ips: list, checkpoint_bucket = "quokka-checkpoint") -> None:
        super().__init__(node_id, coordinator_ip, worker_ips)
        self.LCT = LastCheckpointTable()
        self.EST = ExecutorStateTable()
        self.IRT = InputRequirementsTable()

        if checkpoint_bucket is not None:
            self.checkpoint_bucket = checkpoint_bucket
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(checkpoint_bucket)
            bucket.objects.all().delete()

        self.tape_input_reqs = {}
    
    def check_puttable(self, client):
        buf = pyarrow.allocate_buffer(0)
        action = pyarrow.flight.Action("check_puttable", buf)
        result = next(client.do_action(action))
        #print(result.body.to_pybytes().decode("utf-8"))
        if result.body.to_pybytes().decode("utf-8") != "True":
            print("BACKPRESSURING!")
            return False
        else:
            return True
    
    def output_commit(self, transaction, actor_id, channel_id, out_seq, lineage):

        if FT:
            name_prefix = pickle.dumps((actor_id, channel_id, out_seq))
            
            self.NOT.sadd(transaction, str(self.node_id), name_prefix)
            self.PT.set(transaction, name_prefix, str(self.node_id))
            # this probably doesn't have to be done transactionally, but why not.

            self.LT.set(transaction, name_prefix, lineage)
        else:
            pass
    
    def state_commit(self, transaction, actor_id, channel_id, state_seq, lineage):

        if FT:

            name_prefix = pickle.dumps(('s', actor_id, channel_id, state_seq))
            self.LT.set(transaction, name_prefix, lineage)
        else:
            pass

    def process_output(self, actor_id, channel_id, output, transaction, state_seq, out_seq):
        if output is not None:
            assert type(output) == polars.internals.DataFrame or type(output) == types.GeneratorType
            if type(output) == polars.internals.DataFrame:
                output = [output]
            
            for data in output:
                if actor_id not in self.blocking_nodes:
                    pushed = self.push(actor_id, channel_id, out_seq, data)
                    if not pushed:
                        # you failed to push downstream, most likely due to node failure. wait a bit for coordinator recovery and continue, most like will be choked on barrier.
                        time.sleep(0.2)
                        return -1
                else:
                    transform_fn, dataset = self.blocking_nodes[actor_id]
                    if transform_fn is not None:
                        data = transform_fn(data)
                    ray.get(dataset.added_object.remote(channel_id, [ray.put(data.to_arrow(), _owner = dataset)]))
                self.output_commit(transaction, actor_id, channel_id, out_seq, state_seq)

                out_seq += 1
        return out_seq

    def execute(self):

        pyarrow.set_cpu_count(8)
        count = -1
        self.index = 0
        while True:

            self.check_in_recovery()

            count += 1
            length = self.NTT.llen(self.r, str(self.node_id))
            if length == 0:
                continue

            candidate_tasks = self.NTT.lrange(self.r, str(self.node_id), 0, -1)
            # exec_tape_task = False
            # for candidate_task in candidate_tasks:
            #     task_type, tup = pickle.loads(candidate_task)
            #     # prioritize recovery tasks
            #     if task_type == "exectape":
            #         exec_tape_task = True
            #         break
            # if not exec_tape_task:
            if count > length - 1:
                count = count % length
            candidate_task = candidate_tasks[count]
            task_type, tup = pickle.loads(candidate_task)
        
            if task_type == "input" or task_type == "inputtape" or task_type == "replay":
                raise Exception("unsupported task type", task_type)

            elif task_type == "exec":
                candidate_task = ExecutorTask.from_tuple(tup)

                actor_id = candidate_task.actor_id
                channel_id = candidate_task.channel_id

                if (actor_id, channel_id) not in self.function_objects:
                    self.function_objects[actor_id, channel_id] = ray.cloudpickle.loads(self.FOT.get(self.r, actor_id))
                    if candidate_task.state_seq > 0:
                        s3 = boto3.client("s3")
                        print("RESTORING TO ", candidate_task.state_seq -1 )
                        self.function_objects[actor_id, channel_id].restore(self.checkpoint_bucket, actor_id, channel_id, candidate_task.state_seq - 1)

                input_requirements = candidate_task.input_reqs

                self.update_dst()
                
                if self.dst is not None:
                    input_requirements = input_requirements.join(self.dst, on = ["source_actor_id", "source_channel_id"], how = "left")\
                                                    .fill_null(MAX_SEQ)\
                                                    .filter(polars.col("min_seq") <= polars.col("done_seq"))\
                                                    .drop("done_seq")
                
                # print(input_requirements)
                
                transaction = self.r.pipeline()

                out_seq = candidate_task.out_seq
                state_seq = candidate_task.state_seq

                input_names = []
                if len(input_requirements) > 0:

                    request = ("cache", actor_id, channel_id, input_requirements, False)

                    # if we can bake logic inside the the flight server we probably should, because that will be baked into C++ at some point.

                    reader = self.flight_client.do_get(pyarrow.flight.Ticket(pickle.dumps(request)))

                    # we are going to assume the Flight server gives us results sorted by source_actor_id
                    
                    chunks_list = []
                    names = []
                    while True:
                        try:
                            chunk, metadata = reader.read_chunk()
                            name, format = pickle.loads(metadata)
                            assert format == "polars"
                            if len(names) == 0 or name != names[-1]:
                                chunks_list.append([chunk])
                                names.append(name)
                            else:
                                chunks_list[-1].append(chunk)
                                print("creating multi-chunk")

                        except StopIteration:
                            break

                    batches = []
                    source_actor_ids = set()
                    source_channel_ids = []
                    source_channel_seqs = {}
                    for chunks, name in zip(chunks_list, names):
                        source_actor_id, source_channel_id, seq, target_actor_id, partition_fn, target_channel_id = name

                        # important bug fix: you must ignore objects whose names are not in LineageTable. This means they have not yet
                        # been committed upstream, which means their lineage could potentially change upon reconstruction!
                        #  we need to guarantee that the resulting batches are still contiguous in terms of their sequence numbers
                        # this is true because only the last batch for every source channel can still be uncommitted.

                        if FT and self.LT.get(self.r, pickle.dumps((source_actor_id, source_channel_id, seq))) is None:
                            print_if_debug("SKIPPING UNCOMMITED STUFF ", source_actor_id, source_channel_id, seq)
                            continue

                        input_names.append(name)
                        
                        source_actor_ids.add(source_actor_id)
                        if source_channel_id in source_channel_seqs:
                            source_channel_seqs[source_channel_id].append(seq)
                        else:
                            source_channel_seqs[source_channel_id] = [seq]

                        batches.append(chunks)
                    
                    if len(batches) == 0:
                        self.index += 1
                        continue

                    assert len(source_actor_ids) == 1
                    source_actor_id = source_actor_ids.pop()

                    input = [record_batches_to_polars(batch) for batch in batches if sum([len(b) for b in batch]) > 0]

                    start = time.time()

                    if len(input) > 0:
                        output, _ , _ = candidate_task.execute(self.function_objects[actor_id, channel_id], input, self.mappings[actor_id][source_actor_id] , channel_id)
                    else:
                        output = None
                    
                    if hasattr(self.function_objects[actor_id, channel_id], 'update_sources'):
                        remaining_source_actor_ids = input_requirements["source_actor_id"].unique().to_list()
                        remaining_sources = set([self.mappings[actor_id][source_actor_id] for source_actor_id in remaining_source_actor_ids])
                        self.function_objects[actor_id, channel_id].update_sources(remaining_sources)

                    print_if_profile("execute time", time.time() - start)

                    source_channel_ids = [i for i in source_channel_seqs]
                    source_channel_progress = [len(source_channel_seqs[i]) for i in source_channel_ids]
                    progress = polars.from_dict({"source_actor_id": [source_actor_id] * len(source_channel_ids) , "source_channel_id": source_channel_ids, "progress": source_channel_progress})
                    # progress is guaranteed to have something since len(batches) > 0
                    
                    print_if_debug("progress", progress)
                    
                    new_input_reqs = input_requirements.join(progress, on = ["source_actor_id", "source_channel_id"], how = "left").fill_null(0)
                    new_input_reqs = new_input_reqs.with_column(polars.Series(name = "min_seq", values = new_input_reqs["progress"] + new_input_reqs["min_seq"]))
                    
                    print_if_debug(new_input_reqs)
                    new_input_reqs = new_input_reqs.drop("progress")

                    out_seq = self.process_output(actor_id, channel_id, output, transaction, state_seq, out_seq)
                    if out_seq == -1:
                        continue
                        
                    next_task = ExecutorTask(actor_id, channel_id, state_seq + 1, out_seq, new_input_reqs)

                else:
                    output = self.function_objects[actor_id, channel_id].done(channel_id)

                    out_seq = self.process_output(actor_id, channel_id, output, transaction, state_seq, out_seq)
                    if out_seq == -1:
                        continue
                    last_output_seq = out_seq - 1
                    # print("DONE", actor_id, channel_id)
                    self.DST.set(self.r, pickle.dumps((actor_id, channel_id)), last_output_seq)
                            
                    next_task = None

                if CHECKPOINT_INTERVAL is not None and state_seq % CHECKPOINT_INTERVAL == 0:
                    self.function_objects[actor_id, channel_id].checkpoint(self.checkpoint_bucket, actor_id, channel_id, state_seq)
                    for source_channel_id in source_channel_ids:
                        for seq in source_channel_seqs[source_channel_id]:
                            self.CT.sadd(transaction, pickle.dumps((source_actor_id, source_channel_id, seq)), pickle.dumps((actor_id, channel_id)))

                    self.LCT.rpush(transaction, pickle.dumps((actor_id, channel_id)), pickle.dumps((state_seq, out_seq)))
                    self.IRT.set(transaction, pickle.dumps((actor_id, channel_id, state_seq)), pickle.dumps(new_input_reqs))
                # this way of logging the lineage probably use less space than a Polars table actually.                        

                self.EST.set(transaction, pickle.dumps((actor_id, channel_id)), state_seq)                    
                lineage = pickle.dumps((source_actor_id, source_channel_seqs))
                self.state_commit(transaction, actor_id, channel_id, state_seq, lineage)
                self.task_commit(transaction, candidate_task, next_task)
                
                executed = transaction.execute()
                #if not all(executed):
                #    raise Exception(executed)
                
                if len(input_names) > 0:
                    message = pyarrow.py_buffer(pickle.dumps(input_names))
                    action = pyarrow.flight.Action("cache_garbage_collect", message)
                    result = next(self.flight_client.do_action(action))
                    assert result.body.to_pybytes().decode("utf-8") == "True"
            
            elif task_type == "exectape":
                candidate_task = TapedExecutorTask.from_tuple(tup)
                actor_id = candidate_task.actor_id
                channel_id = candidate_task.channel_id
                state_seq = candidate_task.state_seq

                if (actor_id, channel_id) not in self.function_objects:
                    self.function_objects[actor_id, channel_id] = ray.cloudpickle.loads(self.FOT.get(self.r, actor_id))
                    assert candidate_task.state_seq >= 0

                    if candidate_task.state_seq > 0:
                        print("RESTORING TO ", state_seq -1 )
                        self.function_objects[actor_id, channel_id].restore(self.checkpoint_bucket, actor_id, channel_id, state_seq - 1)
                    
                    new_input_reqs = pickle.loads(self.IRT.get(self.r, pickle.dumps((actor_id, channel_id, state_seq - 1))))
                    assert new_input_reqs is not None
                    assert (actor_id, channel_id) not in self.tape_input_reqs
                    self.tape_input_reqs[actor_id, channel_id] = new_input_reqs

                name_prefix = pickle.dumps(('s', actor_id, channel_id, state_seq))
                input_requirements = self.LT.get(self.r, name_prefix)
                assert input_requirements is not None, pickle.loads(name_prefix)
                
                request = ("cache", actor_id, channel_id, input_requirements, True)
                reader = self.flight_client.do_get(pyarrow.flight.Ticket(pickle.dumps(request)))

                chunks_list = []
                names = []
                while True:
                    try:
                        chunk, metadata = reader.read_chunk()
                        name, format = pickle.loads(metadata)
                        assert format == "polars"
                        if len(names) == 0 or name != names[-1]:
                            chunks_list.append([chunk])
                            names.append(name)
                        else:
                            chunks_list[-1].append(chunk)
                            print("creating multi-chunk")

                    except StopIteration:
                        break

                # we are going to assume the Flight server gives us results sorted by source_actor_id
                batches = chunks_list
                input_names = names

                if len(batches) == 0:
                    self.index += 1
                    continue

                source_actor_id, source_channel_seqs = pickle.loads(input_requirements)
                source_channel_ids = list(source_channel_seqs.keys())
                source_channel_progress = [len(source_channel_seqs[k]) for k in source_channel_ids]
                progress = polars.from_dict({"source_actor_id": [source_actor_id] * len(source_channel_ids) , "source_channel_id": source_channel_ids, "progress": source_channel_progress})

                new_input_reqs = self.tape_input_reqs[actor_id, channel_id].join(progress, on = ["source_actor_id", "source_channel_id"], how = "left").fill_null(0)
                new_input_reqs = new_input_reqs.with_column(polars.Series(name = "min_seq", values = new_input_reqs["progress"] + new_input_reqs["min_seq"]))
                self.tape_input_reqs[actor_id, channel_id] = new_input_reqs.select(["source_actor_id", "source_channel_id","min_seq"])

                input = [record_batches_to_polars(batch) for batch in batches if sum([len(b) for b in batch]) > 0]
                
                if len(input) > 0:
                    output, state_seq, out_seq = candidate_task.execute(self.function_objects[actor_id, channel_id], input, self.mappings[actor_id][source_actor_id] , channel_id)
                else:
                    state_seq = candidate_task.state_seq
                    out_seq = candidate_task.out_seq
                    output = None
                
                transaction = self.r.pipeline()
                    
                out_seq = self.process_output(actor_id, channel_id, output, transaction, state_seq, out_seq)
                if out_seq == -1:
                    continue
                    
                if state_seq + 1 > candidate_task.last_state_seq:
                    next_task = ExecutorTask(actor_id, channel_id, state_seq + 1, out_seq, self.tape_input_reqs[actor_id, channel_id])
                    print("finished exectape, next input requirement is ", new_input_reqs)

                else:
                    next_task = TapedExecutorTask(actor_id, channel_id, state_seq + 1, out_seq, candidate_task.last_state_seq)

                # this way of logging the lineage probably use less space than a Polars table actually.

                self.EST.set(transaction, pickle.dumps((actor_id, channel_id)), state_seq)
                self.task_commit(transaction, candidate_task, next_task)
                
                executed = transaction.execute()
                #if not all(executed):
                #    raise Exception(executed)
            
                message = pyarrow.py_buffer(pickle.dumps(input_names))
                action = pyarrow.flight.Action("cache_garbage_collect", message)
                result = next(self.flight_client.do_action(action))
                assert result.body.to_pybytes().decode("utf-8") == "True"


@ray.remote
class IOTaskManager(TaskManager):
    def __init__(self, node_id: int, coordinator_ip: str, worker_ips: list) -> None:
        super().__init__(node_id, coordinator_ip, worker_ips)
        self.GIT = GeneratedInputTable()
        self.delay = 0.1

    # while the exectaskmanager should error out and proceed with the next task 
    # if check puttable is not true to relieve pressure on itself, inputs should just be held up.
    def check_puttable(self, client):
        delayed = False
        while True:
            buf = pyarrow.allocate_buffer(0)
            action = pyarrow.flight.Action("check_puttable", buf)
            result = next(client.do_action(action))
            #print(result.body.to_pybytes().decode("utf-8"))
            if result.body.to_pybytes().decode("utf-8") != "True":
                print("BACKPRESSURING!")
                # be nice
                if not delayed:
                    self.delay += 1
                    delayed = True
                print(self.delay)
                time.sleep(self.delay)
                continue
            else:
                return True
    
    def output_commit(self, transaction, actor_id, channel_id, out_seq, lineage):

        if FT:
            name_prefix = pickle.dumps((actor_id, channel_id, out_seq))
            
            self.NOT.sadd(transaction, str(self.node_id), name_prefix)
            self.PT.set(transaction, name_prefix, str(self.node_id))
            # this probably doesn't have to be done transactionally, but why not.
            # lineage can be None for taped tasks, since no need to put lineage anymore.
            if lineage is not None:
                self.LT.set(transaction, name_prefix, lineage)
            self.GIT.sadd(transaction, pickle.dumps((actor_id, channel_id)), out_seq)

    def execute(self):
        """
        Let's start with having one functionObject object for each channel.
        This might need to duplicated data etc. future optimization.
        """
    
        pyarrow.set_cpu_count(8)
        count = -1
        while True:
            time.sleep(self.delay)
            if self.delay - 0.1 > 0.1:
                self.delay -= 0.1
            self.check_in_recovery()

            count += 1
            length = self.NTT.llen(self.r, str(self.node_id))
            if length == 0:
                continue 

            candidate_tasks = self.NTT.lrange(self.r, str(self.node_id), 0, -1)
            candidate_task = random.sample(candidate_tasks,1 )[0]
            task_type, tup = pickle.loads(candidate_task)

            if task_type == "input":
                
                candidate_task = InputTask.from_tuple(tup)
                actor_id = candidate_task.actor_id
                channel_id = candidate_task.channel_id

                if (actor_id, channel_id) not in self.function_objects:
                    self.function_objects[actor_id, channel_id] = ray.cloudpickle.loads(self.FOT.get(self.r, actor_id))

                functionObject = self.function_objects[actor_id, channel_id]
                
                start = time.time()

                next_task, output, seq, lineage = candidate_task.execute(functionObject)

                print_if_profile("read time", time.time() - start)

                if next_task is None:
                    # print("DONE", actor_id, channel_id)
                    self.DST.set(self.r, pickle.dumps((actor_id, channel_id)), seq)
            
            elif task_type == "inputtape":
                candidate_task = TapedInputTask.from_tuple(tup)
                actor_id = candidate_task.actor_id
                channel_id = candidate_task.channel_id
                
                if (actor_id, channel_id) not in self.function_objects:
                    self.function_objects[actor_id, channel_id] = ray.cloudpickle.loads(self.FOT.get(self.r, actor_id))
                
                start = time.time()

                functionObject = self.function_objects[actor_id, channel_id]
                seq = candidate_task.tape[0]
                input_object = pickle.loads(self.LT.get(self.r, pickle.dumps((actor_id, channel_id, seq))))
                print_if_profile("lineage  time", time.time() - start)
                start = time.time()

                next_task, output, seq, lineage = candidate_task.execute(functionObject, input_object)
                print_if_profile("read  time", time.time() - start)
            
            else:
                raise Exception("unsupported task type", task_type)       

            pushed = self.push(actor_id, channel_id, seq, output)

            if pushed:
                transaction = self.r.pipeline()
                self.output_commit(transaction, actor_id, channel_id, seq, lineage)
                self.task_commit(transaction, candidate_task, next_task)
                if not all(transaction.execute()):
                   print("COMMITING TRANSACTION FAILED")
                   # raise Exception
            
            # downstream failure detected, will start recovery soon, DO NOT COMMIT!
            else:
                print("push failed!")
                # sleep for 0.2 seconds, since recovery happens every 0.1 seconds
                time.sleep(0.2)
                continue

@ray.remote
class ReplayTaskManager(TaskManager):
    def __init__(self, node_id: int, coordinator_ip: str, worker_ips: list) -> None:
        super().__init__(node_id, coordinator_ip, worker_ips)
    
    def replay(self, source_actor_id, source_channel_id, plan):

        # plan is going to be a polars dataframe with three columns: seq, target_actor, target_channel

        seqs = plan['seq'].unique().to_list()
        
        for seq in seqs:

            targets = plan.filter(polars.col('seq') == seq).select(["target_actor_id", "target_channel_id"])
            target_mask_list = targets.groupby('target_actor_id').agg_list().to_dicts()
            target_mask = {k['target_actor_id'] : k['target_channel_id'] for k in target_mask_list}

            # figure out a pythonic way to convert a dataframe to a dict of lists

            pushed = self.push(source_actor_id, source_channel_id, seq, None, target_mask, True)
            if not pushed:
                return False
        
        return True

    def execute(self):
        """
        Let's start with having one functionObject object for each channel.
        This might need to duplicated data etc. future optimization.
        """
    
        pyarrow.set_cpu_count(8)
        count = -1
        while True:
            self.check_in_recovery()

            count += 1
            length = self.NTT.llen(self.r, str(self.node_id))
            if length == 0:
                continue 

            candidate_task = self.NTT.lindex(self.r, str(self.node_id), 0)
            task_type, tup = pickle.loads(candidate_task)
            assert task_type == "replay"
            
            print_if_debug("executing replay")

            candidate_task = ReplayTask.from_tuple(tup)
            
            replayed = self.replay(candidate_task.actor_id, candidate_task.channel_id, candidate_task.replay_specification)
            if replayed:
                self.NTT.lrem(self.r, str(self.node_id), 1, candidate_task.reduce())
            else:
                print("replay failed!")
                time.sleep(0.2)