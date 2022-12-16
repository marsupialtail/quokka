import ray
import pickle
import redis
from pyquokka.task import * 
from pyquokka.tables import * 
import pyarrow
import pyarrow.flight
import polars
import time
import pandas as pd
import math

DEBUG =  True
def print_if_debug(*x):
    if DEBUG:
        print(*x)

@ray.remote
class Coordinator:
    def __init__(self) -> None:

        self.r = redis.Redis('localhost', 6800, db = 0)
        self.CT = CemetaryTable()
        self.NOT = NodeObjectTable()
        self.PT = PresentObjectTable()
        self.NTT = NodeTaskTable()
        self.GIT = GeneratedInputTable()
        self.EST = ExecutorStateTable()
        self.LT = LineageTable()
        self.DST = DoneSeqTable()
        self.LCT = LastCheckpointTable()
        self.CLT = ChannelLocationTable()
        self.IRT = InputRequirementsTable()

        self.undone = set()

        # input channel locations don't have to be tracked
        self.actor_channel_locations = {}
    
    def dump_redis_state(self, path):
        state = {"CT": self.CT.to_dict(self.r),
        "NOT": self.NOT.to_dict(self.r),
        "PT": self.PT.to_dict(self.r),
        "NTT": self.NTT.to_dict(self.r),
        "GIT": self.GIT.to_dict(self.r),
        "EST": self.EST.to_dict(self.r),
        "LT": self.LT.to_dict(self.r),
        "DST": self.DST.to_dict(self.r),
        "LCT": self.LCT.to_dict(self.r),
        "CLT": self.CLT.to_dict(self.r)}
        flight_client = pyarrow.flight.connect("grpc://0.0.0.0:5005")
        buf = pyarrow.allocate_buffer(0)
        action = pyarrow.flight.Action("get_flights_info", buf)
        result = next(flight_client.do_action(action))
        state["flights"] = pickle.loads(result.body.to_pybytes())[1]

        pickle.dump(state, open(path,"wb"))

    def register_actor_topo(self, topological_order):
        self.topological_order = topological_order

    def register_nodes(self, replay_nodes, io_nodes, compute_nodes):

        # this is going to be a dict of ip -> dict of node_id to actor_handle.
        self.replay_nodes = set(replay_nodes.keys())
        self.io_nodes = set(io_nodes.keys())
        self.compute_nodes = set(compute_nodes.keys())
        
        self.node_handles = {**replay_nodes, **io_nodes, **compute_nodes}
    
    def register_node_ips(self, node_ip_address):
        self.node_ip_address = node_ip_address
        self.ip_replay_node = {}
        for node in self.node_ip_address:
            if node in self.replay_nodes:
                self.ip_replay_node[self.node_ip_address[node]] = node

    def register_actor_location(self, actor_id, channel_to_node_id):
        self.actor_channel_locations[actor_id] = {}
        for channel_id in channel_to_node_id:
            node_id = channel_to_node_id[channel_id]
            self.actor_channel_locations[actor_id][channel_id] = node_id
            self.undone.add((actor_id, channel_id))

    def update_undone(self):
        # you only ever need the actor, channel pairs that have been registered in self.actor_flight_clients
        
        interested_pairs = [k for k in self.undone]
        
        for actor_id, channel_id in interested_pairs:
            seq = self.DST.get(self.r, pickle.dumps((actor_id, channel_id)))
            if seq is None:
                continue
            # print("done seq", seq)
            self.undone.remove((actor_id, channel_id))


    def execute(self):

        execute_handles = {worker : self.node_handles[worker].execute.remote() for worker in self.node_handles}
        execute_handles_list = list(execute_handles.values())
        
        while True:
            time.sleep(0.01)

            try:
                finished, unfinished = ray.wait(execute_handles_list, timeout= 0.01)
                execute_handles_list = unfinished
                ray.get(finished)

                self.update_undone()                
                if len(self.undone) == 0:
                    for worker in self.node_handles:
                        ray.kill(self.node_handles[worker])
                    break

            except ray.exceptions.RayActorError:
                print("detected failure")
                self.r.set("recovery-lock", 1)

                start = time.time()
                while True:
                    time.sleep(0.01)

                    failed_nodes = []
                    alive_nodes = []
                    for worker in execute_handles:
                        try:
                            ray.get(self.node_handles[worker].alive.remote())
                            alive_nodes.append(worker)
                        except:
                            failed_nodes.append(worker)
                        
                    # print("alive", alive_nodes)
                    # print("failed", failed_nodes)
                    for failed_node in failed_nodes:
                        ray.kill(self.node_handles[failed_node])

                    waiting_workers = [int(i) for i in self.r.smembers("waiting-workers")]
                    # print(waiting_workers)

                    # this guarantees that at this point, all the alive nodes are waiting. 
                    # note this does not guarantee that during recovery, all the alive nodes will stay alive, which might not be true.
                    # failed nodes will basically be forgotten about the system. 
                    if set(alive_nodes).issubset(set(waiting_workers)):
                        break
                
                print("WORKER BARRIER TOOK", time.time() - start)
                start = time.time()
                self.recover(alive_nodes, failed_nodes)
                self.r.set("recovery-lock", 0)
                self.r.delete("waiting-workers")
                print("RECOVERY PLANNING TOOK", time.time() - start)
                execute_handles = {worker: execute_handles[worker] for worker in alive_nodes}
                execute_handles_list = list(execute_handles.values())
            

    '''
    The strategy here is that we are going to guarantee that every current running task or future task will have inputs pushed to them.
    This will only update global data structures, it WILL NOT call any RPCs on running actors.
    When this function is executing, all alive task managers should be stuck in check_in_recovery() and thus are NOT sending requests or reading
    stuff from the the global data structures. As a result no locks are ever needed here, I assume you have exclusive access to the entire DB.
    '''

    def recover(self, alive_nodes, failed_nodes):

        def find_lastest_valid_ckpt(actor_id, channel_id, needed_state_seq):
            ckpt_seqs = self.LCT.lrange(self.r, pickle.dumps((actor_id, channel_id)), 0, -1)
            # in order to emit the output at state seq 10, it's insufficient to start at state 10! The next output will be associated with state 11
            valid_seqs = [pickle.loads(x) for x in ckpt_seqs if pickle.loads(x)[0] < needed_state_seq]
            if len(valid_seqs) > 0:
                rewind_ckpt = max(valid_seqs)
            else:
                rewind_ckpt = (-1, 0)
            return rewind_ckpt

        if DEBUG:
            self.dump_redis_state("pre.pkl")

        keys = self.EST.keys(self.r)
        # easy way to check if an actor_id is an executor or an input is check if it's in keys of this table.
        est = {pickle.loads(key): int(self.EST.get(self.r, key)) for key in keys}

        recovery_tasks = []
        for failed_node in failed_nodes:
            recovery_tasks.extend(self.NTT.lrange(self.r, failed_node, 0, -1))
        recovery_tasks = [pickle.loads(task) for task in recovery_tasks]
        replay_tasks = [ReplayTask.from_tuple(k[1]) for k in recovery_tasks if k[0] == "replay"]
        input_tasks = [InputTask.from_tuple(k[1]) for k in recovery_tasks if k[0] == "input"]
        inputtape_tasks = [TapedInputTask.from_tuple(k[1]) for k in recovery_tasks if k[0] == "inputtape"]
        exec_tasks = [ExecutorTask.from_tuple(k[1]) for k in recovery_tasks if k[0] == "exec"]
        exectape_tasks = [TapedExecutorTask.from_tuple(k[1]) for k in recovery_tasks if k[0] == "exectape"]

        needed_objects = []
        for task in replay_tasks:
            needed_objects.extend([pickle.dumps((task.actor_id, task.channel_id, seq)) for seq in task.needed_seqs])

        rewind_requests = {}
        new_input_requests = {}
        remembered_input_objects = {}
        replay_requests = []

        remembered_input_reqs = {}

        d = self.NTT.to_dict(self.r)
        for k in d:
            for tup in [i for i in d[k] if i[0] == "exec"]:
                task = ExecutorTask.from_tuple(tup[1])
                remembered_input_reqs[task.actor_id, task.channel_id] = task.input_reqs
        d = self.IRT.to_dict(self.r)
        for actor, task_id, seq in d:
            if (actor, task_id) in est and est[actor, task_id] == -1:
                remembered_input_reqs[actor,task_id] = d[actor, task_id, seq]


        # you can safely delete all objects this node stores UNLESS there is a replay task asking for it. 
        # other objects are purely for fault recovery. Instead of remaking them here, why not just remake them 
        # when another failure asks for them. Do as little work as possible!

        lost_objects = set.union( *[self.NOT.smembers(self.r, failed_node) for failed_node in failed_nodes ])
        for failed_node in failed_nodes:

            # might be None if these data structures are empty
            self.NOT.delete(self.r, failed_node)
            self.NTT.delete(self.r, failed_node)

        for object in lost_objects:
            if object in needed_objects:
                # the objects have to be reconstructed in case of executor node output or reread in case of input node output.
                # 1) they are not going to be made in the future, since no task should have been spawned that will make an existing object
                # 2) they are not going to be present elsewhere, since an object is only ever present on one node
                # yes currently if you need object seq 10 and you are on seq 20, you have to rewind all the way back to 10
                # this is to make sure there is only one task anywhere in the system executing a channel, even for different nonoverlapping seq numbers
                # this is mainly done to conserve memory. Even though you can totally make seq 10 and continue processing seq 20 onwards separately, you 
                # need two copies of the channel's state to do this. An alternative could be that we remember where we are at seq 20, and after we 
                # remake seq 10 fast forward to seq 20. This is too complicated right now.

                actor_id, channel_id, out_seq = pickle.loads(object)
                # this is an executor object
                if (actor_id, channel_id) in est:
                    needed_state_seq = int(self.LT.get(self.r, object))
                    assert needed_state_seq < int(est[actor_id, channel_id]), "something is wrong"
                    rewind_requests[actor_id, channel_id] = find_lastest_valid_ckpt(actor_id, channel_id, needed_state_seq)
                else:
                    if (actor_id, channel_id) not in new_input_requests:
                        new_input_requests[actor_id, channel_id] = {out_seq}
                    else:
                        new_input_requests[actor_id, channel_id].add(out_seq)

            assert self.PT.delete(self.r, object) == 1
        
        for task in exec_tasks:

            if (task.actor_id, task.channel_id) in rewind_requests:
                rewind_requests[task.actor_id, task.channel_id] = min(rewind_requests[task.actor_id, task.channel_id], find_lastest_valid_ckpt(task.actor_id, task.channel_id, task.state_seq))
            else:
                rewind_requests[task.actor_id, task.channel_id] = find_lastest_valid_ckpt(task.actor_id, task.channel_id, task.state_seq)
        
        for task in exectape_tasks:

            if (task.actor_id, task.channel_id) in rewind_requests:
                rewind_requests[task.actor_id, task.channel_id] = min(rewind_requests[task.actor_id, task.channel_id], find_lastest_valid_ckpt(task.actor_id, task.channel_id, task.state_seq))
            else:
                rewind_requests[task.actor_id, task.channel_id] = find_lastest_valid_ckpt(task.actor_id, task.channel_id, task.state_seq)
            
            # you don't have to record input_reqs since you will never need it. There never will be a scenario where you are a exectape task
            # and your last_known_seq == state_seq

        for task in input_tasks:

            remembered_input_objects[task.actor_id, task.channel_id] = (task.seq, task.input_object)
        
        for task in inputtape_tasks:

            if (task.actor_id, task.channel_id) not in new_input_requests:
                new_input_requests[task.actor_id, task.channel_id] = set([seq for seq in task.tape])
            else:
                for seq in task.tape:
                    new_input_requests[task.actor_id, task.channel_id].add(seq)


        # at the end of the recovery process, we have to ensure that 
        # 1) tasks running before on failed node must be running elsewhere
        # 2) all these tasks will have their inputs pushed to them. 

        # we are going backwards in topological order. This is super important! If the ordering is not right this won't work.
        for actor_id in self.topological_order:
            for channel_id in self.actor_channel_locations[actor_id]:
                assert actor_id, channel_id in est
                if (actor_id, channel_id) in rewind_requests:
                    rewinded_state_seq = rewind_requests[actor_id, channel_id][0]

                    if (actor_id, channel_id) in est:
                        current_state_seq = est[actor_id, channel_id]
                    else:
                        current_state_seq = -1

                    # if you failed right after a checkpoint, current_state_seq will be equal to rewinded_state_seq
                    assert current_state_seq >= rewinded_state_seq

                    required_inputs = {}
                    # for state_seq in range(rewinded_state_seq + 1, current_state_seq + 1):
                    #     name_prefix = pickle.dumps(('s', actor_id, channel_id, state_seq))
                    #     lineage = self.LT.get(self.r, name_prefix)
                    #     source_actor_id, source_channel_seqs = pickle.loads(lineage)
                    #     for source_channel_id in source_channel_seqs:
                    #         if (source_actor_id, source_channel_id) in required_inputs:
                    #             required_inputs[source_actor_id, source_channel_id].extend(source_channel_seqs[source_channel_id])
                    #         else:
                    #             required_inputs[source_actor_id, source_channel_id] = source_channel_seqs[source_channel_id]

                    # important bug fix: you must repush things that you haven't consumed yet. because they will be needed in the future
                    # otherwise deadlock.
                    # print(actor_id, channel_id, rewinded_state_seq)
                    for requirement in pickle.loads(self.IRT.get(self.r, pickle.dumps((actor_id, channel_id, rewinded_state_seq)))).to_dicts():
                        source_actor_id = requirement['source_actor_id']
                        source_channel_id = requirement["source_channel_id"]
                        min_seq = requirement["min_seq"]

                        # you will have to reproduce everything from min_seq, including min_seq all the way up to the last currently generated thing.
                        # exec node
                        if (source_actor_id, source_channel_id) in est:
                            # WARNING: TODO horribly inefficient. but simplest
                            relevant_keys = [pickle.loads(k) for k in self.LT.keys(self.r)]
                            relevant_keys = [key for key in relevant_keys if key[0] == source_actor_id and key[1] == source_channel_id]
                            if len(relevant_keys) > 0:
                                last_pushed_seq = max(relevant_keys)[2]
                                required_inputs[source_actor_id, source_channel_id] = [k for k in range(min_seq, last_pushed_seq + 1)]
                        # input node
                        else:
                            git = self.GIT.smembers(self.r, pickle.dumps((source_actor_id, source_channel_id)))
                            required_inputs[source_actor_id, source_channel_id] = range(min_seq, max([int(i) for i in git]) + 1)\
                                 if len(git) > 0 else []

                    for source_actor_id, source_channel_id in required_inputs:
                        input_seqs = required_inputs[source_actor_id, source_channel_id]
                        object_names = [pickle.dumps((source_actor_id, source_channel_id, seq)) for seq in input_seqs]
                        where = self.PT.mget(self.r, object_names)

                        if None in where:
                            # out of luck! currently just reconstruct everything.
                            if (source_actor_id, source_channel_id) in est:
                                # this is an executor
                                min_input_seq = min(input_seqs)
                                state_seq = int(self.LT.get(self.r, pickle.dumps((source_actor_id, source_channel_id, min_input_seq))))
                                if (source_actor_id, source_channel_id) in rewind_requests:
                                    rewind_requests[source_actor_id, source_channel_id] = min(rewind_requests[source_actor_id, source_channel_id], find_lastest_valid_ckpt(source_actor_id, source_channel_id, state_seq))
                                else:
                                    rewind_requests[source_actor_id, source_channel_id] = find_lastest_valid_ckpt(source_actor_id, source_channel_id, state_seq)
                            else:
                                # this is an input reader
                                for out_seq in input_seqs:
                                    if (source_actor_id, source_channel_id) not in new_input_requests:
                                        new_input_requests[source_actor_id, source_channel_id] = {out_seq}
                                    else:
                                        new_input_requests[source_actor_id, source_channel_id].add(out_seq)

                        else:
                            # you can replay all of them.
                            for seq, loc in zip(input_seqs, where):
                                replay_requests.append((source_actor_id, source_channel_id, loc, seq, actor_id, channel_id))
        

        print(rewind_requests)
        print(new_input_requests)
        print(replay_requests)
        print(remembered_input_objects)
        # print(remembered_input_reqs)

        for actor_id, channel_id in rewind_requests:

            # rewind to this state sequence number
            state_seq, next_out_seq = rewind_requests[actor_id, channel_id]
            last_known_seq = est[actor_id, channel_id] if (actor_id, channel_id) in est else -1

            # check if there is an alive node running this channel, if so kill it.
            node_id = self.actor_channel_locations[actor_id][channel_id]
            if node_id in alive_nodes:

                if last_known_seq <= state_seq:

                    # just let people do their thing, you will be covered!
                    continue
                else:

                    tasks = self.NTT.lrange(self.r, str(node_id), 0, -1)
                    for task_str in tasks:
                        name, tup = pickle.loads(task_str)
                        if name == "exec":
                            task = ExecutorTask.from_tuple(tup)
                            if task.actor_id == actor_id and task.channel_id == channel_id:

                                assert self.NTT.lrem(self.r, str(node_id),1 , task_str) == 1
                                break
                        
                        elif name == "exectape":
                            task = TapedExecutorTask.from_tuple(tup)
                            if task.actor_id == actor_id and task.channel_id == channel_id:

                                assert self.NTT.lrem(self.r, str(node_id),1 , task_str) == 1
                                last_known_seq = task.last_state_seq
                                break
            

            # must make sure you pick an ExecTaskManager not an IOTaskManager!
            alive_compute_nodes = [k for k in alive_nodes if k in self.compute_nodes]
            if len(alive_compute_nodes) == 0:
                print("Ran out of ExecTaskManagers to schedule work, fault recovery has failed most likely because of catastrophic number of server losses")
                exit()

            unlucky_one = random.choice(alive_compute_nodes)

            # the coordinator must NOT register the function object. Instead when a node realizes it doesn't have the function object it should go fetch it somewhere. 
            # the coordinator only ever touches the control data stores. It cannot do physical operations like RPCs!
            if last_known_seq == state_seq:
                # you are recovering right into a checkpoint
                self.NTT.lpush(self.r, unlucky_one, ExecutorTask(actor_id, channel_id, state_seq + 1, next_out_seq, remembered_input_reqs[actor_id, channel_id]).reduce())
            else:
                self.NTT.lpush(self.r, unlucky_one, TapedExecutorTask(actor_id, channel_id, state_seq + 1, next_out_seq, last_known_seq).reduce())

            self.actor_channel_locations[actor_id][channel_id] = unlucky_one
            self.CLT.set(self.r, pickle.dumps((actor_id, channel_id)), self.node_ip_address[unlucky_one])
            self.EST.set(self.r, pickle.dumps((actor_id, channel_id)), state_seq )

        ip_scores = {}
        ip_to_alive_io_nodes = {}
        alive_io_nodes = [k for k in alive_nodes if k in self.io_nodes]
        for io_node in alive_io_nodes:
            ip = self.node_ip_address[io_node]
            if ip not in ip_to_alive_io_nodes:
                ip_to_alive_io_nodes[ip] = [io_node]
                ip_scores[ip] = 0
            else:
                ip_to_alive_io_nodes[ip].append(io_node)

        for actor_id, channel_id in remembered_input_objects:
            seq, input_object = remembered_input_objects[actor_id, channel_id]
            alive_io_nodes = [k for k in alive_nodes if k in self.io_nodes]
            if len(alive_io_nodes) == 0:
                print("Ran out of IOTaskManagers to schedule work, fault recovery has failed most likely because of catastrophic number of server losses")
                exit()

            ip = min(ip_scores, key=ip_scores.get)
            ip_scores[ip] += 1
            unlucky_one = random.choice(ip_to_alive_io_nodes[ip])

            assert unlucky_one is not None
            
            self.NTT.lpush(self.r, unlucky_one, InputTask(actor_id, channel_id, seq, input_object).reduce())

        # now do the taped input tasks. This should pretty much be everything after the "Merge"

        actor_ids = set()
        for actor_id, channel_id in new_input_requests:
            if actor_id not in actor_ids:
                actor_ids.add(actor_id)
        
        # you want to interleave the alive io nodes by IP address so things are evenly balanced. How good are you at Python anyways?
        alive_io_nodes = [val for tup in zip(*list(ip_to_alive_io_nodes.values())) for val in tup]
        if len(alive_io_nodes) == 0:
            print("Ran out of IOTaskManagers to schedule work, fault recovery has failed most likely because of catastrophic number of server losses")
            exit()
        for actor_id in actor_ids:
            # rotate the list for every actor so the shit doesn't always end up on the first worker machine

            alive_io_nodes = alive_io_nodes[4:] + alive_io_nodes[:4]

            partitions = []
            for my_actor_id, channel_id in new_input_requests:
                if my_actor_id != actor_id:
                    continue
                for seq in new_input_requests[actor_id, channel_id]:
                    partitions.append((actor_id, channel_id, seq))
            partitions_per_node = [math.floor(len(partitions) / len(alive_io_nodes))] * len(alive_io_nodes)
            extras = len(partitions) - sum(partitions_per_node)
            for i in range(extras):
                partitions_per_node[i] += 1
            start = 0
            for k in range(len(alive_io_nodes)):
                my_stuff = partitions[start : start + partitions_per_node[k]]
                start += partitions_per_node[k]
                my_stuff = pd.DataFrame(my_stuff, columns = ["actor", "channel", "seq"])
                for tup, df in my_stuff.groupby(["actor", "channel"]):
                    a, c = tup
                    seqs = df.seq.to_list()
                    self.NTT.lpush(self.r, alive_io_nodes[k], TapedInputTask(int(a), int(c), [int(i) for i in seqs]).reduce())
        
        replay_requests = pd.DataFrame(replay_requests, columns = ['source_actor_id','source_channel_id','location','seq', 'target_actor_id', 'target_channel_id'])
        for location, location_df in replay_requests.groupby('location'):
            assert int(location) in alive_nodes, (location, alive_nodes)

            # find the replay node on that alive node
            ip = self.node_ip_address[int(location)]
            replay_node = self.ip_replay_node[ip]

            for tup, df in location_df.groupby(["source_actor_id", "source_channel_id"]):
                source_actor_id, source_channel_id = tup
                self.NTT.lpush(self.r, replay_node, ReplayTask(source_actor_id, source_channel_id, polars.from_pandas(df[["seq", "target_actor_id", "target_channel_id"]])).reduce())
        
        if DEBUG:
            self.dump_redis_state("post.pkl")