import numpy as np
import pandas as pd
import ray
from collections import deque, OrderedDict
from dataset import InputCSVDataset
import pickle
import os
import redis
from threading import Lock
import time
# isolated simplified test bench for different fault tolerance protocols

ray.init(ignore_reinit_error=True)


NUM_MAPPERS = 2
NUM_JOINS = 4

class Node:

    # will be overridden
    def __init__(self) -> None:
        raise NotImplementedError

    def append_to_targets(self,tup):
        node_id,  partition_key = tup
        
        self.target_partition_fn[node_id] = partition_key
        self.target_output_state[node_id] = 0

    def help_downstream_recover(self, target, target_out_seq_state):

        # send logged outputs 
        print("HELP RECOVER",target,target_out_seq_state)
        self.output_lock.acquire()
        pipeline = self.r.pipeline()
        for key in self.logged_outputs:
            if key > target_out_seq_state:
                print("RESENDING", key)
                pipeline.publish("mailbox-"+str(target),pickle.dumps(self.target_partition_fn[target](self.logged_outputs[key],target)))
                pipeline.publish("mailbox-id-"+str(target),pickle.dumps((self.id, key)))
        results = pipeline.execute()
        if False in results:
            raise Exception
        self.output_lock.release()

    def truncate_logged_outputs(self, target, target_ckpt_state):
        
        print("STARTING TRUNCATE", target, target_ckpt_state, self.target_output_state)
        old_min = min(self.target_output_state.values())
        self.target_output_state[target] = target_ckpt_state
        new_min = min(self.target_output_state.values())

        self.output_lock.acquire()
        if new_min > old_min:
            for key in range(old_min, new_min):
                if key in self.logged_outputs:
                    print("REMOVING KEY",key,"FROM LOGGED OUTPUTS")
                    self.logged_outputs.pop(key)
        self.output_lock.release()    

    # reliably log state tag
    def log_state_tag(self):
        assert self.r.rpush("state-tag-" + str(self.id), pickle.dumps(self.state_tag))

    def push(self, data):
        
        self.out_seq += 1

        self.output_lock.acquire()
        self.logged_outputs[self.out_seq] = data
        self.output_lock.release()

        for target in self.target_partition_fn:
            
            payload = self.target_partition_fn[target](data, target)
            
            pipeline = self.r.pipeline()
            pipeline.publish("mailbox-"+str(target),pickle.dumps(payload))

            pipeline.publish("mailbox-id-"+str(target),pickle.dumps((self.id, self.out_seq)))
            results = pipeline.execute()

            # if a downstream node is dead its subscription will be down, there could be a 0 in here, noting noone got this.
            if False in results:
                print("Downstream failure detected")
                #raise Exception(results)
    
    def done(self):

        self.out_seq += 1

        try:
            print("IM DONE", self.id)
            print(sum(len(i) for i in self.state0), sum(len(i) for i in self.state1))
        except:
            pass

        self.output_lock.acquire()
        self.logged_outputs[self.out_seq] = "done"
        self.output_lock.release()

        for target in self.target_partition_fn:
            pipeline = self.r.pipeline()
            pipeline.publish("mailbox-"+str(target),pickle.dumps("done"))
            pipeline.publish("mailbox-id-"+str(target),pickle.dumps((self.id,self.out_seq)))
            results = pipeline.execute()
            if False in results:
                print("Downstream failure detected")

@ray.remote
class InputActor(Node):
    def __init__(self, id, channel, bucket, key, names, ckpt = None) -> None:

        self.r = redis.Redis(host="localhost",port=6800,db=0)
        self.target_partition_fn = {} # dict of id -> functions
        self.id = id 
        print("INPUT ACTOR LAUNCH", self.id)

        self.accessor = InputCSVDataset(bucket, key, names, 0, stride = 1024 * 1024)
        self.accessor.set_num_mappers(NUM_MAPPERS)
        self.output_lock = Lock()

        if ckpt is None:
            self.logged_outputs = OrderedDict()
            self.target_output_state = {}
            self.out_seq = 0
            self.state_tag = 0
            self.state = None
            self.input_generator = self.accessor.get_next_batch(channel)
        else:
            recovered_state = pickle.load(open(ckpt,"rb"))
            self.logged_outputs = recovered_state["logged_outputs"]
            self.target_output_state = recovered_state["target_output_state"]
            self.state = recovered_state["state"]
            self.out_seq = recovered_state["out_seq"]
            self.state_tag = recovered_state["tag"]
            self.input_generator = self.accessor.get_next_batch(channel, self.state)
    
    def checkpoint(self):

        # write logged outputs, state, state_tag to reliable storage
        # for input nodes, log the outputs instead of redownlaoding is probably worth it. since the outputs could be filtered by predicate
        state = { "logged_outputs": self.logged_outputs, "out_seq" : self.out_seq, "tag":self.state_tag, "target_output_state":self.target_output_state,
        "state":self.state}
        pickle.dump(state, open("ckpt-" + str(self.id) + "-temp.pkl","wb"))

        # if this fails we are dead, but probability of this failing much smaller than dump failing
        os.rename("ckpt-" + str(self.id) + "-temp.pkl", "ckpt-" + str(self.id) + ".pkl")
        
    def execute(self):
        
        # no need to log the state tag in an input node since we know the expected path...

        for pos, batch in self.input_generator:
            self.state = pos
            self.push(batch)
            if self.state_tag % 10 == 0:
                self.checkpoint()
            self.state_tag += 1
        
        self.done()
    
    

@ray.remote
class Actor(Node):
    def __init__(self, id, parents, ckpt = None) -> None:

        self.r = redis.Redis(host="localhost",port=6800,db=0)
        self.p = self.r.pubsub(ignore_subscribe_messages=True)
        self.p.subscribe("mailbox-" + str(id), "mailbox-id-" + str(id))
        self.buffered_inputs = {parent: deque() for parent in parents}
        self.id = id 
        self.parents = parents # dict of id -> actor handles
        self.target_partition_fn = {} # dict of id -> functions

        if ckpt is None:
            self.state_tag = np.zeros(len(self.parents),dtype=np.int32)
            
            self.latest_input_received = {parent: 0 for parent in parents}
            self.logged_outputs = OrderedDict()
            self.output_lock = Lock()
            self.target_output_state = {}

            self.state0 = []
            self.state1 = []
            self.out_seq = 0

            self.expected_path = deque()

            self.ckpt_counter = -1

        else:

            recovered_state = pickle.load(open(ckpt,"rb"))
            self.state_tag= recovered_state["tag"]
            print("RECOVERED TO STATE TAG", self.state_tag)
            self.latest_input_received = recovered_state["latest_input_received"]
            self.state0, self.state1 = recovered_state["join_state"]
            self.out_seq = recovered_state["out_seq"]
            self.logged_outputs = recovered_state["logged_outputs"]
            self.output_lock = Lock()
            self.target_output_state = recovered_state["target_output_state"]

            self.expected_path = self.get_expected_path()
            print("EXPECTED PATH", self.expected_path)

            self.ckpt_counter = -1
        
        self.log_state_tag()
        

    def func(self, stream_id, batch):
        # update self.state
        # return outputs
        results = []

        if stream_id  // NUM_MAPPERS == 0:
            if len(self.state1) > 0:
                results = [batch.merge(i,on = "key" ,how='inner',suffixes=('_a','_b')) for i in self.state1]
            self.state0.append(batch)
             
        elif stream_id // NUM_MAPPERS == 1:
            if len(self.state0) > 0:
                results = [i.merge(batch,on = "key" ,how='inner',suffixes=('_a','_b')) for i in self.state0]
            self.state1.append(batch)
        
        if len(results) > 0:
            return results
        
    def get_batches(self, mailbox, mailbox_id, ):
        while True:
            message = self.p.get_message()
            if message is None:
                break
            if message['channel'].decode('utf-8') == "mailbox-" + str(self.id):
                mailbox.append(message['data'])
            elif message['channel'].decode('utf-8') ==  "mailbox-id-" + str(self.id):
                # this should be a tuple (source_id, source_tag)
                mailbox_id.append(pickle.loads(message['data']))
        
        batches_returned = 0
        while len(mailbox) > 0 and len(mailbox_id) > 0:
            first = mailbox.popleft()
            stream_id, tag = mailbox_id.popleft()

            if tag <= self.state_tag[stream_id]:
                print("rejected an input stream's tag smaller than or equal to current state tag")
                continue
            if tag > self.latest_input_received[stream_id] + 1:
                print("DROPPING INPUT. THIS IS A FUTURE INPUT THAT WILL BE RESENT (hopefully)")
                continue

            batches_returned += 1
            self.latest_input_received[stream_id] = tag
            if len(first) < 20 and pickle.loads(first) == "done":
                # the responsibility for checking how many executors this input stream has is now resting on the consumer.
                self.parents.pop(stream_id)
                print("done", stream_id)
            else:
                self.buffered_inputs[stream_id].append(pickle.loads(first))
            
        return batches_returned
    
    def get_expected_path(self):
        return deque([pickle.loads(i) for i in self.r.lrange("state-tag-" + str(self.id), 0, self.r.llen("state-tag-" + str(self.id)))])
    
    # truncate the log to the checkpoint
    def truncate_log(self):
        while True:
            if self.r.llen("state-tag-" + str(self.id)) == 0:
                raise Exception
            tag = pickle.loads(self.r.lpop("state-tag-" + str(self.id)))
            
            if np.product(tag == self.state_tag) == 1:
                return

    def schedule_for_execution(self):
        if len(self.expected_path) == 0:
            # process the source with the most backlog
            lengths = {i: len(self.buffered_inputs[i]) for i in self.buffered_inputs}
            #print(lengths)
            source_to_do = max(lengths, key=lengths.get)
            length = lengths[source_to_do]
            if length == 0:
                return None, None

            # now drain that source
            batch = pd.concat(self.buffered_inputs[source_to_do])
            self.state_tag[source_to_do] += length
            self.buffered_inputs[source_to_do].clear()
            self.log_state_tag()
            return source_to_do, batch

        else:
            expected = self.expected_path[0]
            nzp = np.nonzero(expected - self.state_tag)[0]
            assert len(nzp) == 1 # since the state evolves one element at a time, there can be at most one difference with current state
            nzp = nzp[0]
            required_batches = (expected - self.state_tag)[nzp]
            if len(self.buffered_inputs[nzp]) < required_batches:
                # cannot fulfill expectation
                print("CANNOT FULFILL EXPECTATION")
                return None, None
            else:
                batch = pd.concat([self.buffered_inputs[nzp].popleft() for i in range(required_batches)])
            self.state_tag = expected
            self.expected_path.popleft()
            self.log_state_tag()
            return nzp, batch

    def input_buffers_drained(self):
        for key in self.buffered_inputs:
            if len(self.buffered_inputs[key]) > 0:
                return False
        return True

    def execute(self):

        mailbox = deque()
        mailbox_meta = deque()


        while not (len(self.parents) == 0 and self.input_buffers_drained()):

            # append messages to the mailbox
            batches_returned = self.get_batches(mailbox, mailbox_meta)
            if batches_returned == 0:
                continue
            # deque messages from the mailbox in a way that makes sense
            stream_id, batch = self.schedule_for_execution()

            print(self.state_tag)

            if stream_id is None:
                continue

            results = self.func( stream_id, batch)
            
            self.ckpt_counter += 1
            if self.ckpt_counter %10 == 0:
                print(self.id, "CHECKPOINTING")
                self.checkpoint()

            if results is None:
                continue           
            for result in results:
                self.push(result)
        
        self.done()
    
    def checkpoint(self):

        # write logged outputs, state, state_tag to reliable storage

        state = {"latest_input_received": self.latest_input_received, "logged_outputs": self.logged_outputs, "out_seq" : self.out_seq,
        "join_state": (self.state0,self.state1), "tag":self.state_tag, "target_output_state": self.target_output_state}
        pickle.dump(state, open("ckpt-" + str(self.id) + "-temp.pkl","wb"))

        # if this fails we are dead, but probability of this failing much smaller than dump failing
        os.rename("ckpt-" + str(self.id) + "-temp.pkl", "ckpt-" + str(self.id) + ".pkl")

        self.truncate_log()
        truncate_tasks = []
        for parent in self.parents:
            handler = self.parents[parent]
            truncate_tasks.append(handler.truncate_logged_outputs.remote(self.id, self.state_tag[parent]))
        try:
            ray.get(truncate_tasks)
        except ray.exceptions.RayActorError:
            print("A PARENT HAS FAILED")
            pass
    
    def ask_upstream_for_help(self):
        recover_tasks = []
        print("UPSTREAM",self.parents)
        for key in self.parents:
            handler = self.parents[key]
            recover_tasks.append(handler.help_downstream_recover.remote(self.id, self.state_tag[key]))
        ray.get(recover_tasks)
    
r = redis.Redis(host="localhost",port=6800,db=0)
r.flushall()
input_actors_a = {k: InputActor.options(max_concurrency = 2, num_cpus = 0.001).remote(k,k,"yugan","a-big.csv",["key"] + ["avalue" + str(i) for i in range(100)]) for k in range(NUM_MAPPERS)}
input_actors_b = {k: InputActor.options(max_concurrency = 2, num_cpus = 0.001).remote(k + NUM_MAPPERS,k,"yugan","b-big.csv",["key"] + ["bvalue" + str(i) for i in range(100)]) for k in range(NUM_MAPPERS)}
parents = {**input_actors_a, **{i + NUM_MAPPERS: input_actors_b[i] for i in input_actors_b}}
join_actors = {i: Actor.options(max_concurrency = 2, num_cpus = 0.001).remote(i,parents) for i in range(NUM_MAPPERS * 2, NUM_MAPPERS * 2 + NUM_JOINS)}

def partition_fn(data, target):
    if type(data) == str and data == "done":
        return "done"
    else:
        return data[data.key % NUM_JOINS == target - NUM_MAPPERS * 2]

for i in range(NUM_MAPPERS * 2, NUM_MAPPERS * 2 + NUM_JOINS):
    for j in range(NUM_MAPPERS):
        ray.get(input_actors_a[j].append_to_targets.remote((i, partition_fn)))
        ray.get(input_actors_b[j].append_to_targets.remote((i, partition_fn)))

handlers = {}
for i in range(NUM_MAPPERS):
    handlers[i] = input_actors_a[i].execute.remote()
    handlers[i + NUM_MAPPERS] = input_actors_b[i].execute.remote()
for i in range(NUM_MAPPERS * 2, NUM_MAPPERS * 2 + NUM_JOINS):
    handlers[i] = join_actors[i].execute.remote()

def no_failure_test():
    ray.get(list(handlers.values()))

def actor_failure_test():
    time.sleep(1)

    to_dies = [NUM_MAPPERS * 2, NUM_MAPPERS * 2 + 2]

    for to_die in to_dies:
        ray.kill(join_actors[to_die])
        join_actors.pop(to_die)

    # immediately restart this actor
    for actor_id in to_dies:
        join_actors[actor_id] = Actor.options(max_concurrency = 2, num_cpus = 0.001).remote(actor_id,parents,"ckpt-" + str(actor_id) + ".pkl")
        handlers.pop(actor_id)

    helps = []
    for actor_id in to_dies:
        helps.append(join_actors[actor_id].ask_upstream_for_help.remote())
    ray.get(helps)

    for actor_id in to_dies:
        handlers[actor_id] = (join_actors[actor_id].execute.remote())
    ray.get(list(handlers.values()))

def input_actor_failure_test():
    time.sleep(1)
    to_dies = [0,1]

    for to_die in to_dies:
        ray.kill(input_actors_a[to_die])
        input_actors_a.pop(to_die)

    # immediately restart this actor
    for actor_id in to_dies:
        input_actors_a[actor_id] = InputActor.options(max_concurrency = 2, num_cpus = 0.001).remote(actor_id,actor_id,"yugan","a-big.csv",["key"] + ["avalue" + str(i) for i in range(100)],"ckpt-" + str(actor_id) + ".pkl" ) 
        handlers.pop(actor_id)
    
    appends = []
    for actor_id in to_dies:
        for i in range(NUM_MAPPERS * 2, NUM_MAPPERS * 2 + NUM_JOINS):
            appends.append(input_actors_a[actor_id].append_to_targets.remote((i, partition_fn)))
    ray.get(appends)

    for actor_id in to_dies:
        handlers[actor_id] = (input_actors_a[actor_id].execute.remote())
    ray.get(list(handlers.values()))

def correlated_failure_test():
    time.sleep(3)
    input_to_dies = [0,1]
    actor_to_dies = [NUM_MAPPERS * 2, NUM_MAPPERS * 2 + 2]
    for to_die in actor_to_dies:
        ray.kill(join_actors[to_die])
        join_actors.pop(to_die)
    for to_die in input_to_dies:
        ray.kill(input_actors_a[to_die])
        input_actors_a.pop(to_die)
    
    for actor_id in input_to_dies:
        input_actors_a[actor_id] = InputActor.options(max_concurrency = 2, num_cpus = 0.001).remote(actor_id,actor_id,"yugan","a-big.csv",["key"] + ["avalue" + str(i) for i in range(100)],"ckpt-" + str(actor_id) + ".pkl" ) 
        handlers.pop(actor_id)
        parents[actor_id] = input_actors_a[actor_id]
    for actor_id in actor_to_dies:
        join_actors[actor_id] = Actor.options(max_concurrency = 2, num_cpus = 0.001).remote(actor_id,parents,"ckpt-" + str(actor_id) + ".pkl")
        handlers.pop(actor_id)
    
    appends = []
    for actor_id in input_to_dies:
        for i in range(NUM_MAPPERS * 2, NUM_MAPPERS * 2 + NUM_JOINS):
            appends.append(input_actors_a[actor_id].append_to_targets.remote((i, partition_fn)))
    ray.get(appends)

    helps = []
    for actor_id in actor_to_dies:
        helps.append(join_actors[actor_id].ask_upstream_for_help.remote())
    ray.get(helps)

    for actor_id in actor_to_dies:
        handlers[actor_id] = (join_actors[actor_id].execute.remote())
    for actor_id in input_to_dies:
        handlers[actor_id] = (input_actors_a[actor_id].execute.remote())
    ray.get(list(handlers.values()))

#no_failure_test()
#actor_failure_test() # there is some problem with non-contiguous state variables that only occur with some runs. Need to track those down.
#input_actor_failure_test()
correlated_failure_test()