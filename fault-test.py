from nodes import *
from sql import PolarJoinExecutor
import sys
sys.path.append("/home/ziheng/quokka-dev/")

ray.init(ignore_reinit_error=True)

NUM_MAPPERS = 2
NUM_JOINS = 4

r = redis.Redis(host="localhost",port=6800,db=0)
r.flushall()
input_actors_a = {k: InputS3CSVNode.options(max_concurrency = 2, num_cpus = 0.001).remote(0,k,"yugan","a-big.csv",["key"] + ["avalue" + str(i) for i in range(100)], NUM_MAPPERS, ("quokka-checkpoint","ckpt")) for k in range(NUM_MAPPERS)}
input_actors_b = {k: InputS3CSVNode.options(max_concurrency = 2, num_cpus = 0.001).remote(1,k,"yugan","b-big.csv",["key"] + ["bvalue" + str(i) for i in range(100)], NUM_MAPPERS, ("quokka-checkpoint","ckpt")) for k in range(NUM_MAPPERS)}
parents = {0:input_actors_a, 1: input_actors_b}
join_executor = PolarJoinExecutor(on="key")
join_actors = {i: NonBlockingTaskNode.options(max_concurrency = 2, num_cpus = 0.001).remote(2,i, {0:0,1:1}, None, join_executor, parents, ("quokka-checkpoint","ckpt")) for i in range(NUM_JOINS)}
join_channel_to_ip = {i: 'localhost' for i in range(NUM_JOINS)}

for j in range(NUM_MAPPERS):
    ray.get(input_actors_a[j].append_to_targets.remote((2, join_channel_to_ip, "key")))
    ray.get(input_actors_b[j].append_to_targets.remote((2, join_channel_to_ip, "key")))

handlers = {}
for i in range(NUM_MAPPERS):
    handlers[(0,i)] = input_actors_a[i].execute.remote()
    handlers[(1,i)] = input_actors_b[i].execute.remote()
for i in range(NUM_JOINS):
    handlers[(2,i)] = join_actors[i].execute.remote()

def no_failure_test():
    ray.get(list(handlers.values()))

def actor_failure_test():
    time.sleep(1)

    to_dies = [0, 2]

    for to_die in to_dies:
        ray.kill(join_actors[to_die])
        join_actors.pop(to_die)

    # immediately restart this actor
    for actor_id in to_dies:
        join_actors[actor_id] = NonBlockingTaskNode.options(max_concurrency = 2, num_cpus = 0.001).remote(2,actor_id, {0:0,1:1}, None, join_executor, parents, ("quokka-checkpoint","ckpt"), ckpt = "ckpt-2-"+str(actor_id)+".pkl")
        handlers.pop((2, actor_id))

    helps = []
    for actor_id in to_dies:
        helps.append(join_actors[actor_id].ask_upstream_for_help.remote("localhost"))
    ray.get(helps)

    for actor_id in to_dies:
        handlers[(2,actor_id)] = (join_actors[actor_id].execute.remote())
    ray.get(list(handlers.values()))

def input_actor_failure_test():
    time.sleep(1)
    to_dies = [0,1]

    for to_die in to_dies:
        ray.kill(input_actors_a[to_die])
        input_actors_a.pop(to_die)

    # immediately restart this actor
    for actor_id in to_dies:
        input_actors_a[actor_id] =  InputS3CSVNode.options(max_concurrency = 2, num_cpus = 0.001).remote(0,actor_id,"yugan","a-big.csv",["key"] + ["avalue" + str(i) for i in range(100)], NUM_MAPPERS, ("quokka-checkpoint","ckpt"), ckpt = "ckpt-0-"+str(actor_id)+".pkl") 
        handlers.pop((0,actor_id))
    
    appends = []
    for actor_id in to_dies:
        appends.append(input_actors_a[actor_id].append_to_targets.remote((2, join_channel_to_ip, "key")))
    ray.get(appends)

    for actor_id in to_dies:
        handlers[(0,actor_id)] = (input_actors_a[actor_id].execute.remote())
    ray.get(list(handlers.values()))

def correlated_failure_test():
    time.sleep(3)
    input_to_dies = [0,1]
    actor_to_dies = [0, 2]
    for to_die in actor_to_dies:
        ray.kill(join_actors[to_die])
        join_actors.pop(to_die)
    for to_die in input_to_dies:
        ray.kill(input_actors_a[to_die])
        input_actors_a.pop(to_die)
    
    for actor_id in input_to_dies:
        input_actors_a[actor_id] =  InputS3CSVNode.options(max_concurrency = 2, num_cpus = 0.001).remote(0,actor_id,"yugan","a-big.csv",["key"] + ["avalue" + str(i) for i in range(100)], NUM_MAPPERS, ("quokka-checkpoint","ckpt"), ckpt = "ckpt-0-"+str(actor_id)+".pkl") 
        handlers.pop((0,actor_id))
        parents[0][actor_id] = input_actors_a[actor_id]
    for actor_id in actor_to_dies:
        join_actors[actor_id] = NonBlockingTaskNode.options(max_concurrency = 2, num_cpus = 0.001).remote(2,actor_id, {0:0,1:1}, None, join_executor, parents, ("quokka-checkpoint","ckpt"), ckpt = "ckpt-2-"+str(actor_id)+".pkl")
        handlers.pop((2,actor_id))
    
    appends = []
    for actor_id in input_to_dies:
        appends.append(input_actors_a[actor_id].append_to_targets.remote((2, join_channel_to_ip, "key")))
    ray.get(appends)

    helps = []
    for actor_id in actor_to_dies:
        helps.append(join_actors[actor_id].ask_upstream_for_help.remote("localhost"))
    ray.get(helps)

    for actor_id in actor_to_dies:
        handlers[(2,actor_id)] = (join_actors[actor_id].execute.remote())
    for actor_id in input_to_dies:
        handlers[(0,actor_id)] = (input_actors_a[actor_id].execute.remote())
    ray.get(list(handlers.values()))

#no_failure_test()
#actor_failure_test() # there is some problem with non-contiguous state variables that only occur with some runs. Need to track those down.
#input_actor_failure_test()
correlated_failure_test()
