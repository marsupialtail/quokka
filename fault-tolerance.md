For people who want to fully understand Quokka's recovery mechanism. This is currently written for me as a guideline for implementation, so perhaps it's not laid out in a way for people to understand. I will try to fix this at some point.

High level: In normal operation, Quokka works just like a push-based dataflow engine. There are input nodes which generate streams of objects and task node which operate on those objects to generate new streams and update state. Just like Storm, Flink and Kafka Streams. 

How Quokka does fault recovery is very different. In Quokka every object can be replayed or reconstructed since we log the lineage of everything. Quokka nodes optionally buffer some outputs until they are for sure no longer needed. After node dies we only reconstruct a subset of the objects buffered on that node so that it can keep on doing what it was doing, and those that are needed by other running tasks. That's it. The details however are a lot more complicated, mostly having to do with implementation details around locks, how you store what objects are needed etc.

Quokka relies heavily on a persistent and consistent key-value storage (KVS), which seems to have emerged as one of the major engineering accomplishments in the past two decades. For small scales, Redis on coordinator works, for large scales consider DynamoDB. Or a persistent K-V store integrated with Quokka, probably based on this: https://github.com/voldemort/voldemort/tree/master. The open source version of Quokka will only support Redis which I think is scalable to 1TB of data and around 100 nodes. 

Quokka guarantees that the objects needed by current running tasks or future running tasks are either present or will be generated. These objects are called "alive objects". This guarantee might be broken due to node failure, which will cause 1) object loss 2) relaunching of historical tasks whose inputs might have been garbage collected. The job of the coordinator is to recover the guarantee by relaunching some tasks. The coordinator recovers from one failure at a time. 

============ Naming Scheme =====================

An object's name:
(Actor-id, channel, sequence number, target_actor_id, partition_function_number, target_channel)

Note that an object can have only one producer task and only one consumer task. 

Note partition_function_number implies information about target actor. In the future the partition function might change even for the same target actor due to autoscaling etc, so this is making it forward compatible.

============ Global data structures ===============

- Cemetary Table (CT): track if an object is considered alive, i.e. should be present or will be generated.
The key is (actor-id, channel, target_actor, partition_function_number, target_channel). The value is a Redis set of sequence numbers. If a sequence number is found in this set, then it is not needed by any current or future tasks, and can be safely garbage collected.

The current scheme does not distinguish the different needs of different consumers, since a single object can only be consumed by one task. So as long as that task adds the object to the cemetary, no other live or future tasks will want to use it. This condition could be violated if you relaunch a task that's the same as a current running task or has overlapping input requirements. The coordinator just has to make sure this doesn't happen after fault recovery. This will never happen in normal operation.

- Node Object Table (NOT): track the objects held by each node. Persistent storage like S3 is treated like a node. Key - Set value
- Present Objects Table (PT): This is a Redis set data structure that just keeps track rack the last known location of each object. This will be accurate if the object is not GCed or the node hosting it had died. Key: object name, Value: node ID.
- Node Task Table (NTT): track the current tasks for each node. Key- node IP. Value - Set of tasks on that node. 
- Lineage Table (LT): track the lineage of each object
- PT delete lock: every time a node wants to delete from the PT (engage in GC), they need to check this lock is not acquired but the coordinator. It's like a shared lock. Two nodes can hold this lock at the same time, but no node can hold this lock if the coordinator has it. 


During normal operation, a node does something like this:

- periodically pull from NTT. Things in the NTT must be executed by the node. New tasks must be pushed to the NTT before executing locally, otherwise they won't be recovered. There might be a window of time where a task is completed locally but has not reflected in NTT, this will lead to recreating the object. (Redo logging due to idempotent objects)
- periodically garbage collect after taking out the PT delete lock.
- Run the main execution loop as follows:

tasks = deque()
while True:
    candidate_task = tasks.popleft()
    # get relevant information 
    cache_info = get_cache_info(filter = candidate_task.get_input_requirements)
    # exec_plan is a pandas dataframe that tells you what input sequence numbers to deplete
    exec_plan = candidate.satisfiable(cache_info)

    # if the local cache cannot produce a satisfiable execution plan and this task is high priority, you should query other nodes to pull inputs instead of relying on your local cache. Quokka's correctness guarantee kicks in here -- it says the inputs that you rely on is either present or will be made.
    if exec_plan is None and SHOULD_GO_GLOBAL:
        exec_plan = query_PT(filter = candidate_task.get_input_requirements())

    if exec_plan is not None:
        # outputs will consist of all the partitioned slices for all the downstream channels.
        outputs = candidate_task.execute(exec_plan)
        # store all the outputs locally in a high bandwidth queue (HBQ) in mem/disk
        for output in outputs:
            HBQ.put(output)

        clear_cache(exec_plan)

        # I can update the cemetary right now. I haven't told the NTT that I am done with this task, so these will add be resurrected if the node is lost. No need for any atomic operations here either. If you fail in the middle everything will still work.
        my_actor_id, my_partition_fn, my_channel = candidate_task.get_task_info()
        for actor_id, channel, stuff in exec_plan.groupby(actor_id, channel):
            for seq_number in stuff["seq_number"].to_list()
                CT.spop(key = (actor_id, channel, my_actor_id, my_partiion_fn, my_target_channel), member = seq_number)

        # iterate through all the outputs to different target channels.
        for output in outputs:

            # a candidate task specifies if the outputs should be pushed to downstream caches. This is usually true in normal operation but might be turned off in recovery to let dependent tasks explicitly pull objects. this would be if your downstream channel was parallel recovered and the upstream source has no idea where to push to.
            if candidate_task.should_push_outputs:
                try:
                    output.push(router[output.target_actor, output.target_channel])
                except:
                    # failed to push to cache, probably because downstream node failed. That's ok it will be recovered, and somebody will ask for this object.
                    pass

            # I can update these right now. I haven't told the NTT that I am done with this task, so I will just redo it. All the objects that are in a dead node's NOT doesn't matter. The multi-exec here is probably not needed but doesn't hurt and simplifies reasoning.
            MULTI
            NOT.sadd(key = node, member = output.name)

            # record where this object was made. This value will be wrong if this object is lost due to node failure or garbage collected on this node and recovered somewhere else. That's ok -- QUokka guarantees alive objects will be recovered eventually, and whoever recovers this will update this value.
            PT.set(output.name, name)
            EXEC

            # You only ever add to the LT, it's ok.
            # When we put tasks into the LT, we don't need to keep track of the future tasks this task could launch. as such it doesn't need # # the serialization of the entire task, just task.get_task_info()
            LT.add(output.name, task.get_input_requirements(), task.get_task_info())


        # the task should have facilities to generate what is going to happen next.
        next_task = candidate_task.get_next_task()
        if next_task is not None:
            tasks.append(next_task)
            # now we need to actually reflect this information in the global data structures
            # note when we put task info in the NTT, the info needs to include the entire task, including all the future tasks it could launch. i.e. the tape in taped tasks.

            MULTI
            NTT.spop(key = node, member = candidate_task.__reduce__())
            NTT.sadd(key = node, member = next_task.__reduce__())
            EXEC
            
        else:
            # this task is not spawning off more tasks, it's considered done
            # most likely it got done from all its input sources
            NTT.spop(key = node, member = candidate_task.__reduce__())
            
    else:
        # task inputs cannot be satisfied by cache or other nodes. Just wait.
        # nothing happened from the perspective of all the global data sturctures.
        tasks.append(candidate_task) 


The task node periodically will run a garbage collection process for its local cache as well as its HBQ. To do that it will need to take out the delete lock on the PT, since the coordinator cannot be doing recovery when this happens.

# this can be run every X iterations in the main loop
def garbage_collect():
    lock_acquired = try to acquire PT delete lock
    # we have acquired the lock, this means the coordinator is not running fault recovery
    if lock_acquired:
        # you can safely garbage collect. 
        for object in local_cache:
            if object in CT:
                local_cache.drop(object)
            # you don't have to update global data structures when you delete stuff from your local cache because nobody else cares (for the moment.... until we decide to optimize and say cache also counts as object storage to avoid double object presence as mentioned later in the cons.)
        for object in HBQ:
            if object in CT:
                HBQ.drop(object)
            MULTI
            NOT.sadd(key = node, member = output.name)
            PT.add(output.name)
            EXEC
    else:
        wait.

# this can also be run every X iterations in the main loop
def update tasks():
    for task in NTT[node]:
        if task not in tasks:
            tasks.append(task)

Upon failure, the coordinator initiates recovery procedure. After recovery procedure, the global data structures must be in a consistent state that satisfies the guarantee.

- Read NTT to determine current pending tasks on the lost node, these need to be recovered. The NTT's entry for the lost node will not change since the only other person who can change it is dead, so it's okay if we don't immediately start the recovery procedure post failure, or if there's a delay between the completion of this step and the next step (acquiring NOT lock).
- Take out table-wide delete lock on PT. We need a consistent view of the PT during the recovery process. It's okay if new objects are created and buffered (though this could lead to redundant recovery) but it's not okay if an object gets removed.
- Now we need to figure out a recovery plan. We need to recover two types of objects: 1) the objects needed to resume the lost tasks on the node 2) the alive objects that were kept on the node. We will read the LT and our view of the NOT to determine what tasks need to be relaunched to recover these objects. How exactly we do this is subject to optimization. There are three types of tasks in Quokka. The first is a task that has state dependency, the second is a stateless task, the third is input task. 


Use ray.wait to detect dead_node

objects_on_node = NOT[node]. Note you do not have to lock the cemetary table because normal workers cannot resurrect dead objects. If you happen to decide something is alive here and recover it and while computing the plan it becomes dead, then you just wasted some work. There will never be a case where you decide something is dead and it became alive. In fact this straightforwardly violates our guarantee. Workers don't have this power.

# we will only recover the alive objects on this node. This is effectively forced garbage collection after the node has failed.
alive_outputs_to_make = set()
for object in objects_on_node:
    if check_if_alive(object, CT):
        alive_outputs_to_make.add(objec)

object_recovery_tasks = [get_task_from_lineage(LT[output]) for output in alive_outputs_to_make]
new_tasks = NTT[dead_node]  + object_recovery_tasks
# you don't have to change the cemetary table for the current_tasks as their requirements are already there. but you do have to ressurect the input requirements of the new tasks you launch from the cemetary.

tasks_to_launch = []
new_tasks = deque(new_tasks)
newly_recovered = set()
while len(new_tasks) > 0:
    curr_task = new_tasks.popleft()
    if curr_task is not InputTask:
        input_ranges = curr_task.get_input_requirements()
        for input in input_ranges:
            # we are going to consolidate in the end anyways, the not in newly_recovered is just an optimization to reduce calls to LT.
            
            # IMPORTANT NOTE: an object could be moved to CT by a node while this is happening.
            # this means there could be a state for an object where it is being slated for recovery while it is in CT. That's ok because nobody could garbage collect this object and actually remove it from PT. When the owner of that object finally acquires the PT delete lock, it has to check CT, and the coordinator must have updated CT before giving up the PT delete lock. This is done as a performance optimization. If this leads to wrong behavior, i.e. coordinator relying on objects that are actually GCed, try to use a separate lock for the CT. Changes in CT are expected to be frequent while deletions to PT only happen at GC intervals. The actual locking is fine since it's a shared lock between nodes and recoveries are expected to be far apart -- though this would mean no worker can make forward progress during fault recovery without somehow buffering updates to CT, too complicated!

            # also there could be a state for an object where the coordinator sees it is in PT, but actually the machine hosting it failed when the coordinator is coming up with this recovery plan. This is also ok since all failures can be serialized, and this object will be recovered when the coordinator handles this failure, since this object will be removed from the CT if it was ever in there.

            if input in CT and input not in PT and input not in newly_recovered:
                # need to make a new task to recover this input!
                new_task = get_task_from_lineage(LT[input])
                new_tasks.append(new_task)
                newly_recovered.add(input)
    else:
        pass
    tasks_to_launch.append([(curr_task.actor_id, curr_task.channel),  curr_task])

# now figure out the best way to launch tehse tasks
# we will groupby the actor-id and channel. Then for each actor type, stateful, stateless, input, we will figure out a plan
# our task-based recovery scheme gives us immense flexibility on how to reschedule recovery
# parallel recovery is easily supported for stateless and input nodes. For stateful nodes, we can optionally support parallel recovery in the future, i.e. spawn three tasks to recover a sort node that has already sorted 10 numbers -- sort first 5, sort next 5, merge and sort the rest. The third task will depend on the first two.

tasks_to_launch = pd.DataFrame(tasks_to_launch, columns = ["key", "task"])
for key, group in tasks_to_launch.groupby("key"):
    actor_id, channel = key
    # if it's an input node we will just launch several long tasks with tapes
    # this doesn't have to worry about removing things from the cemetary
    if actor_type[actor_id] == "INPUT":
        number_of_new_tasks = get_FT_parallelism()

# if the old current tasks key hashes are reused, update their values in AT. Add the new tasks info in there as well. This should be done in the end in one atomic action with everything else.
MULTI
for task in new_tasks:
    for actor_id, channel in task.input_requirements:
        resurrect from cemetary

update the NTT as well. 
delete the node's entry in the NOT. 
delete all items relevant to the node in the PT.
EXEC

Pros

Probably works.

Cons

1) Still too complicated. I wonder if it's possible to change the guarantee from all the inputs for current or future tasks will be present or is present to all inputs for current or future tasks will be pushed to the task. This is a bit complicated if you are going to engage in any form of parallel recovery, since a single actor's task queue could be spread across multiple nodes. Then one of those nodes could fail and the task queue would be further spread out. Tracking where each object should be pushed to in this case is extremely hard. You probably will end up doing something like tracking which task consumes each object, and have a registry to look up where each task is currently residing. This is problematic because tasks are currently indexed by output sequences since they can arbitrarily batch input elements, so we don't exactly know which task an input object should be sent to. 

2) Two copies of the same object could exist across the system, though they should both be GC'ed. The cached one will be cleared as soon as it is used, and both will be cleared when the object is put in CT. This only happens when you enable push on the task, which is the default mode of operation. In this mode though, the producer will write the object to disk directly since it doesn't expect people to come look for it unless there are failures. This is also a pro I suppose, since having two copies of the same object allows you to be faster in fault recovery.

3) It might be too flexible. The current scheme assumes that the nodes can GC their objects on any schedule, though some schedules clearly make more sense than other schedules. For example, if producer is writing to a consumer, and consumer checkpoints at regular intervals, then it probably makes sense to buffer producer outputs up to the last checkpoint, or not at all. If you only buffered the last 3 objects, and the last checkpoint was 5 objects away, then you have to regenerate two objects on the producer side, which involves a rewind. 

Note in this case, there will be two copies of the producer running!! Quokka currently does not have the facilities to strike down a running task that has not failed. So what would happen is that a separate producer gets spun up at a suitable checkpoint to regenerate these two lost objects, while the original producer keeps chugging along. This could be fine if there is not too much memory constraint on the system, which Quokka aims to achieve anyways by using disk in a good way. i.e. the states of tasks that are not slated for execution might be flushed to disk.

You can make sure this never happens by tuning some GC policies with your custom messages on top of Quokka. Quokka's fault tolerance aims to be correct regardless what GC policies people end up implementing -- fun masters project in 1 year? lol.

4) Not truly scalable. The number of object names grow quadratically with the number of channels in the system in a shuffle. This is similar to Spark, which I'd argue is also not great. This will make the coordinator actions slow. If you have 1000 source channels and 1000 target channels, you will have to keep track of 1 million sets for the CT! Each set could easily have 1000 numbers and that is 4GB of storage right there. Of course the cemetary design could be optimized by keeping a minimum and a maximum etc. But that doesn't solve the fundamnetal issues posed by the naming scheme -- there are simply too many objects. You could probably optimize it if you track objects only at the producer level. But then multiple consumers might hold claims to a single object, which will make aliveness tracking very difficult. This is because the number of consumers might change if you do parallel recovery or elastic scaling! You probably need to introduce something like a task hash, and keep track of the aliveness requirements of different task hashes for the same object's sequence numbers. This is likely even more complicated.