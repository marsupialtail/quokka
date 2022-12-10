'''
lists of redis client wrappers that prepend keys with schema
can't use different Redis DBs because that's not best practice, and you can't do transactions across different DBs.
to do a transaction here just take out r.pipeline on the main redis client that's passed in to construct these tables.
'''
import pickle

class ClientWrapper:
    def __init__(self,  key_prefix) -> None:
        self.key_prefix = key_prefix.encode("utf-8")
    
    def wrap_key(self, key):
        assert type(key) == str or type(key) == bytes or type(key) == int, (key, type(key))
        if type(key) == str:
            key = key.encode("utf-8")
        elif type(key) == int:
            key = str(key).encode("utf-8")
        return self.key_prefix + b'-' + key
    
    def srem(self, redis_client, key, fields):
        key = self.wrap_key(key)
        return redis_client.srem(key, *fields)
    
    def sadd(self, redis_client, key, field):
        key = self.wrap_key(key)
        return redis_client.sadd(key, field)
    
    def scard(self, redis_client, key):
        key = self.wrap_key(key)
        return redis_client.scard(key)
    
    def set(self, redis_client, key, value):
        key = self.wrap_key(key)
        return redis_client.set(key, value)
    
    def get(self, redis_client, key):
        key = self.wrap_key(key)
        return redis_client.get(key)
    
    def mget(self, redis_client, keys):
        keys = [self.wrap_key(key) for key in keys]
        return redis_client.mget(keys)
    
    def mset(self, redis_client, vals):
        vals = {self.wrap_key(key): vals[key] for key in vals}
        return redis_client.mset(vals)
    
    def delete(self, redis_client, key):
        key = self.wrap_key(key)
        return redis_client.delete(key)
    
    def smembers(self, redis_client, key):
        key = self.wrap_key(key)
        return redis_client.smembers(key)
    
    def sismember(self, redis_client, key, value):
        key = self.wrap_key(key)
        return redis_client.sismember(key, value)
    
    def srandmember(self, redis_client, key):
        key = self.wrap_key(key)
        return redis_client.srandmember(key)
    
    def lrem(self, redis_client, key, count, element):
        key = self.wrap_key(key)
        return redis_client.lrem(key, count, element)
    
    def lpush(self, redis_client, key, value):
        key = self.wrap_key(key)
        return redis_client.lpush(key, value)
    
    def rpush(self, redis_client, key, value):
        key = self.wrap_key(key)
        return redis_client.rpush(key, value)
    
    def lpop(self, redis_client, key, count = 1):
        key = self.wrap_key(key)
        return redis_client.lpop(key, count)
    
    def llen(self, redis_client, key):
        key = self.wrap_key(key)
        return redis_client.llen(key)

    def lindex(self, redis_client, key, index):
        key = self.wrap_key(key)
        return redis_client.lindex(key, index)
    
    def lrange(self, redis_client, key, start , end):
        key = self.wrap_key(key)
        return redis_client.lrange(key, start, end)
    
    def keys(self, redis_client):
        key = self.key_prefix + b'*'
        return [i.replace(self.key_prefix + b'-',b'') for i in redis_client.keys(key)]

'''
Cemetary Table (CT): track if an object is considered alive, i.e. should be present or will be generated.
The key is (actor-id, channel, seq). The value is another set of values with form (target-actor-id, partition_fn, target-channel-id)
During garbage collection, we garbage collect all the objects with prefix actor-id, channel, seq together when all of them are no longer needed.
Objects are stored in the HBQ together by prefix. As a result, we have to delete all objects with the same prefix all at once or not at all.
'''

class CemetaryTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__( "CT")
    
    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        result = {}
        for key in keys:
            result[pickle.loads(key)] = [pickle.loads(k) for k in self.smembers(redis_client, key)]
        return result
    

'''
Node Object Table (NOT): track the objects held by each node. Persistent storage like S3 is treated like a node. 
    Key is node_id, value is a set of object name prefixes. We just need to store source-actor-id, source-channel-id and seq
    because all objects generated for different target channels are all present or not together. This saves order of magnitude storage.
'''

class NodeObjectTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__( "NOT")

    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        result = {}
        for key in keys:
            result[key] = [pickle.loads(k) for k in self.smembers(redis_client, key)]
        return result

'''
- Present Objects Set (POT): This ikeeps track of all the objects that are present across the cluster.
  This is like an inverted index of NOT, maintained transactionally to save time.
    Key: object_name, value is where it is. The key is again just the prefix.
'''

class PresentObjectTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__( "POT")
    
    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {pickle.loads(key): value for key, value in zip(keys, values)}

'''
- Node Task Table (NTT): this keeps track of all the tasks on a node.
    Key: node_id, value is a list of tasks. Poor man's priority queue.
'''

class NodeTaskTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__( "NTT")
    
    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        result = {}
        for key in keys:
            result[key] = [pickle.loads(k) for k in self.lrange(redis_client, key, 0, -1)]
        return result

'''
- Generated Input Table (GIT): no tasks in the system are running to generate those objects. This could be figured out
 by just reading through all the node tasks, but that can be expensive. 
    Key: (source_actor_id, source_channel_id)
    Value: set of seq numbers in this NOTT.
'''

class GeneratedInputTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__("GIT")
    
    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        result = {}
        for key in keys:
            result[pickle.loads(key)] = self.smembers(redis_client, key)
        return result


'''
- Lineage Table (LT): this tracks the inputs of each output. This dynamically tells you what is IN(x)
- The key is simply (actor_id, channel_id, seq). Since you know what partition_fn to apply to get the objects.
'''

class LineageTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__( "LT")
    
    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {pickle.loads(key): value for key, value in zip(keys, values)}

'''
- Done Seq Table (DST): this tracks the last sequence number of each actor_id, channel_id. There can only be one value
'''

class DoneSeqTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__( "DST")
    
    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {pickle.loads(key): value for key, value in zip(keys, values)}


'''
- Last Checkpoint Table (LCT): this tracks the last state_seq number that has been checkpointed for actor_id, channel_id
'''

class LastCheckpointTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__("LCT")
    
    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        result = {}
        for key in keys:
            result[key] = [pickle.loads(k) for k in self.lrange(redis_client, key, 0, -1)]
        return result


'''
- Executor State Table (EST): this tracks what state_seq each actor_id and channel_id are on (last committed)
'''

class ExecutorStateTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__("EST")
    
    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {pickle.loads(key): value for key, value in zip(keys, values)}

'''
- Channel Location Table (CLT): this tracks where each channel is scheduled
'''

class ChannelLocationTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__("CLT")
    
    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {pickle.loads(key): value for key, value in zip(keys, values)}

'''
- Function Object Table (FOT): this stores the function objects
    key: actor_id, value: serialized Python object
'''

class FunctionObjectTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__("FOT")

'''
- Input Requirements Table (IRT): this stores the new input requirements
    key: actor_id, channel_id, seq (only seqs will be ckpt seqs), value: new_input_reqs df
'''

class InputRequirementsTable(ClientWrapper):
    def __init__(self) -> None:
        super().__init__("IRT")
    
    def to_dict(self, redis_client):
        keys = self.keys(redis_client)
        values = self.mget(redis_client, keys)
        return {pickle.loads(key): pickle.loads(value) for key, value in zip(keys, values)}