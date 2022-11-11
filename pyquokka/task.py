from collections import deque
import random
import pickle

'''
The names of objects are (source_actor_id, source_channel_id, seq, target_actor_id, partition_fn, target_channel_id)
'''
class Object:
    def __init__(self, name: tuple, data) -> None:
        assert len(name) == 6
        self.tuple_name = name
        self.data = data
    
    @property
    def source_actor_id(self):
        return self.tuple_name[0]
    
    @property
    def source_channel_id(self):
        return self.tuple_name[1]
    
    @property
    def seq(self):
        return self.tuple_name[2]
    
    @property
    def target_actor_id(self):
        return self.tuple_name[3]
    
    @property
    def partition_fn(self):
        return self.tuple_name[4]
    
    @property
    def target_channel_id(self):
        return self.tuple_name[5]
    
    @property
    def name(self):
        return pickle.dumps(self.tuple_name)

class Task:
    def __init__(self, actor_id, channel_id) -> None:
        self.actor_id = actor_id
        self.channel_id = channel_id

class InputTask(Task):
    def __init__(self, actor_id, channel_id, seq, input_object) -> None:
        super().__init__(actor_id, channel_id)
        self.input_object = input_object
        self.seq = seq
    
    @classmethod
    def from_tuple(cls, tup):
        assert len(tup) == 4
        return cls(tup[0], tup[1], tup[2], tup[3])

    def reduce(self):
        return pickle.dumps(("input",(self.actor_id, self.channel_id, self.seq, self.input_object)))

    def execute(self, functionObject):

        # any object, Polars DataFrame    
        next_input_object, result = functionObject.execute(self.channel_id, self.input_object)

        # needs to return the next task to launch if any, the result and the lineage
        if next_input_object is not None:
            return InputTask(self.actor_id, self.channel_id, self.seq + 1, next_input_object), result, self.seq, pickle.dumps(self.input_object)
        else:
            # you are done. 
            return None, result, self.seq, pickle.dumps(self.input_object)


class TapedInputTask(Task):
    def __init__(self, actor_id, channel_id, tape ) -> None:
        super().__init__(actor_id,channel_id)
        assert len(tape) > 0
        self.tape = tape
    
    @classmethod
    def from_tuple(cls, tup):
        assert len(tup) == 3
        return cls(tup[0], tup[1], tup[2])

    def reduce(self):

        # currently we have to serialize the tape. This maybe fine for input tasks but is definitely not acceptable for real tasks
        # gotta figure out what to do in that regard.
        return pickle.dumps(("inputtape", (self.actor_id, self.channel_id, self.tape)))

    def execute(self, functionObject, input_object):

        seq = self.tape[0]

        # we don't care what's the next thing we are supposed to read       
        next_input_object, result = functionObject.execute(self.channel_id, input_object)

        if len(self.tape) == 1:
            return None, result, seq, None
        else:
            return TapedInputTask(self.actor_id, self.channel_id, self.tape[1:]), result, seq, None

"""
An example input_reqs is a Polars DataFrame that looks like this:

source_actor_id, source_channel_id, seq
0, 0, 3
0, 1, 2
0, 2, 4
1, 0, 0
1, 1, 3
1, 2, 4
"""

class ExecutorTask(Task):
    def __init__(self, actor_id, channel_id, state_seq, out_seq, input_reqs) -> None:
        super().__init__(actor_id, channel_id)
        self.input_reqs = input_reqs
        self.state_seq = state_seq
        self.out_seq = out_seq

    @classmethod
    def from_tuple(cls, tup):
        assert len(tup) == 5, tup
        return cls(tup[0], tup[1], tup[2], tup[3], tup[4])

    def execute(self, functionObject, inputs, stream_id, channel_id):

        output = functionObject.execute(inputs, stream_id, channel_id)
        return output, self.state_seq, self.out_seq 

    def reduce(self):
        return pickle.dumps(("exec", (self.actor_id, self.channel_id, self.state_seq, self.out_seq, self.input_reqs)))

class TapedExecutorTask(Task):
    def __init__(self, actor_id, channel_id, state_seq, out_seq, last_state_seq) -> None:
        super().__init__(actor_id, channel_id)
        self.state_seq = state_seq
        self.out_seq = out_seq
        self.last_state_seq = last_state_seq

    @classmethod
    def from_tuple(cls, tup):
        assert len(tup) == 5, tup
        return cls(tup[0], tup[1], tup[2], tup[3], tup[4])

    def execute(self, functionObject, inputs, stream_id, channel_id):

        output = functionObject.execute(inputs, stream_id, channel_id)
        return output, self.state_seq, self.out_seq 

    def reduce(self):
        return pickle.dumps(("exectape", (self.actor_id, self.channel_id, self.state_seq, self.out_seq, self.last_state_seq)))


class ReplayTask(Task):
    def __init__(self, actor_id, channel_id, replay_specification) -> None:
        super().__init__(actor_id, channel_id)
        self.replay_specification = replay_specification
        self.needed_seqs = replay_specification["seq"].unique().to_list()
    
    @classmethod
    def from_tuple(cls, tup):
        assert len(tup) == 3, tup
        return cls(tup[0], tup[1], tup[2])

    def reduce(self):
        return pickle.dumps(("replay", (self.actor_id, self.channel_id, self.replay_specification)))