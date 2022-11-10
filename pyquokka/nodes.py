import pandas as pd
import ray
from collections import deque
import pickle
from threading import Lock
import time
import gc
import sys
import polars
import pyarrow as pa
import types
#import duckdb
import psutil
import pyarrow.flight
# isolated simplified test bench for different fault tolerance protocols

VERBOSE = False
PROFILE = False

# above this limit we are going to start flushing things to disk
INPUT_MAILBOX_SIZE_LIMIT = 1024 * 1024 * 1024 * 2 # you can have 2GB in your input mailbox

def convert_to_format(batch, format):
    format = pickle.loads(format.to_pybytes())
    if format == "polars":

        aligned_batch = pa.record_batch([pa.concat_arrays([arr]) for arr in batch], schema=batch.schema)
        return polars.from_arrow(pa.Table.from_batches([aligned_batch]))

        # ideally you use zero copy, but this could result in bugs sometimes. https://issues.apache.org/jira/browse/ARROW-17783
        # return polars.from_pandas(pa.Table.from_batches([batch]).to_pandas())
        # return polars.from_arrow(pa.Table.from_batches([batch]))
    elif format == "pandas":
        return batch.to_pandas()
    elif format == "custom":
        return pickle.loads(batch.to_pandas()['object'][0])
    elif format == "arrow-table":
        return pa.Table.from_batches([batch])
    elif format == "arrow-batch":
        return batch
    else:
        raise Exception("don't understand this format", format)

def convert_from_format(payload):

    if type(payload) == pd.core.frame.DataFrame:
        batches = [pa.RecordBatch.from_pandas(payload)]
        my_format = "pandas"
    elif type(payload) == polars.internals.DataFrame:
        batches = payload.to_arrow().to_batches()
        my_format = "polars"
    elif type(payload) == pa.lib.RecordBatch:
        batches = [payload]
        my_format = "arrow-batch"
    elif type(payload) == pa.lib.Table:
        batches = payload.to_batches()
        my_format = "arrow-table"
    else:
        batches = [pa.RecordBatch.from_pydict({"object":[pickle.dumps(payload)]})]
        my_format = "custom"
    
    assert len(batches) > 0, payload
    return batches, my_format

# making a new class of things so we can easily type match when we start processing the input batch
class FlushedMessage:
    def __init__(self, loc) -> None:
        self.loc = loc

class SharedMemMessage:
    def __init__(self, form, name):
        self.format = form
        self.name = name

class Node:

    # will be overridden
    def __init__(self, id, channel) -> None:

        self.ip = ray.util.get_node_ip_address() 
        self.id = id
        self.channel = channel

        self.targets = {}
        
        self.flight_clients = {}
        self.flight_client = pyarrow.flight.connect("grpc://0.0.0.0:5005")

        # track the targets that are still alive
        self.alive_targets = {}
        self.output_lock = Lock()

        self.out_seq = {}

    def initialize(self):
        raise NotImplementedError

    def append_to_targets(self,tup):
        node_id, channel_to_ip, target_info = tup

        self.out_seq[node_id] = {channel: 0 for channel in channel_to_ip}

        unique_ips = set(channel_to_ip.values())
        self.targets[node_id] = (channel_to_ip, target_info)
        
        self.flight_clients[node_id] = {}

        flight_clients = {i: pyarrow.flight.connect("grpc://" + str(i) + ":5005") if i != self.ip else pyarrow.flight.connect("grpc://0.0.0.0:5005") for i in unique_ips}
        for channel in channel_to_ip:
            self.flight_clients[node_id][channel] = flight_clients[channel_to_ip[channel]]
        
        
        self.alive_targets[node_id] = {i for i in channel_to_ip}
        # remember the self.strikes stuff? Now we cannot check for that because a downstream target could just die.
        # it's ok if we send stuff to dead people. Fault tolerance is supposed to take care of this.

    def push(self, data):
            
        '''
        Quokka should have some support for custom data types, similar to DaFt by Eventual.
        Since we transmit data through Arrow Flight, we need to convert the data into arrow record batches.
        All Quokka data readers and executors should take a Polars DataFrame as input batch type and output type.
        The other data types are not going to be used that much.
        '''

        # your format at this point doesn't matter. we need to write down the format after all the batch funcs.

        if type(data) == pd.core.frame.DataFrame:
            data = polars.from_pandas(data)
        elif type(data) == polars.internals.DataFrame:
            data = data
        elif type(data) == pa.lib.Table:
            data = polars.from_arrow(data)
        elif type(data) == pa.lib.RecordBatch:
            data = polars.from_arrow(pa.Table.from_batches([data]))
        else:
            my_format = "custom"
            print("custom data does not support predicates and projections")
            raise NotImplementedError
        
        # if type(data) == pd.core.frame.DataFrame:
        #     data = pa.Table.from_pandas(data)
        # elif type(data) == polars.internals.DataFrame:
        #     data = data.to_arrow()
        # elif type(data) == pa.lib.Table:
        #     pass
        # elif type(data) == pa.lib.RecordBatch:
        #     data = pa.Table.from_batches([data])
        # else:
        #     my_format = "custom"
        #     print("custom data does not support predicates and projections")
        #     raise NotImplementedError

        # downstream targets are done. You should be done too then.

        for target in self.alive_targets:
            original_channel_to_ip, target_info = self.targets[target]

            # target.partitioner will be a FunctionPartitioner with a function that takes in three arguments
            # the data (arbitrary python object), self.channel, channel of this node and number of downstream channels
            # it must return a dictionary of downstream channel number -> arbitrary python object
            # this partition key could be user specified or be supplied by Quokka from default strategies

            assert target_info.lowered

            start = time.time()
            if target_info.predicate != True:
                data = data.filter(target_info.predicate(data))
            # # data = self.con.execute("select * from data where " + target_info.predicate).arrow()
            #data = polars.from_arrow(data)
            # #print("post-filer", data)

            if PROFILE:
                print("filter ", time.time() - start)
            
            start = time.time()
            partitioned_payload = target_info.partitioner.func(data, self.channel, len(original_channel_to_ip))

            if PROFILE:
                print("partition ", time.time() - start)

            
            assert type(partitioned_payload) == dict and max(partitioned_payload.keys()) < len(original_channel_to_ip)
            
            for channel in self.alive_targets[target]:

                self.out_seq[target][channel] += 1
                if channel not in partitioned_payload:
                    payload = None
                else:
                    payload = partitioned_payload[channel]

                    # the batch funcs here all expect arrow data fomat.

                    start = time.time()

                    for func in target_info.batch_funcs:
                        if len(payload) == 0:
                            payload = None
                            break
                        payload = func(payload)
                    
                    if PROFILE:
                        print("batch func time per channel ", time.time() - start)

                if payload is None or len(payload) == 0:
                    payload = None
                    # out seq number must be sequential
                    self.out_seq[target][channel] -= 1
                    continue
                
                if target_info.projection is not None:
                    # have to make sure the schema appears in the same order or the arrow lfight reader on the recevier might have problems.
                    # need to work out the set/list with the projection better.
                    payload = payload[sorted(list(target_info.projection))]
                
                client = self.flight_clients[target][channel]
                batches, my_format = convert_from_format(payload)

                start = time.time()
                
                # format (target id, target channel, source id, source channel, tag, format)
                #print("pushing to target",target,"channel",channel,"from source",self.id,"channel",self.channel,"tag",self.out_seq)
                upload_descriptor = pyarrow.flight.FlightDescriptor.for_command(pickle.dumps((target, channel, self.id, self.channel, self.out_seq[target][channel], my_format)))

                while True:
                    buf = pyarrow.allocate_buffer(0)
                    action = pyarrow.flight.Action("check_puttable", buf)
                    result = next(client.do_action(action))
                    #print(result.body.to_pybytes().decode("utf-8"))
                    if result.body.to_pybytes().decode("utf-8") != "True":
                        time.sleep(0.1)
                    else:
                        break

                assert len(batches) > 0, batches
                writer, _ = client.do_put(upload_descriptor, batches[0].schema)
                for batch in batches:
                    writer.write_batch(batch)
                writer.close()
            
                if PROFILE:
                    print("push time per channel ", time.time() - start)
       
        return True

    def done(self):

        if VERBOSE:
            print("IM DONE", self.id, self.channel, time.time())

        for target in self.alive_targets:
            for channel in self.alive_targets[target]:
                self.out_seq[target][channel] += 1
                #if VERBOSE:
                    #print("SAYING IM DONE TO", target, channel, "MY OUT SEQ", self.out_seq[target][channel])

                client = self.flight_clients[target][channel]
                payload = pickle.dumps((target, channel, self.id, self.channel, self.out_seq[target][channel], "done"))
                upload_descriptor = pyarrow.flight.FlightDescriptor.for_command(payload)
                writer, _ = client.do_put(upload_descriptor, pa.schema([]))
                writer.close()
                
        return True

class InputNode(Node):
    def __init__(self, id, channel) -> None:

        super().__init__( id, channel) 

        if VERBOSE:
            print("INPUT ACTOR LAUNCH", self.id)
        
    def ping(self):
        return True

    def initialize(self):
        pass
        
    def execute(self):

        pa.set_cpu_count(8)
        #self.con = duckdb.connect()
        #self.con.execute('PRAGMA threads=%d' % 8)
        self.input_generator = self.accessor.get_next_batch(self.channel, None)

        for pos, batch in self.input_generator:
            if batch is not None and len(batch) > 0:
                start = time.time()
                self.push(batch)
                if PROFILE:
                    print("pushing", time.time() - start)

        if VERBOSE:
            print("INPUT DONE", self.id, self.channel, time.time())
        self.done()

@ray.remote
class InputReaderNode(InputNode):
    def __init__(self, id, channel, accessor, num_channels) -> None:
        super().__init__(id, channel)
        self.accessor = accessor


class TaskNode(Node):
    def __init__(self, id, channel,  mapping, functionObject, parents) -> None:

        # id: int. Id of the node
        # channel: int. Channel of the node
        # streams: dictionary of logical_id : streams
        # mapping: the mapping between the name you assigned the stream to the actual id of the string.

        super().__init__(id, channel)

        self.id = id 
        self.parents = parents # dict of id -> dict of channel -> actor handles        
        self.functionObject = functionObject
        self.physical_to_logical_mapping = mapping


    def initialize(self):
        message = pyarrow.py_buffer(pickle.dumps((self.id,self.channel, {i:list(self.parents[i].keys()) for i in self.parents})))
        action = pyarrow.flight.Action("register_channel",message)
        result = next(self.flight_client.do_action(action))
        assert result.body.to_pybytes() == b'Channel registered'

    def get_batches(self):

        message = pyarrow.py_buffer(pickle.dumps((self.id, self.channel)))
        action = pyarrow.flight.Action("get_batches_info", message)
        result = next(self.flight_client.do_action(action))
        batch_info, should_terminate = pickle.loads(result.body.to_pybytes())
        assert type(batch_info) == dict

        return batch_info, should_terminate

    def schedule_for_execution(self, batch_info):

        parent, channel = max(batch_info, key=batch_info.get)
        length = batch_info[(parent,channel)]
        if length == 0:
            return None, None

        # now drain that source
        requests = {}
        for daparent, channel in batch_info.keys():
            if daparent != parent:
                continue
            requests[daparent, channel] = batch_info[daparent, channel]
        
        return parent, requests

@ray.remote
class NonBlockingTaskNode(TaskNode):
    def __init__(self, id, channel,  mapping, functionObject, parents) -> None:
        super().__init__(id, channel,  mapping, functionObject, parents)
    
    def execute(self):

        while True:

            # be nice. 
            time.sleep(0.001)
            pa.set_cpu_count(8)
            # self.con = duckdb.connect()
            batch_info, should_terminate = self.get_batches()
            if should_terminate:
                break
            # deque messages from the mailbox in a way that makes sense
            stream_id, requests = self.schedule_for_execution(batch_info)
            if stream_id is None:
                continue

            request = ((self.id, self.channel), requests)
            reader = self.flight_client.do_get(pyarrow.flight.Ticket(pickle.dumps(request)))

            batches = []
            while True:
                try:
                    chunk, metadata = reader.read_chunk()
                    batches.append(convert_to_format(chunk, metadata))
                except StopIteration:
                    break

            batches = [i for i in batches if i is not None]
            results = self.functionObject.execute( batches, self.physical_to_logical_mapping[stream_id], self.channel)
            
            # this is a very subtle point. You will only breakout if length of self.target, i.e. the original length of 
            # target list is bigger than 0. So you had somebody to send to but now you don't
            if type(results) == types.GeneratorType:
                for result in results:
                    break_out = False
                    if result is not None and len(self.targets) > 0:
                        if self.push(result) is False:
                            break_out = True
                            break
                    else:
                        pass
            else:
                break_out = False
                if results is not None and len(self.targets) > 0:
                    if self.push(results) is False:
                        break_out = True
                        break
                else:
                    pass
        
            if break_out:
                break
                    
        obj_done =  self.functionObject.done(self.channel) 

        if type(obj_done) == types.GeneratorType:
            for object in obj_done:
                self.push(object)
            del self.functionObject
            gc.collect()
        else:
            del self.functionObject
            gc.collect()
            if obj_done is not None:
                self.push(obj_done)
        if VERBOSE:
            print("TASK NODE DONE", self.id, self.channel, time.time())
        self.done()
    
@ray.remote
class BlockingTaskNode(TaskNode):
    def __init__(self, id, channel,  mapping, output_dataset, functionObject, parents, transform_fn = None) -> None:
        super().__init__(id, channel,  mapping, functionObject, parents)
        self.output_dataset = output_dataset
        self.object_count = 0 
        self.transform_fn = transform_fn

    # explicit override with error. Makes no sense to append to targets for a blocking node. Need to use the dataset instead.
    def append_to_targets(self,tup):
        raise Exception("Trying to stream from a blocking node")
    
    def execute(self):

        pa.set_cpu_count(8)
        spill_no = 0
        # self.con = duckdb.connect()

        add_tasks = []

        while True:

            batch_info, should_terminate = self.get_batches()
            if should_terminate:
                break
            # deque messages from the mailbox in a way that makes sense
            
            stream_id, requests = self.schedule_for_execution(batch_info)
            
            start = time.time()

            if stream_id is None:
                continue
            request = ((self.id, self.channel), requests)

            reader = self.flight_client.do_get(pyarrow.flight.Ticket(pickle.dumps(request)))

            #print("making reader", time.time() - start)
            start = time.time()

            batches = []
            while True:
                try:
                    #start = time.time()
                    chunk, metadata = reader.read_chunk()
                    #print("read time", time.time() - start)
                    #start = time.time()
                    batches.append(convert_to_format(chunk, metadata))
                    #print("convert time", time.time() - start)
                except StopIteration:
                    break
            
            #print("reading batches", time.time() - start)
            start = time.time()

            batches = [i for i in batches if i is not None]
            results = self.functionObject.execute( batches,self.physical_to_logical_mapping[stream_id], self.channel)

            #print("executing", time.time() - start)

            if self.transform_fn is not None:
                results = self.transform_fn(results)
            
            if results is not None and len(results) > 0:

                cursor = 0
                stride = 1000000
                while cursor < len(results):

                    if psutil.virtual_memory().percent < 70:
                        add_tasks.append(self.output_dataset.added_object.remote(self.channel, [ray.put(results[cursor: cursor + stride].to_arrow(), _owner = self.output_dataset)]))
                    else:
                        filename = "/data/spill-" + str(self.id) + "-" + str(self.channel) + "-" + str(spill_no) + ".parquet"
                        results[cursor: cursor + stride].write_parquet(filename)
                        add_tasks.append(self.output_dataset.added_parquet.remote(self.channel, filename))
                        spill_no += 1

                    cursor += stride
            else:
                pass
        
        obj_done =  self.functionObject.done(self.channel) 
        if obj_done is not None:
            assert type(obj_done) == polars.internals.DataFrame or type(obj_done) == types.GeneratorType
            if type(obj_done) == polars.internals.DataFrame:
                obj_done = [obj_done]

            for object in obj_done:
                if self.transform_fn is not None:
                    object = self.transform_fn(object)

                if object is not None:
                    assert type(object) == polars.internals.DataFrame

                    if psutil.virtual_memory().percent < 70:
                        add_tasks.append(self.output_dataset.added_object.remote(self.channel, [ray.put(object.to_arrow(), _owner = self.output_dataset)]))
                    else:
                        filename = "/data/spill-" + str(self.id) + "-" + str(self.channel) + "-" + str(spill_no) + ".parquet"
                        object.write_parquet(filename)
                        add_tasks.append(self.output_dataset.added_parquet.remote(self.channel, filename))
                        spill_no += 1

        del self.functionObject
        gc.collect()

        ray.get(add_tasks)
        ray.get(self.output_dataset.done_channel.remote(self.channel))
        
        self.done()
