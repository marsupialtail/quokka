import threading
import time
import pyarrow
import pyarrow.flight
from multiprocessing import Lock
import os, psutil
import pickle
import os
import polars

DEBUG = False
def print_if_debug(*x):
    if DEBUG:
        print(*x)

class FlightServer(pyarrow.flight.FlightServerBase):
    def __init__(self, host="localhost", location=None):
        super(FlightServer, self).__init__(location)

        # flights will be a dictionary for now name->(object, format). This should be called cache.
        self.flights = {}
        # self.flight_keys = polars.DataFrame(columns = ["source_actor_id", "source_channel_id", "seq", "target_actor_id", "partition_fn", "target_channel_id"])
        self.flight_keys = None
        self.flights_lock = Lock()

        self.host = host
        self.config_dict = {"mem_limit" : 0.25, "max_batches": 10, "actor_stages": None, "batch_attempt": 5}
        self.process = psutil.Process(os.getpid())
        self.requests_hashes = {}
        #self.log_file = open("/home/ubuntu/flight-log","w")

    @classmethod
    def descriptor_to_key(self, descriptor):
        return (descriptor.descriptor_type.value, descriptor.command,
                tuple(descriptor.path or tuple()))

    def _all_done(self, target_id, target_channel):
        if not all(k=="done" for k in self.latest_input_received[target_id, target_channel].values()):
            #self.log_file.write(str(self.latest_input_received) + "\n")
            #self.log_file.flush()
            return False
        return True

    def do_put(self, context, descriptor, reader, writer):
        key = FlightServer.descriptor_to_key(descriptor)
        is_push, name, my_format = pickle.loads(key[1])
        
        print_if_debug(name)        
        assert is_push

        source_actor_id, source_channel_id, seq, target_actor_id, partition_fn, target_channel_id = name
        new_row = polars.from_dict({"source_actor_id": [source_actor_id], "source_channel_id":[source_channel_id], "seq":[seq], "target_actor_id":[target_actor_id],
            "partition_fn":[partition_fn], "target_channel_id":[target_channel_id]})

        batches = []
        while True:
            try:
                batches.append(reader.read_chunk().data)
            except StopIteration:
                break
        
        assert len(batches) > 0
        data = batches

        print_if_debug('acquiring flight lock')
        self.flights_lock.acquire()
        if name in self.flights:
            # print("duplicate data detected")
            # assert data  == self.flights[name][0], "duplicate data not the same"
            # important bug fix: the same name could be pushed again with different data.
            # in case of failure upstream after push and before commit, the object with that name will be reconstructed with different inputs.
            # we want to accept the most up to date version!
            self.flights[name] = (data, my_format)
        
        else:
            self.flights[name] = (data, my_format)
            
            # very important this happens after update self.flights, due to locking strategy in do_get.
            if self.flight_keys is None:
                self.flight_keys = new_row
            else:
                self.flight_keys.vstack(new_row,in_place=True)
        self.flights_lock.release()
        print_if_debug('flight lock released')

    @staticmethod
    def number_batches(batches, fake_row = None):
        for name, batch in batches:
            for b in batch[0]:
                if "__empty__" in b.schema.names:
                    # print("YIELDING FAKE ROW!")
                    yield fake_row, pickle.dumps((name, batch[1]))
                else:
                    yield b, pickle.dumps((name, batch[1]))

    def do_get(self, context, ticket):

        mode, actor_id, channel_id, input_requirements, exact, sorted_reqs = pickle.loads(ticket.ticket)

        assert mode == "cache"
        batches = []

        self.flights_lock.acquire()
        if not exact:
            # you might want to make sure that you don't return things out of order here or have gaps in the things you do return

            if self.flight_keys is None or len(self.flight_keys) == 0:
                print_if_debug("flights empty!")
                self.flights_lock.release()
                return pyarrow.flight.GeneratorStream(pyarrow.schema([]), self.number_batches([]))


            exec_plan = self.flight_keys.lazy().filter((polars.col("target_actor_id") == actor_id) & (polars.col("target_channel_id") == channel_id))\
                                            .join(input_requirements.lazy(), left_on= ["source_actor_id", "source_channel_id"], right_on=["source_actor_id", "source_channel_id"])\
                                            .join(self.config_dict["actor_stages"].lazy(), left_on="source_actor_id", right_on="actor_id")\
                                            .filter(polars.col("seq") >= polars.col("min_seq")).collect()

            # picking the minimum stage in the flight server should be sufficient to ensure that we don't process things out of stage order
            # since the global stage variable only increases in normal operation IF all the outputs of a particular stage have been 
            # successfully received at their target flight servers. Since the lineage is determined in normal operation, this should ensure that 
            # the determined lineage is correct. Of course upon recovery we assume this lineage is correct and don't bother checking stages.
            # there could be some subtle bugs here but we can worry about that once we raise $50M from Sequoia. Spark still has fault tolerance bugs, right?

            min_stage = exec_plan["stage"].min()
            exec_plan = exec_plan.filter(polars.col("stage") == min_stage).sort("seq").limit(self.config_dict["max_batches"])\
                                            .select(["source_actor_id", "source_channel_id", "seq", "partition_fn", "min_seq"])                
            if len(exec_plan) == 0:
                self.flights_lock.release()
                return pyarrow.flight.GeneratorStream(pyarrow.schema([]), self.number_batches([]))
            
            # try to accumulate more batches in one go before returning
            if len(exec_plan) < self.config_dict["batch_attempt"]:
                if ticket.ticket not in self.requests_hashes:
                    self.requests_hashes[ticket.ticket] = len(exec_plan)
                    self.flights_lock.release()
                    return pyarrow.flight.GeneratorStream(pyarrow.schema([]), self.number_batches([]))
                elif ticket.ticket in self.requests_hashes and len(exec_plan) > self.requests_hashes[ticket.ticket]:
                    self.requests_hashes[ticket.ticket] = len(exec_plan)
                    self.flights_lock.release()
                    return pyarrow.flight.GeneratorStream(pyarrow.schema([]), self.number_batches([]))
                else:
                    pass # didn't get any more batches for this request, maybe it's the last seq? just do it.

            # pick the source with the most batches to give.
            source_actor_id = exec_plan.groupby("source_actor_id").agg(polars.count()).sort("count",descending=True).limit(1).to_dicts()[0]["source_actor_id"]
            
            # we need to sort the exec plan!
            # exec_plan = exec_plan.filter(polars.col("source_actor_id") == source_actor_id).sort(["source_channel_id","seq"])

            exec_plan_candidate = exec_plan.filter(polars.col("source_actor_id") == source_actor_id)

            if sorted_reqs is None or source_actor_id not in sorted_reqs:

                # we are going to return batches channel contiguous, i.e. 1-2, 1-3, 1-4, 2-1, 2-2, etc.
                for source_channel_id, df in exec_plan_candidate.groupby("source_channel_id"):
                    seqs = sorted(df["seq"].to_list())
                    min_seq = df["min_seq"][0]
                    last_seq = min_seq
                    for seq in seqs:
                        if seq == last_seq:
                            name = (source_actor_id, source_channel_id, seq, actor_id, 0, channel_id)
                            assert name in self.flights
                            batches.append((name, self.flights[name]))
                            last_seq += 1
                        else:
                            break
                
            else:

                # we have to return batches strided:  2-1, 3-1, 1-2, 2-2, ...
                # first we need to figure out which channel we need to start from from the input reqs
                # print(input_requirements, source_actor_id)
                input_requirements = input_requirements.filter(polars.col("source_actor_id") == source_actor_id)
                stuff = input_requirements.sort("source_channel_id")\
                                        .with_columns((input_requirements["min_seq"] - input_requirements["min_seq"].shift(1)).alias("lag"))\
                                        .filter(polars.col("lag") == -1)
                if len(stuff) == 0:
                    # we are starting from the beginning
                    start_channel_id = input_requirements.sort("source_channel_id").to_dicts()[0]["source_channel_id"]
                    # assert start_channel_id == 0, "start channel id must be zero"
                    curr_seq_no = input_requirements.sort("source_channel_id").to_dicts()[0]["min_seq"]
                else:
                    assert len(stuff) == 1, "must have only one row on the edge"

                    # using to_dicts()[0][key] results in unexplained errors where the dict might be empty even though len(stuff) == 1, 
                    # unable to reproduce in standalone script.
                    start_channel_id = stuff["source_channel_id"][0]
                    curr_seq_no = stuff["min_seq"][0]

                total_source_channels = sorted_reqs[source_actor_id]
                # now we are going to go up the required batches until we hit one that's not in the flight_keys

                while True:
                    leave = False
                    for curr_channel in range(start_channel_id, total_source_channels):
                        name = (source_actor_id, curr_channel, curr_seq_no, actor_id, 0, channel_id)
                        if name in self.flights:
                            # print(name)
                            batches.append((name, self.flights[name]))
                        else:
                            leave = True
                            break
                    if leave:
                        break
                    curr_seq_no += 1
                    start_channel_id = 0
            
            if len(batches) == 0:
                self.flights_lock.release()
                return pyarrow.flight.GeneratorStream(pyarrow.schema([]), self.number_batches([]))

        else:

            if self.flight_keys is None or len(self.flight_keys) == 0:
                print_if_debug("flights empty!")
                self.flights_lock.release()
                return pyarrow.flight.GeneratorStream(pyarrow.schema([]), self.number_batches([]))

            partition_fn = 0 # TODO: only support 1 partition fn right now
            source_actor_id, source_channel_seqs = pickle.loads(input_requirements)

            if sorted_reqs is None or source_actor_id not in sorted_reqs:
                for source_channel_id in source_channel_seqs:
                    for seq in source_channel_seqs[source_channel_id]:
                        name = (source_actor_id, source_channel_id, seq, actor_id, partition_fn, channel_id)
                        if name not in self.flights:
                            self.flights_lock.release()
                            return pyarrow.flight.GeneratorStream(pyarrow.schema([]), self.number_batches([]))
                        batches.append((name, self.flights[name]))
            else:
                name_list = []
                for source_channel_id in source_channel_seqs:
                    for seq in source_channel_seqs[source_channel_id]:
                        name = (source_actor_id, source_channel_id, seq, actor_id, partition_fn, channel_id)
                        if name not in self.flights:
                            self.flights_lock.release()
                            return pyarrow.flight.GeneratorStream(pyarrow.schema([]), self.number_batches([]))
                        name_list.append(name)
                name_list = polars.DataFrame(name_list, columns = ["source_actor_id", "source_channel_id", "seq", "actor_id", "partition_fn", "channel_id"], orient="row")
                name_list = name_list.sort(["seq","source_channel_id"])
                for name in name_list.to_dicts():
                    name = (name["source_actor_id"], name["source_channel_id"], name["seq"], name["actor_id"], name["partition_fn"], name["channel_id"])
                    batches.append((name, self.flights[name]))

            print_if_debug("current flights", self.flight_keys)
            if len(batches) == 0:
                self.flights_lock.release()
                return pyarrow.flight.GeneratorStream(pyarrow.schema([]), self.number_batches([]))

        self.flights_lock.release()
        
        fake_row = None
        schema = None
        for name, batch in batches:
            for b in batch[0]:
                if "__empty__" not in b.schema.names:
                    schema = b.schema
                    fake_row = b[:0]
        # this means that all the batches are actually empty!
        # we need to send some actual data back or else arrow flight will complain
        if fake_row is None:
            fake_row = pyarrow.RecordBatch.from_pydict({"__empty__":[]})
            schema = fake_row.schema
        return pyarrow.flight.GeneratorStream(schema, self.number_batches(batches, fake_row = fake_row))

    def list_actions(self, context):
        return [
            ("clear", "Clear the stored flights."),
            ("check_puttable","check if puttable"),
            ("shutdown", "Shut down this server."),
            ("get_flights_info", "get information of flights"),
            ("cache_garbage_collect", "garbage collect from cache")
        ]

    def do_action(self, context, action):
        if action.type == "clear":
            self.flight_keys = None
            self.flights.clear()
            # clear out the datasets that you store, not implemented yet.
            cond = True
            yield pyarrow.flight.Result(pyarrow.py_buffer(bytes(str(cond), "utf-8")))
        elif action.type == "set_configs":
            config_dict = pickle.loads(action.body.to_pybytes())
            for key in config_dict:
                assert key in self.config_dict, "got an unrecognized Flight server config"
                if key == "actor_stages":
                    self.config_dict[key] = polars.DataFrame(list(config_dict[key].items()), orient = "row", schema = ["actor_id","stage"])
                else:
                    self.config_dict[key] = config_dict[key]

            cond = True
            yield pyarrow.flight.Result(pyarrow.py_buffer(bytes(str(cond), "utf-8")))
        elif action.type == "check_puttable":
            # puts should now be blocking due to the DiskQueue!
            # cond = True #sum(self.flights[i].nbytes for i in self.flights) < self.mem_limit
            cond = (psutil.virtual_memory().available / psutil.virtual_memory().total) > self.config_dict["mem_limit"]
            yield pyarrow.flight.Result(pyarrow.py_buffer(bytes(str(cond), "utf-8")))
        
        elif action.type == "get_flights_info":

            yield pyarrow.flight.Result(pyarrow.py_buffer(pickle.dumps((self.flight_keys, list(self.flights.keys())))))
        
        elif action.type == "cache_garbage_collect":

            gcable = pickle.loads(action.body.to_pybytes())

            self.flights_lock.acquire()
            for tup in gcable:
                assert tup in self.flights, "tuple not in flights"
                del self.flights[tup]

            gcable = polars.DataFrame( gcable, schema= ["source_actor_id", "source_channel_id", "seq", "target_actor_id",\
                 "partition_fn", "target_channel_id"], orient='row')
            self.flight_keys = self.flight_keys.join(gcable, on = ["source_actor_id", "source_channel_id", "seq", "target_actor_id",\
                 "partition_fn", "target_channel_id"], how = "anti")
            
            self.flights_lock.release()
                
            yield pyarrow.flight.Result(pyarrow.py_buffer(bytes(str(True), "utf-8")))

        elif action.type == "healthcheck":
            pass
        elif action.type == "shutdown":
            yield pyarrow.flight.Result(pyarrow.py_buffer(b'Shutdown!'))
            # Shut down on background thread to avoid blocking current
            # request
            threading.Thread(target=self._shutdown).start()
        else:
            raise KeyError("Unknown action {!r}".format(action.type))

    def _shutdown(self):
        """Shut down after a delay."""
        print("Server is shutting down...")
        time.sleep(2)
        #self.log_file.close()
        self.shutdown()

if __name__ == '__main__':
    server = FlightServer("0.0.0.0", location = "grpc+tcp://0.0.0.0:5005")
    server.serve()