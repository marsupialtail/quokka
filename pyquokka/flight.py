import threading
import time
import pyarrow
import pyarrow.flight
from multiprocessing import Lock
import os, psutil
import pickle

from collections import deque
import pyarrow.parquet as pq
import os
import polars

DEBUG = False
def print_if_debug(*x):
    if DEBUG:
        print(*x)


class DiskFile:
    def __init__(self,filename) -> None:
        self.filename = filename
    def delete(self):
        os.remove(self.filename)


class FlightServer(pyarrow.flight.FlightServerBase):
    def __init__(self, host="localhost", location=None):
        super(FlightServer, self).__init__(location)

        # we will have one DiskQueue per channel scheduled on the server
        # pros: easy to transiition, quick lookup for schedule_execution, can move one queue as unit
        # cons: might use too much RAM, having more things is always bad. 
        # might transition to having one DiskQueue or a meta-object like DiskQeuues in future

        # flights will be a dictionary for now name->(object, format). This should be called cache.
        self.flights = {}
        # self.flight_keys = polars.DataFrame(columns = ["source_actor_id", "source_channel_id", "seq", "target_actor_id", "partition_fn", "target_channel_id"])
        self.flight_keys = None
        self.flights_lock = Lock()

        # hbq will be a dictionary for now name->(object, format). This should be called push storage.
        self.hbq = {}
        # self.hbq_keys = polars.DataFrame(columns = ["source_actor_id", "source_channel_id", "seq", "target_actor_id", "partition_fn", "target_channel_id"])
        self.hbq_keys = None
        self.hbq_lock = Lock()

        self.host = host
        self.config_dict = {"mem_limit" : 0.25, "max_batches": 10}
        self.process = psutil.Process(os.getpid())
        #self.log_file = open("/home/ubuntu/flight-log","w")

    @classmethod
    def descriptor_to_key(self, descriptor):
        return (descriptor.descriptor_type.value, descriptor.command,
                tuple(descriptor.path or tuple()))

    # def _make_flight_info(self, key, descriptor, table):
    # this is wrong, table.schema won't happen, table is a list!
    #     location = pyarrow.flight.Location.for_grpc_tcp(
    #         self.host, self.port)
    #     endpoints = [pyarrow.flight.FlightEndpoint(repr(key), [location]), ]
    #     # not going to try to get the size, just return 0. not using it anyways.
    #     return pyarrow.flight.FlightInfo(table.schema,
    #                                      descriptor, endpoints,
    #                                      table.num_rows, 0)

    # def list_flights(self, context, criteria):
    #     self.flights_lock.acquire()
    #     for key, table in self.flights.items():
            
    #         descriptor = \
    #             pyarrow.flight.FlightDescriptor.for_command(key[1])
    #         yield self._make_flight_info(key, descriptor, table)
    #     self.flights_lock.release()
        
    # def get_flight_info(self, context, descriptor):
    #     key = FlightServer.descriptor_to_key(descriptor)
    #     if key in self.flights:
    #         table = self.flights[key]
    #         return self._make_flight_info(key, descriptor, table)
    #     raise KeyError('Flight not found.')

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
        
        if is_push:

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

        else:

            source_actor_id, source_channel_id, seq = name
            new_row = polars.from_dict({"source_actor_id": [source_actor_id], "source_channel_id":[source_channel_id], "seq":[seq]})

            batches = []
            while True:
                try:
                    batches.append(reader.read_chunk().data)
                except StopIteration:
                    break
            assert len(batches) > 0
            data = batches

            self.hbq_lock.acquire()
            if name in self.hbq:
                print("duplicate data detected")
                assert data == self.hbq[name][0], "duplicate data not the same"
            else:

                self.hbq[name] = (data, my_format)
                
                if self.hbq_keys is None:
                    self.hbq_keys = new_row
                else:
                    self.hbq_keys.vstack(new_row, in_place = True)
            
            self.hbq_lock.release()
        

    @staticmethod
    def number_batches(batches):
        for name, batch in batches:
            print_if_debug(batch[0])
            for b in batch[0]:
                yield b , pickle.dumps((name, batch[1]))

    def do_get(self, context, ticket):

        mode, actor_id, channel_id, input_requirements, exact = pickle.loads(ticket.ticket)

        if mode == "hbq":

            batches = []

            self.hbq_lock.acquire()

            for seq in input_requirements:
                assert (actor_id, channel_id, seq) in self.hbq
                batches.append(((actor_id, channel_id, seq) , self.hbq[actor_id, channel_id, seq]))
            
            self.hbq_lock.release()

        elif mode == "cache":

            batches = []

            self.flights_lock.acquire()
            if not exact:
                # you might want to make sure that you don't return things out of order here or have gaps in the things you do return

                if self.flight_keys is None or len(self.flight_keys) == 0:
                    print_if_debug("flights empty!")
                    self.flights_lock.release()
                    return pyarrow.flight.GeneratorStream(pyarrow.schema([]), self.number_batches(batches))


                exec_plan = self.flight_keys.lazy().filter((polars.col("target_actor_id") == actor_id) & (polars.col("target_channel_id") == channel_id))\
                                                .join(input_requirements.lazy(), left_on= ["source_actor_id", "source_channel_id"], right_on=["source_actor_id", "source_channel_id"])\
                                                .filter(polars.col("seq") >= polars.col("min_seq")).limit(self.config_dict["max_batches"])\
                                                .select(["source_actor_id", "source_channel_id", "seq", "partition_fn", "min_seq"])\
                                                .collect()
                
                print_if_debug(self.flight_keys, input_requirements, exec_plan)

                if len(exec_plan) == 0:
                    self.flights_lock.release()
                    return pyarrow.flight.GeneratorStream(pyarrow.schema([]), self.number_batches(batches))

                # pick the source with the most batches to give.
                source_actor_id = exec_plan.lazy().groupby("source_actor_id").agg(polars.count()).sort("count",reverse=True).limit(1).collect().to_dicts()[0]["source_actor_id"]
                
                # we need to sort the exec plan!
                # exec_plan = exec_plan.filter(polars.col("source_actor_id") == source_actor_id).sort(["source_channel_id","seq"])

                exec_plan_candidate = exec_plan.filter(polars.col("source_actor_id") == source_actor_id)

                for df in exec_plan_candidate.groupby("source_channel_id"):
                    source_channel_id = df["source_channel_id"][0]
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

                # print_if_debug("exec plan", exec_plan)
                # print_if_debug("current flights", self.flight_keys)

                # # do a check here that the returned input batches are contiguous. there could be failure cases but this check should handle most of it.
                # z = exec_plan.groupby("source_channel_id").agg([polars.max("seq"), polars.count(), polars.max("min_seq")])
                # print_if_debug(z)
                # assert (z["seq"] == z["min_seq"] + z["count"] - 1).all(), "Failed check, returned sequence numbers likely not contiguous"
                                                
                # for name_dict in exec_plan.to_dicts():
                #     name = (name_dict["source_actor_id"], name_dict["source_channel_id"], name_dict["seq"], actor_id, name_dict["partition_fn"], channel_id)
                #     print_if_debug(self.flights)
                #     assert name in self.flights , "exec plan name not in flights"
                #     batches.append((name, self.flights[name]))
                
                if len(batches) == 0:
                    self.flights_lock.release()
                    return pyarrow.flight.GeneratorStream(pyarrow.schema([]), self.number_batches(batches))

            else:

                if self.flight_keys is None or len(self.flight_keys) == 0:
                    print_if_debug("flights empty!")
                    self.flights_lock.release()
                    return pyarrow.flight.GeneratorStream(pyarrow.schema([]), self.number_batches([]))

                partition_fn = 0 # TODO: only support 1 partition fn right now
                source_actor_id, source_channel_seqs = pickle.loads(input_requirements)

                for source_channel_id in source_channel_seqs:
                    for seq in source_channel_seqs[source_channel_id]:
                        name = (source_actor_id, source_channel_id, seq, actor_id, partition_fn, channel_id)
                        if name not in self.flights:
                            self.flights_lock.release()
                            return pyarrow.flight.GeneratorStream(pyarrow.schema([]), self.number_batches([]))
                        batches.append((name, self.flights[name]))
 
                print_if_debug("current flights", self.flight_keys)
                if len(batches) == 0:
                    self.flights_lock.release()
                    return pyarrow.flight.GeneratorStream(pyarrow.schema([]), self.number_batches(batches))

            self.flights_lock.release()
            

        else:
            raise Exception("mode not supported")
        
        print_if_debug(batches)
        return pyarrow.flight.GeneratorStream(batches[0][1][0][0].schema, self.number_batches(batches))

    def list_actions(self, context):
        return [
            ("clear", "Clear the stored flights."),
            ("check_puttable","check if puttable"),
            ("shutdown", "Shut down this server."),
            ("get_hbq_info", "get information of hbq"),
            ("get_flights_info", "get information of flights"),
            ("cache_garbage_collect", "garbage collect from cache"),
            ("garbage_collect", "garbage collect hbq")
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
                self.config_dict[key] = config_dict[key]
            cond = True
            yield pyarrow.flight.Result(pyarrow.py_buffer(bytes(str(cond), "utf-8")))
        elif action.type == "check_puttable":
            # puts should now be blocking due to the DiskQueue!
            # cond = True #sum(self.flights[i].nbytes for i in self.flights) < self.mem_limit
            cond = (psutil.virtual_memory().available / psutil.virtual_memory().total) > self.config_dict["mem_limit"]
            yield pyarrow.flight.Result(pyarrow.py_buffer(bytes(str(cond), "utf-8")))
    
        elif action.type == "get_hbq_info":

            yield pyarrow.flight.Result(pyarrow.py_buffer(pickle.dumps((self.hbq_keys, list(self.hbq.keys())))))
        
        elif action.type == "get_flights_info":

            yield pyarrow.flight.Result(pyarrow.py_buffer(pickle.dumps((self.flight_keys, list(self.flights.keys())))))
        
        elif action.type == "cache_garbage_collect":

            gcable = pickle.loads(action.body.to_pybytes())

            self.flights_lock.acquire()
            for tup in gcable:
                assert tup in self.flights, "tuple not in flights"
                del self.flights[tup]

            gcable = polars.DataFrame( gcable, columns= ["source_actor_id", "source_channel_id", "seq", "target_actor_id",\
                 "partition_fn", "target_channel_id"], orient='row')
            self.flight_keys = self.flight_keys.join(gcable, on = ["source_actor_id", "source_channel_id", "seq", "target_actor_id",\
                 "partition_fn", "target_channel_id"], how = "anti")
            
            self.flights_lock.release()
                
            yield pyarrow.flight.Result(pyarrow.py_buffer(bytes(str(True), "utf-8")))

        elif action.type == "garbage_collect":

            gcable = pickle.loads(action.body.to_pybytes())

            self.hbq_lock.acquire()
            for tup in gcable:
                assert tup in self.flights, "tuple not in flights"
                del self.hbq[tup]

            gcable = polars.DataFrame( gcable, columns= ["source_actor_id", "source_channel_id", "seq"], orient='row')
            self.hbq_keys = self.hbq_keys.join(gcable, on = ["source_actor_id", "source_channel_id", "seq"], how = "anti")

            self.hbq_lock.release()

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