import polars
import threading
import time
from pyquokka.diskqueue import DiskQueue
import pyarrow
import pyarrow.flight
from multiprocessing import Lock
import os, psutil
import pickle

class FlightServer(pyarrow.flight.FlightServerBase):
    def __init__(self, host="localhost", location=None):
        super(FlightServer, self).__init__(location)

        # we will have one DiskQueue per channel scheduled on the server
        # pros: easy to transiition, quick lookup for schedule_execution, can move one queue as unit
        # cons: might use too much RAM, having more things is always bad. 
        # might transition to having one DiskQueue or a meta-object like DiskQeuues in future

        # flights will be a dictionary: (node, channel) -> DiskQueue(parents)
        self.flights = {}
        # dict: (target_node,target_channel) -> dict of (source_node, source_channel) -> int, strictly used for debugging
        self.latest_input_received = {}
        self.host = host
        self.flights_lock = Lock()
        self.mem_limit = 1e9
        self.process = psutil.Process(os.getpid())

    @classmethod
    def descriptor_to_key(self, descriptor):
        return (descriptor.descriptor_type.value, descriptor.command,
                tuple(descriptor.path or tuple()))

    def _make_flight_info(self, key, descriptor, table):
        
        location = pyarrow.flight.Location.for_grpc_tcp(
            self.host, self.port)
        endpoints = [pyarrow.flight.FlightEndpoint(repr(key), [location]), ]
        # not going to try to get the size, just return 0. not using it anyways.
        return pyarrow.flight.FlightInfo(table.schema,
                                         descriptor, endpoints,
                                         table.num_rows, 0)

    def list_flights(self, context, criteria):
        self.flights_lock.acquire()
        for key, table in self.flights.items():
            
            descriptor = \
                pyarrow.flight.FlightDescriptor.for_command(key[1])
            yield self._make_flight_info(key, descriptor, table)
        self.flights_lock.release()
        
    def get_flight_info(self, context, descriptor):
        key = FlightServer.descriptor_to_key(descriptor)
        if key in self.flights:
            table = self.flights[key]
            return self._make_flight_info(key, descriptor, table)
        raise KeyError('Flight not found.')

    def _all_done(self, target_id, target_channel):
        if not all(k=="done" for k in self.latest_input_received[target_id, target_channel].values()):
            return False
        return True

    def do_put(self, context, descriptor, reader, writer):
        key = FlightServer.descriptor_to_key(descriptor)
        target_id, target_channel, source_id, source_channel, out_seq, my_format = pickle.loads(key[1])
        assert (target_id, target_channel) in self.flights

        # don't really need any locks with the latest_input_received because there will be no conflict.
        if self.latest_input_received[target_id, target_channel][source_id, source_channel] == "done":
            print(source_id, source_channel , self.parents)
            print("this channel has already received the done signal. stop wasting your breath.")
            raise Exception

        if out_seq <= self.latest_input_received[target_id, target_channel][source_id, source_channel]:
            print("rejected an input stream's tag smaller than or equal to latest input received. input tag", out_seq, "current latest input received", self.latest_input_received[target_id, target_channel][source_id, source_channel])
            raise Exception

        # this won't be required anymore in quokka 2.0
        # if out_seq > self.latest_input_received[target_id, target_channel][source_id, source_channel] + 1:
        #     print("DROPPING INPUT. THIS IS A FUTURE INPUT THAT WILL BE RESENT (hopefully)", tag, stream_id, channel, "current tag", self.latest_input_received[(stream_id,channel)])
        #     raise Exception
            
        if my_format == "done":
            self.latest_input_received[target_id, target_channel][source_id, source_channel] = "done"
            return
        else:
            self.latest_input_received[target_id, target_channel][source_id, source_channel] = out_seq

        #print(key)
        self.flights_lock.acquire()
        self.flights[target_id, target_channel].append((source_id, source_channel) ,reader.read_chunk().data, my_format)
        self.flights_lock.release()
        #print(self.flights[key])

    @staticmethod
    def number_batches(batches):
        for batch, format in batches:
            yield batch, format

    def do_get(self, context, ticket):

        self.flights_lock.acquire()
        request = pickle.loads(ticket.ticket)
        # this is going to be a tuple ((target_id, target_channel),  dictionary of (source_id, source_channel) -> num) 
        assert type(request) == tuple and type(request[1]) == dict

        target_id, target_channel = request[0]
        requests = request[1]
        batches = []

        for source_id, source_channel in requests:
            num = requests[source_id, source_channel]
            batches.extend(self.flights[target_id, target_channel].get_batches_for_key((source_id, source_channel), num = num))

        self.flights_lock.release()
        return pyarrow.flight.GeneratorStream(batches[0][0].schema, self.number_batches(batches))

    def list_actions(self, context):
        return [
            ("clear", "Clear the stored flights."),
            ("check_puttable","check if puttable"),
            ("shutdown", "Shut down this server."),
            ("get_batches_info", "get information of batches")
        ]

    def do_action(self, context, action):
        if action.type == "clear":
            self.flights.clear()
            # clear out the datasets that you store, not implemented yet.
            yield pyarrow.flight.Result(pyarrow.py_buffer(b'Cleared!'))
        elif action.type == "clear_messages": # clears out messages but keeps all the data items.
            self.flights.clear()
            yield pyarrow.flight.Result(pyarrow.py_buffer(b'Cleared!'))
        elif action.type == "check_puttable":
            # puts should now be blocking due to the DiskQueue!
            cond = True #sum(self.flights[i].nbytes for i in self.flights) < self.mem_limit
            #cond = self.process.memory_info().rss < self.mem_limit
            yield pyarrow.flight.Result(pyarrow.py_buffer(bytes(str(cond), "utf-8")))
        elif action.type == "get_batches_info":

            # this is the format of the key: (target, channel, self.id, self.channel, self.out_seq, my_format)
            action_data = pickle.loads(action.body.to_pybytes())
            node, channel = action_data
            # you will return the length of each key of the DiskQueue of that node, channel pair
            result = self.flights[node,channel].get_all_len()
            if max(result.values()) ==0 and self._all_done(node, channel):
                should_terminate = True
            else:
                should_terminate = False
            yield pyarrow.flight.Result(pyarrow.py_buffer(pickle.dumps((result, should_terminate))))
        
        elif action.type == "register_channel":

            # add a channel to self.flights
            action_data = pickle.loads(action.body.to_pybytes())
            node, channel, parents = action_data
            #print(node, channel, parents)
            self.flights[node, channel] = DiskQueue(parents,"spill-" + str(node) + "-" + str(channel), "/data")
            self.latest_input_received[node, channel] = {(i,j):0 for i in parents for j in parents[i]}
            yield pyarrow.flight.Result(pyarrow.py_buffer(b'Channel registered'))

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
        self.shutdown()

if __name__ == '__main__':
    server = FlightServer("0.0.0.0", location = "grpc+tcp://0.0.0.0:5005")
    server.serve()