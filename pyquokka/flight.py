import ast
import threading
import time

import pyarrow
import pyarrow.flight
import ray
from multiprocessing import Lock
import os, psutil

class FlightServer(pyarrow.flight.FlightServerBase):
    def __init__(self, host="localhost", location=None):
        super(FlightServer, self).__init__(location)
        self.flights = {}
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
        # self.flights_lock.acquire()
        for key, table in self.flights.items():
            
            descriptor = \
                pyarrow.flight.FlightDescriptor.for_command(key[1])
            yield self._make_flight_info(key, descriptor, table)
        #self.flights_lock.release()
        
        #self.flights_lock.release()

    def get_flight_info(self, context, descriptor):
        key = FlightServer.descriptor_to_key(descriptor)
        if key in self.flights:
            table = self.flights[key]
            return self._make_flight_info(key, descriptor, table)
        raise KeyError('Flight not found.')

    def do_put(self, context, descriptor, reader, writer):
        self.flights_lock.acquire()
        key = FlightServer.descriptor_to_key(descriptor)
        #print(key)
        self.flights[key] = reader.read_all()
        self.flights_lock.release()
        #print(self.flights[key])

    def do_get(self, context, ticket):
        self.flights_lock.acquire()
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            return None
        result = self.flights[key]
        del self.flights[key]
        self.flights_lock.release()
        return pyarrow.flight.RecordBatchStream(result)

    def list_actions(self, context):
        return [
            ("clear", "Clear the stored flights."),
            ("check_puttable","check if puttable"),
            ("shutdown", "Shut down this server."),
        ]

    def do_action(self, context, action):
        if action.type == "clear":
            self.flights.clear()
            yield pyarrow.flight.Result(pyarrow.py_buffer(b'Cleared!'))
        elif action.type == "clear_messages": # clears out messages but keeps all the data items.
            pass
        elif action.type == "check_puttable":
            cond = sum(self.flights[i].nbytes for i in self.flights) < self.mem_limit
            if not cond:
                print(self.flights)
            #cond = self.process.memory_info().rss < self.mem_limit
            yield pyarrow.flight.Result(pyarrow.py_buffer(bytes(str(cond), "utf-8")))
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

@ray.remote
class FlightServerWrapper():
    def __init__(self, host="localhost", location=None,
                 tls_certificates=None, verify_client=False,
                 root_certificates=None, auth_handler=None) -> None:
        self.host = host
        self.location = location
        
    def start_server(self):
        self.server = FlightServer(self.host, self.location)
        self.server.serve()

if __name__ == '__main__':
    server = FlightServer("0.0.0.0", location = "grpc+tcp://0.0.0.0:5005")
    server.serve()