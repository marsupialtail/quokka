import redis
import pyarrow
import pyarrow.flight
from pyquokka.tables import * 

class Debugger:
    def __init__(self, address) -> None:

        self.r = redis.Redis(address, 6800, db = 0)
        self.CT = CemetaryTable()
        self.NOT = NodeObjectTable()
        self.PT = PresentObjectTable()
        self.NTT = NodeTaskTable()
        self.GIT = GeneratedInputTable()
        self.EST = ExecutorStateTable()
        self.LT = LineageTable()
        self.DST = DoneSeqTable()
        self.LCT = LastCheckpointTable()
        self.CLT = ChannelLocationTable()

        self.undone = set()
        self.actor_channel_locations = {}
    
    def dump_redis_state(self, path):
        state = {"CT": self.CT.to_dict(self.r),
        "NOT": self.NOT.to_dict(self.r),
        "PT": self.PT.to_dict(self.r),
        "NTT": self.NTT.to_dict(self.r),
        "GIT": self.GIT.to_dict(self.r),
        "EST": self.EST.to_dict(self.r),
        "LT": self.LT.to_dict(self.r),
        "DST": self.DST.to_dict(self.r),
        "LCT": self.LCT.to_dict(self.r),
        "CLT": self.CLT.to_dict(self.r)}
        flight_client = pyarrow.flight.connect("grpc://0.0.0.0:5005")
        buf = pyarrow.allocate_buffer(0)
        action = pyarrow.flight.Action("get_flights_info", buf)
        result = next(flight_client.do_action(action))
        state["flights"] = pickle.loads(result.body.to_pybytes())[1]

        pickle.dump(state, open(path,"wb"))