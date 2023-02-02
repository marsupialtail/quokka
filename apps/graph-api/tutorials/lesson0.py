from pyquokka.quokka_runtime import TaskGraph
from pyquokka.utils import LocalCluster
from pyquokka.executors import Executor
from pyquokka.target_info import TargetInfo, PassThroughPartitioner
from pyquokka.placement_strategy import SingleChannelStrategy
import sqlglot
import time
import polars

cluster = LocalCluster()

# this dataset will generate a sequence of numbers, from 0 to limit. 
class SimpleDataset:
    def __init__(self, limit) -> None:
        
        self.limit = limit
        self.num_channels = None

    def get_own_state(self, num_channels):
        channel_info = {}
        for channel in range(num_channels):
            channel_info[channel] = [i for i in range(channel, self.limit, num_channels)]
        return channel_info

    def execute(self, channel, state = None):
        curr_number = state
        return None, polars.DataFrame({"number": [curr_number]})

class AddExecutor(Executor):
    def __init__(self) -> None:
        self.sum = None
    def execute(self,batches,stream_id, channel):
        for batch in batches:
            self.sum = polars.from_arrow(batch) if self.sum is None else self.sum + polars.from_arrow(batch)

    def done(self,channel):
        print("I am executor ", channel, " my sum is ", self.sum)
        return self.sum

task_graph = TaskGraph(cluster)
reader = SimpleDataset(80)
numbers = task_graph.new_input_reader_node(reader)

executor = AddExecutor()
sum = task_graph.new_blocking_node({0:numbers},executor, placement_strategy = SingleChannelStrategy(), source_target_info={0:TargetInfo(partitioner = PassThroughPartitioner(), 
                                    predicate = sqlglot.exp.TRUE,
                                    projection = None,
                                    batch_funcs = [])})

task_graph.create()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(sum.to_df())
