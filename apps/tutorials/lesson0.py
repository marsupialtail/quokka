from pyquokka.quokka_runtime import TaskGraph
from pyquokka.utils import LocalCluster
from pyquokka.executors import Executor
import time
import ray

cluster = LocalCluster()

# this dataset will generate a sequence of numbers, from 0 to limit. Channel 
class SimpleDataset:
    def __init__(self, limit) -> None:
        
        self.limit = limit
        self.num_channels = None

    def set_num_channels(self, num_channels):
        self.num_channels = num_channels

    def get_next_batch(self, mapper_id, pos=None):
        # let's ignore the keyword pos = None, which is only relevant for fault tolerance capabilities.
        assert self.num_channels is not None
        curr_number = mapper_id
        while curr_number < self.limit:
            yield curr_number, curr_number
            curr_number += self.num_channels

class AddExecutor(Executor):
    def __init__(self) -> None:
        self.sum = 0
    def execute(self,batches,stream_id, executor_id):
        for batch in batches:
            assert type(batch) == int
            self.sum += batch
    def done(self,executor_id):
        print("I am executor ", executor_id, " my sum is ", self.sum)
        return self.sum

task_graph = TaskGraph(cluster)
reader = SimpleDataset(80)
numbers = task_graph.new_input_reader_node(reader)

executor = AddExecutor()
sum = task_graph.new_blocking_node({0:numbers},executor)

task_graph.create()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(sum.to_list())
