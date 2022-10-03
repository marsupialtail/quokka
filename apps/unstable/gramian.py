import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.dataset import InputHDF5Dataset, InputDiskHDF5Dataset
import ray
import numpy as np
from pyquokka.utils import LocalCluster, QuokkaClusterManager
import os
manager = QuokkaClusterManager()
cluster = manager.get_cluster_from_json("config.json")
manager.launch_all("pip3 install h5py", list(cluster.public_ips.values()), "Failed to install h5py")
#cluster = LocalCluster()

class GramianExecutor:
    def __init__(self) -> None:
        self.state = None
        self.num_states = 1
    def initialize(datasets):
        pass
    def serialize(self):
        return {0: self.state}, "all"
    def deserialize(self, s):
        assert type(s) == list and len(s) == 1
        self.state = s[0][0]

    def execute(self,batches,stream_id, executor_id):
        batches = [batch for batch in batches if batch is not None]
        os.environ["OMP_NUM_THREADS"] = "8"

        print("start",time.time())
        for batch in batches:
            if self.state is None:
                self.state = np.transpose(batch).dot(batch)
            else:
                self.state += np.transpose(batch).dot(batch)
        print("end",time.time())

    def done(self,executor_id):
        print("done")
        return self.state 

reader = InputHDF5Dataset("quokka-examples","bigmatrix3.hdf5","data")
#reader = InputDiskHDF5Dataset("/data/bigmatrix3.hdf5","data")

task_graph = TaskGraph(cluster)

matrix = task_graph.new_input_reader_node(reader)

gramian = GramianExecutor()

output = task_graph.new_blocking_node({0:matrix}, gramian)

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)
print(output.to_pandas())
