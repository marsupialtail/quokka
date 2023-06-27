from pyquokka.dataset.ordered_readers import *
from pyquokka.dataset.unordered_readers  import *
from pyquokka.dataset.crypto_dataset import *

class InputPolarsDataset:

    def __init__(self, df):
        self.df = df

    def get_own_state(self, num_channels):
        assert num_channels == 1
        return {0: [0]}

    def execute(self, mapper_id, state=None):
        return None, self.df

# this is the only non fault tolerant operator here. This is because the Ray object store is not fault tolerant and not replayable.
class InputRayDataset:

    # objects_dict is a dictionary of IP address to a list of Ray object references
    def __init__(self, objects_dict):
        self.objects_dict = objects_dict

    def execute(self, mapper_id, state=None):
        # state will be a tuple of IP, index
        try:
            # print("Getting object from {} at index {}.".format(state[0], state[1]), self.objects_dict[state[0]][state[1]])
            result = ray.get(ray.cloudpickle.loads(self.objects_dict[state[0]][state[1]]), timeout=10)
            # print(result)
        except:
            raise Exception("Unable to get object from {} at index {}.".format(state[0], state[1]))
        
        return None, result
