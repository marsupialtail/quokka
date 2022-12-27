
import ray
import polars
import pandas as pd
import pyarrow as pa

class Dataset:

    def __init__(self, wrapped_dataset) -> None:
        self.wrapped_dataset = wrapped_dataset
    
    def to_list(self):
        return ray.get(self.wrapped_dataset.to_list.remote())
    
    def to_df(self):
        return ray.get(self.wrapped_dataset.to_df.remote())
    
    def to_dict(self):
        return ray.get(self.wrapped_dataset.to_dict.remote())
    
    def to_arrow_refs(self):
        return ray.get(self.wrapped_dataset.to_arrow_refs.remote())


@ray.remote
class ArrowDataset:

    def __init__(self, num_channels) -> None:
        self.num_channels = num_channels
        self.objects = {i: [] for i in range(self.num_channels)}
        self.metadata = {}
        self.done = False

    def added_object(self, channel, object_handle):
        self.objects[channel].append(object_handle[0])
    
    def get_objects(self):
        assert self.is_complete()
        return self.objects

    def to_arrow_refs(self):
        results = []
        for channel in self.objects:
            results.extend(self.objects[channel])
        return results

    def to_df(self):
        dfs = []
        for channel in self.objects:
            for object in self.objects[channel]:
                dfs.append(ray.get(object))
        arrow_table = pa.concat_tables(dfs)
        return polars.from_arrow(arrow_table)
    
    def ping(self):
        return True