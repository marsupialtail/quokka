
import ray
import polars
import pandas as pd
import pyarrow as pa

class Dataset:

    def __init__(self, schema, wrapped_dataset, dataset_id) -> None:
        self.schema = schema
        self.wrapped_dataset = wrapped_dataset
        self.dataset_id = dataset_id
        
    def __str__(self):
        return "DataSet[" + ",".join(self.schema) + "]"

    def __repr__(self):
        return "DataSet[" + ",".join(self.schema) + "]"
    
    def __copy__(self):
        return Dataset(self.schema, self.wrapped_dataset, self.dataset_id)
    
    def __deepcopy__(self, memo):
        return Dataset(self.schema, self.wrapped_dataset, self.dataset_id)

    def to_df(self):
        return ray.get(self.wrapped_dataset.to_df.remote(self.dataset_id))
    
    def to_dict(self):
        return  ray.get(self.wrapped_dataset.to_dict.remote(self.dataset_id))
    
    def to_arrow_refs(self):
        return ray.get(self.wrapped_dataset.to_arrow_refs.remote(self.dataset_id))
    
    def to_ray_dataset(self):
        return ray.data.from_arrow_refs(self.to_arrow_refs())

    def length(self):
        return ray.get(self.wrapped_dataset.length.remote(self.dataset_id))

# we need to figure out how to clean up dead objects on dead nodes.
# not a big problem right now since Arrow Datasets are not fault tolerant anyway

@ray.remote
class ArrowDataset:

    def __init__(self) -> None:
        self.latest_dataset = 0
        self.objects = {}
        self.done = {}
        self.length = {}
    
    def length(self, dataset):
        return self.length[dataset]

    def to_dict(self, dataset):
        return {ip: [ray.cloudpickle.dumps(object) for object in self.objects[dataset][ip]] for ip in self.objects[dataset]}

    def create_dataset(self):
        self.latest_dataset += 1
        self.done[self.latest_dataset] = False
        self.length[self.latest_dataset] = 0
        self.objects[self.latest_dataset] = {}
        return self.latest_dataset

    def delete_dataset(self):
        del self.done[self.latest_dataset]
        del self.length[self.latest_dataset]
        del self.objects[self.latest_dataset]

    def added_object(self, dataset, ip, object_handle):
        assert dataset in self.objects, "must create the dataset first!"
        if ip not in self.objects[dataset]:
            self.objects[dataset][ip] = []
        self.objects[dataset][ip].append(object_handle[0])
        self.length[dataset] += object_handle[1]

    def to_arrow_refs(self, dataset):
        results = []
        for ip in self.objects[dataset]:
            results.extend(self.objects[dataset][ip])
        return results

    def to_df(self, dataset):
        dfs = []
        for ip in self.objects[dataset]:
            for object in self.objects[dataset][ip]:
                dfs.append(ray.get(object))
        if len(dfs) > 0:
            arrow_table = pa.concat_tables(dfs)
            return polars.from_arrow(arrow_table)
        else:
            return None
    
    def ping(self):
        return True