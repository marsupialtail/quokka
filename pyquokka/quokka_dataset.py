
import ray
import polars
import pandas as pd
import pyarrow as pa

class Dataset:

    def __init__(self, schema, wrapped_dataset) -> None:
        self.schema = schema
        self.wrapped_dataset = wrapped_dataset
        
    def __str__(self):
        return "DataSet[" + ",".join(self.schema) + "]"

    def __repr__(self):
        return "DataSet[" + ",".join(self.schema) + "]"
    
    def __copy__(self):
        return Dataset(self.schema, self.wrapped_dataset)
    
    def __deepcopy__(self, memo):
        return Dataset(self.schema, self.wrapped_dataset)

    def to_list(self):
        return ray.get(self.wrapped_dataset.to_list.remote())
    
    def to_df(self):
        return ray.get(self.wrapped_dataset.to_df.remote())
    
    def to_dict(self):
        return  ray.get(self.wrapped_dataset.to_dict.remote())
    
    def to_arrow_refs(self):
        return ray.get(self.wrapped_dataset.to_arrow_refs.remote())

    def length(self):
        return ray.get(self.wrapped_dataset.length.remote())

# we need to figure out how to clean up dead objects on dead nodes.
# not a big problem right now since Arrow Datasets are not fault tolerant anyway

@ray.remote
class ArrowDataset:

    def __init__(self) -> None:
        self.objects = {}
        self.metadata = {}
        self.done = False
        self.length = 0
    
    def length(self):
        return self.length

    def to_dict(self):
        return {ip: [ray.cloudpickle.dumps(object) for object in self.objects[ip]] for ip in self.objects}

    def added_object(self, ip, object_handle):
        if ip not in self.objects:
            self.objects[ip] = []
        self.objects[ip].append(object_handle[0])
        self.length += object_handle[1]
    
    def get_objects(self):
        assert self.is_complete()
        return self.objects

    def to_arrow_refs(self):
        results = []
        for ip in self.objects:
            results.extend(self.objects[ip])
        return results

    def to_df(self):
        dfs = []
        for ip in self.objects:
            for object in self.objects[ip]:
                dfs.append(ray.get(object))
        if len(dfs) > 0:
            arrow_table = pa.concat_tables(dfs)
            return polars.from_arrow(arrow_table)
        else:
            return None
    
    def ping(self):
        return True