import polars
import uuid
import glob
import os

# class HBQ:
#     def __init__(self) -> None:
#         self.store = {}

#     def put(self, source_actor_id, source_channel_id, seq, target_actor_id, outputs):
#         self.store[source_actor_id, source_channel_id, seq, target_actor_id] = outputs

#     def get(self, source_actor_id, source_channel_id, seq, target_actor_id):
#         return self.store[source_actor_id, source_channel_id, seq, target_actor_id]

#     def delete(self, name):
#         del self.store[name]

#     def objects(self):
#         return list(self.store.keys())
    
#     def gc(self, gcable):
#         assert type(gcable) == list
#         for name in gcable:
#             assert name in self.store
#             self.delete(name)

class HBQ:
    def __init__(self, path = "/data/") -> None:
        self.store = {}
        self.path = path

        # there will be a race con
        try:
            files = glob.glob(self.path + '*')
            for f in files:
                os.remove(f)
        except:
            pass

    def put(self, source_actor_id, source_channel_id, seq, target_actor_id, outputs):
        assert type(outputs) == dict
        new_outputs = {}
        for key in outputs:
            assert type(outputs[key]) == polars.internals.DataFrame
            new_outputs[key] = self.path + "hbq-" + str(source_actor_id) + "-" + str(source_channel_id) + "-" + str(seq) \
                + "-" + str(target_actor_id) + "-" + str(key) + ".parquet"
            outputs[key].write_parquet( new_outputs[key] )

        self.store[source_actor_id, source_channel_id, seq, target_actor_id] = new_outputs

    def get(self, source_actor_id, source_channel_id, seq, target_actor_id):

        results = {}

        files = glob.glob(self.path + "hbq-" + str(source_actor_id) + "-" + str(source_channel_id) + "-" + str(seq) \
                + "-" + str(target_actor_id) + "-*")
        
        for file in files:
            channel = int(file.split(".")[0].split("-")[-1])
            results[channel] = polars.read_parquet(file)

        # for key in self.store[source_actor_id, source_channel_id, seq, target_actor_id]:
        #     results[key] = polars.read_parquet(self.store[source_actor_id, source_channel_id, seq, target_actor_id][key])

        return results

    def delete(self, name):
        del self.store[name]

    def objects(self):
        return list(self.store.keys())
    
    def gc(self, gcable):
        assert type(gcable) == list
        for name in gcable:
            assert name in self.store
            self.delete(name)