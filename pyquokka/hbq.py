import polars
class HBQ:
    def __init__(self) -> None:
        self.store = {}

    def put(self, source_actor_id, source_channel_id, seq, target_actor_id, outputs):
        self.store[source_actor_id, source_channel_id, seq, target_actor_id] = outputs

    def get(self, source_actor_id, source_channel_id, seq, target_actor_id):
        return self.store[source_actor_id, source_channel_id, seq, target_actor_id]

    def delete(self, name):
        del self.store[name]

    def objects(self):
        return list(self.store.keys())
    
    def gc(self, gcable):
        assert type(gcable) == list
        for name in gcable:
            assert name in self.store
            self.delete(name)