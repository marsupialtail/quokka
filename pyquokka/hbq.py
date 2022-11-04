import polars
class HBQ:
    def __init__(self) -> None:
        self.store = {}

    def put(self, name, data):
        assert len(name) == 3
        self.store[name] = data

    def get(self, name):
        return self.store[name]

    def delete(self, name):
        del self.store[name]

    def objects(self):
        return list(self.store.keys())
    
    def gc(self, gcable):
        assert type(gcable) == list
        for name in gcable:
            assert name in self.store
            self.delete(name)