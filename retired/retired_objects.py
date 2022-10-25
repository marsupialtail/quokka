@ray.remote
class WrappedDataset:

    def __init__(self, num_channels) -> None:
        self.num_channels = num_channels
        self.objects = {i: [] for i in range(self.num_channels)}
        self.metadata = {}
        self.remaining_channels = {i for i in range(self.num_channels)}
        self.done = False

    def added_object(self, channel, object_handle):

        if channel not in self.objects or channel not in self.remaining_channels:
            raise Exception
        self.objects[channel].append(object_handle)
    
    def add_metadata(self, channel, object_handle):
        if channel in self.metadata or channel not in self.remaining_channels:
            raise Exception("Cannot add metadata for the same channel twice")
        self.metadata[channel] = object_handle
    
    def done_channel(self, channel):
        self.remaining_channels.remove(channel)
        if len(self.remaining_channels) == 0:
            self.done = True

    def is_complete(self):
        return self.done

    # debugging method
    def print_all(self):
        for channel in self.objects:
            for object in self.objects[channel]:
                r = redis.Redis(host=object[0], port=6800, db=0)
                print(pickle.loads(r.get(object[1])))
    
    def get_objects(self):
        assert self.is_complete()
        return self.objects

    def to_df(self):
        assert self.is_complete()
        dfs = []
        for channel in self.objects:
            for object in self.objects[channel]:
                r = redis.Redis(host=object[0], port=6800, db=0)
                dfs.append(pickle.loads(r.get(object[1])))
        try:
            return polars.concat(dfs)
        except:
            return pd.concat(dfs)
    
    def to_list(self):
        assert self.is_complete()
        ret = []
        for channel in self.objects:
            for object in self.objects[channel]:
                r = redis.Redis(host=object[0], port=6800, db=0)
                ret.append(pickle.loads(r.get(object[1])))
        return ret

    def to_dict(self):
        assert self.is_complete()
        d = {channel:[] for channel in self.objects}
        for channel in self.objects:
            for object in self.objects[channel]:
                r = redis.Redis(host=object[0], port=6800, db=0)
                thing = pickle.loads(r.get(object[1]))
                if type(thing) == list:
                    d[channel].extend(thing)
                else:
                    d[channel].append(thing)
        return d