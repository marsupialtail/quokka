class PlacementStrategy:
    def __init__(self) -> None:
        pass

'''
Launch a single channel. Useful for aggregator nodes etc.
'''
class SingleChannelStrategy(PlacementStrategy):
    def __init__(self) -> None:
        super().__init__()

'''
Launch a custom number of cahnnels per node. Note the default is cpu_count for SourceNodes and 1 for task nodes.
'''
class CustomChannelsStrategy(PlacementStrategy):
    def __init__(self, channels) -> None:
        super().__init__()
        self.channels_per_node = channels

'''
Launch only on GPU instances.
'''
class GPUStrategy(PlacementStrategy):
    def __init__(self) -> None:
        super().__init__()

