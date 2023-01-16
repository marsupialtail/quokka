import datetime

class Window:
    def __init__(self) -> None:
        pass
    
    @staticmethod
    def val_to_polars(val):
        if type(val) == int:
            return str(val) + "i"
        elif type(val) == datetime.timedelta:
            return str(int(val.total_seconds() * 1e6)) + "us"
        else:
            raise Exception("Unsupported value type, only int and datetime.timedelta are supported for now for window hops and sizes")

class HoppingWindow(Window):
    def __init__(self, hop, size) -> None:
        self.hop = hop
        self.size = size
        self.hop_polars = self.val_to_polars(hop)
        self.size_polars = self.val_to_polars(size)

class TumblingWindow(HoppingWindow):
    def __init__(self, size) -> None:
        super.__init__(size, size)

class SlidingWindow(Window):
    def __init__(self, size_before, size_after) -> None:
        self.size_before = size_before
        self.size_after = size_after
        self.size_before_polars = self.val_to_polars(size_before)
        self.size_after_polars = self.val_to_polars(size_after)

class SessionWindow(Window):
    def __init__(self, timeout) -> None:
        self.timeout = timeout
        self.timeout_polars = self.val_to_polars(timeout)