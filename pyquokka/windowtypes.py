import datetime
from pyquokka.sql_utils import evaluate
import polars
import sqlglot

class Window:
    def __init__(self) -> None:

        self.aggregation_dict = None
        pass

    def add_aggregation_dict(self, aggregations: dict) -> None:
        """
        aggregations: dict of aggregation name and aggregation function
        For example, key-value pairs can be: 
           "avg_price": "AVG(price)"
           "max_discounted_price": "MAX(price * (1-discount))"
           "total_count": "count(*)"
        """
        
        self.aggregation_dict = aggregations
    
    def polars_aggregations(self):
        assert self.aggregation_dict is not None, "aggregation_dict is not set"
        agg_list = []
        for key, value in self.aggregation_dict.items():
            agg_list.append(evaluate(sqlglot.parse_one(value)).alias(key))
        return agg_list

    def sql_aggregations(self):
        assert self.aggregation_dict is not None, "aggregation_dict is not set"
        # this should be easy lol
        sql_agg_str = ""
        for key, value in self.aggregation_dict.items():
            sql_agg_str += f"{value} OVER win AS {key}, "
        return sql_agg_str[:-2]
    
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
    def __init__(self, size_before) -> None:
        # we are not going to support size_after for now
        self.size_before = size_before
        self.size_before_polars = self.val_to_polars(size_before)

class SessionWindow(Window):
    def __init__(self, timeout) -> None:
        self.timeout = timeout
        self.timeout_polars = self.val_to_polars(timeout)

class Trigger:
    def __init__(self) -> None:
        pass

class OnEventTrigger(Trigger):
    def __init__(self) -> None:
        super().__init__()

class OnCompletionTrigger(Trigger):
    # triggers on completion of the window plus delay.
    # for session windows, this only makes sense if the delay is greater than the timeout?
    def __init__(self, delay = None) -> None:
        super().__init__()
        self.delay = delay

class WindowAggregations:
    # the aggregation_dict will look something like this
    # {"new_name": "sum()"}
    def __init__(self, aggregation_dict) -> None:
        pass