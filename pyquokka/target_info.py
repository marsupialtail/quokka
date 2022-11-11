import sqlglot
import sqlglot.optimizer as optimizer

class TargetInfo:
    def __init__(self, partitioner, predicate: sqlglot.Expression, projection, batch_funcs: list) -> None:
        
        # unlowered types:
        # Partitioner, sqlglot.Expression, set, list of functions

        # lowered types:
        # FunctionPartitioner, function, list, list of functions
        
        self.partitioner = partitioner
        self.predicate = predicate if predicate is not None else sqlglot.exp.TRUE
        self.projection = projection
        self.batch_funcs = batch_funcs
        self.lowered = False

    def and_predicate(self, predicate) -> None:
        self.predicate = optimizer.simplify.simplify(
            sqlglot.exp.and_(self.predicate, predicate))

    def predicate_required_columns(self) -> set:
        return set(i.name for i in self.predicate.find_all(sqlglot.expressions.Column))

    def append_batch_func(self, f) -> None:
        self.batch_funcs.append(f)

    def __str__(self) -> str:
        return "partitioner: " + str(self.partitioner) + "\n\t  predicate: " + self.predicate.sql() + "\n\t  projection: " + str(self.projection) + "\n\t batch_funcs: " + str(self.batch_funcs)


class Partitioner:
    def __init__(self) -> None:
        pass

class PassThroughPartitioner(Partitioner):
    def __init__(self) -> None:
        super().__init__()
    def __str__(self):
        return 'pass_thru'

class BroadcastPartitioner(Partitioner):
    def __init__(self) -> None:
        super().__init__()
    def __str__(self):
        return 'broadcast'

class HashPartitioner(Partitioner):
    def __init__(self, key) -> None:
        super().__init__()
        self.key = key
    def __str__(self):
        return self.key

class RangePartitioner(Partitioner):
    # total_range needs to be filled by the cardinality estimator
    def __init__(self, key, total_range) -> None:
        super().__init__()
        assert type(total_range) == int
        self.key = key
        self.total_range = total_range
    
    def __str__(self):
        return "range partitioner on " + str(self.key) + ", range estimate " + str(self.total_range)

class FunctionPartitioner(Partitioner):
    def __init__(self, func) -> None:
        super().__init__()
        self.func = func
    def __str__(self):
        return "custom partitioner"