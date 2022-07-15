from pyquokka.dataset import * 
from pyquokka.quokka_runtime import TaskGraph

from utils import * 


class CardinalityEstimate:
    def __init__(self, total_bytes, sample) -> None:
        # total number of bytes of input
        self.total_bytes = total_bytes
        # pyarrow table
        self.sample = sample

class SQLContext:

    def __init__(self, cluster) -> None:
        self.catalog = {} # dict of table name to tuple( table_location, table_type, schema, which is a dict of column name to data type)
        self.task_graph = TaskGraph(cluster)

    # to register a local table, use "absolute path"
    # to register a table on S3, use "s3://bucket/key" or "s3://bucket/prefix*"
    def register_csv(self, table_name, table_location, schema = None, sep = ",", header = False, stride = 128 * 1024 * 1024):
        if table_location[:5] == "s3://":
            self.table_location = self.table_location[5:]
            bucket = self.table_location.split("/")[0]
            if "*" in self.table_location:
                assert self.table_location[-1] == "*" , "wildcard can only be the last character in address string"
                prefix = "/".join(self.table_location[:-1].split("/")[1:])
                table_location = {"bucket":bucket, "prefix":prefix}
                table_type = "s3-multi-csv"
            else:
                key = "/".join(self.table_location.split("/")[1:])
                table_location = {"bucket": bucket, "key": key}
                table_type = "s3-single-csv"
        else:
            if "*" in self.table_location:
                raise NotImplementedError
            else:
                table_location = {"filepath": table_location}
                table_type = "disk-single-csv"

        self.catalog[table_name] = {"table_location" : table_location , "table_type" : table_type, "schema" : schema, "sep": sep, "header":header, "stride":stride}
    
    def register_parquet(self, table_name, table_location): 
        if table_location[:5] == "s3://":
            self.table_location = self.table_location[5:]
            bucket = self.table_location.split("/")[0]
            if "*" in self.table_location:
                assert self.table_location[-1] == "*" , "wildcard can only be the last character in address string"
                prefix = "/".join(self.table_location[:-1].split("/")[1:])
                table_location = {"bucket":bucket, "prefix":prefix}
                table_type = "s3-multi-parquet"
            else:
                raise NotImplementedError
                key = "/".join(self.table_location.split("/")[1:])
                table_location = {"bucket": bucket, "key": key}
                table_type = "s3-single-parquet"
        else:
            raise NotImplementedError

        self.catalog[table_name] = {"table_location" : table_location , "table_type" : table_type}

class TaskNode:
    def __init__(self, sql_context, parents) -> None:
        self.sql_context = sql_context
        self.pre_function = None
        self.post_function = None

        self.parents = parents 
        self.targets = [] # targets are symmetric. Since they all receive the same stuff from you

        self.cardinality = None # estimate of your own cardinality
    
    def add_target(self, node):
        self.targets.append(node)

    def update_cardinality(self):
        # assumes that all of your parents have updated their own cardinality estimates
        pass

    def __str__(self):
        pass

    def walk(self):
        print(self)
        for parent in self.parents:
            parent.walk()

class ScanNode:
    # overriden child classes will have their own initializers that include how to actually do the scan, disk/s3, parquet/csv, etc.
    def __init__(self, sql_context, table_name, projection, condition, sample_factor = 8) -> None:
        self.sql_context = sql_context
        
        self.projection = projection
        self.condition = condition

        self.targets = [] # targets are symmetric. Since they all receive the same stuff from you

        assert table_name in self.sql_context.catalog, "table name not in catalog, must register table with location and schema first"
        
        args = self.sql_context.catalog[table_name]
        table_location = args["table_location"]
        table_type = args["table_type"]

        # go figure out the cardinality right away because this literally decides what you are
        print("this will fail. you need to get rid of the names of the results")
        projection_required_cols = required_columns_from_exp(projection)

        if table_type == "s3-single-csv":
            self.dataset = InputS3CSVDataset(table_location["bucket"], names = args["schema"], key = table_location["key"], sep= args["sep"], stride = args["stride"], header = args["header"])
            condition_func, condition_required_cols = csv_condition_decomp(condition)
        elif table_type == "s3-multi-csv":
            self.dataset = InputS3CSVDataset(table_location["bucket"], names = args["schema"], prefix = table_location["prefix"], sep= args["sep"], stride = args["stride"], header = args["header"])
        elif table_type == "disk-single-csv":
            self.dataset = InputDiskCSVDataset(table_location["filepath"] , names =args["schema"] , sep= args["sep"], stride=args["stride"], header = args["header"])
        elif table_type == "s3-multi-parquet":
            filters, condition_func, condition_required_cols = parquet_condition_decomp(condition)
            self.dataset = InputMultiParquetDataset(table_location["bucket"], table_location["prefix"], columns = projection_required_cols.union(condition_required_cols), filters= filters)
            self.dataset.get_own_state(1)
            self.dataset.set_num_channels(1)
            _, sample = next(self.dataset.get_next_batch(0))
            filtered_sample = condition_func(sample)
            length = self.dataset.length
            
        self.dataset.get_own_state(sample_factor)

    def add_target(self, node):
        self.targets.append(node)
    
    
            


class ResultNode:
    def __init__(self,parents) -> None:
        assert len(parents) == 1
        self.parents = parents

        self.cardinality = None
    
    def update_cardinality(self):
        # assume parents have updated cardinality
        return self.parents[0].cardinality


class MultiJoinNode(TaskNode):
    def __init__(self, parents, join_info) -> None:
        super().__init__(parents)

        # up to you to decide how you want to represent the join graph. I recommend a 2D array K of size N x N, where N is the number of parents
        # basically an adjacency matrix representation of the join graph. K[i,j] is the join type between parent i and parent j.
        self.join_graph = self.compute_join_graph(join_info)
    
    def compute_join_graph(self, join_info):
        pass

    # this is part of the physical plan. You do this after you have updated the cardinality
    def determine_join_strategy(self):
        pass

class GroupedAggNode(TaskNode):
    # if keys is None this means global aggregation without grouping
    def __init__(self, parents, keys = None) -> None:
        assert len(parents) == 1
        super().__init__(parents)
        self.keys = keys

class SortNode(TaskNode):
    # keys is a dict of key -> order (incr,decr)
    def __init__(self, parents, keys,limit = None) -> None:
        assert len(parents) == 1
        super().__init__(parents)
        self.keys = keys
        self.limit = limit


# TPCH-3

customer = ScanNode()
orders = ScanNode()
lineitem = ScanNode()
joined = MultiJoinNode([customer, orders, lineitem], None)
grouped = GroupbyNode([joined], keys = ["l_orderkey","o_orderdate","o_shippriority"])
sorted = SortNode([grouped],keys = {"revenue":"desc","o_orderdate":"incr"}, limit=10)
result = ResultNode([sorted])