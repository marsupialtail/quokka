class TaskNode:
    def __init__(self, parents) -> None:
        self.pre_filter = None # you can decide what data structure these should be. sqlglot expression trees should be ok.
        self.post_filter = None
        self.pre_projection = None # sqlglot expression trees should be ok. or rather a list of expression trees.
        self.post_projection = None

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
    def __init__(self) -> None:
        self.post_filter = None
        self.post_projection = None

        self.targets = [] # targets are symmetric. Since they all receive the same stuff from you

        self.cardinality = None

    def add_target(self, node):
        self.targets.append(node)
    
    def update_cardinality(self):
        # implemented by the child class
        pass 

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

class GroupbyNode(TaskNode):
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