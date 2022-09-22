import pyarrow
import pyarrow.compute as compute
from copy import copy
"""

We are going to build a SQL query optimizer in Python. Yay.

In this system, there will be four kinds of "foundational nodes": input node, unary node, binary node and shuffle node. They will be parent classes for more specific nodes.

Each node has an optional post-filter and a stateless batch function

InputNode:
- InputReaderNode: read from S3/disk dataset
- InputCachedNode: read from memory dataset

UnaryNode:
- FilterNode: implements predicates
- SelectNode: implements projection
- StatelessUnaryNode: implements stateless transformation, e.g. map, first phase of aggregate, udf
- StatefulUnaryNode: implements stateful transformation, e.g. aggregate, deep learning inference, broadcast join, distinct, count, etc.

BinaryNode: (in the future this can be easily extended to multi-nary nodes)
- JoinNode: implements a join, with different varieties
- StatelessBinaryNode, e.g. concat
- StatefulBinaryNode: custom binary node, e.g. asof join

ShuffleNode:
Nodes have default communication strategy which is pass through. Say four nodes in A and two nodes in B, and B follows A.
Then first two nodes in A will pass batches to first node in B and third/fourth node in A pass batches to second node in B.
This changes the communication strategy with a partition function.
E.g. hash partition for joins or groupby, range partitions for sort, etc.

I guess there are two other kinds of nodes, compute node and collect node.
Compute node specifies that the result of the node directly preceding compute must be materialized in memory, possibly in a distributed fashion.
Collect node specifies that the result must be collected on the driver. 
These can be explicitly constructed from the API with .compute or .collect, or forced through blocking operations like write, aggregation and sort.

When the tree optimization starts, we will go through the following steps:
1) Filter pushdown. Remove all filter nodes except the ones absolutely necessary. Filters can be merged with each other and pushed through all nodes all the way to the input.
Each node can specify "stop columns" that stop filter pushing. For example if pushing the filter past a stateful node might change its semantics. For nonblocking stateful nodes
this is typically not the case. For blocking stateful nodes such as count or aggregation this is, but these nodes are the last in the graph anyways and will not experience filter pushing.
Filter pushing updates the list of "required columns" at each node that are required for evaluation of the node. This interacts with early projection.
In the end, the only filter nodes that should exist represent different views of the same input source. Those cannot go away. Filter pushdown through DFS to the root.

2) Early projection. Remove all select nodes except the ones absolutely necessary. We update what columns each node will emit. The invariant is that the columns not emitted by a node will never be 
used again in the future. There will be select nodes present after this, namely ones that corresponds to different views of a relation. "tempscan" in hyper parlance.

3) Stateless Fusion. Fuse all the stateless map functions. In the end there can still be StatelessUnaryNodes, similar to the two scenarios above, where you have different views of the same relation.
You might wonder why we are not going to fuse stateful nodes like deep learning inference and broadcast join. We certainly can, but it's probably not worth it since those tend to be more compute intensive,
and might benefit from the async interface between different nodes.

After these three steps, most of the nodes in the system should be StatefulUnaryNode (i.e. expensive nodes that shouldn't be fused) and BinaryNodes. Other UnaryNodes could exist in "uncle" locations,
where there is a fork in the expression tree. We will now proceed with the next step, which is cardinality estimation.

4) Join Fusion. We are not going to replace trees of join nodes with join nodes with multiple operands. This will greatly simplify the tree and expose different choice of joins. We will do predicate 
pull up from the individual join nodes and merge neded columns.

5) Cardinality Estimation. Input nodes now should have all the relevant early projection and pushed down filter, and can estimate how much data will be produced based on this. 
We are going to estimate the number of unique values for each column as well as the total expected size of the output from this node. This estimate will be pushed up the tree as far as possible.
If the estimated size is smaller than some threshold, the entire input will be materialized as a constant variable in the query plan. Some say you should do this at first, because it's 
unlikely you'll decide to materialize something after the filtering but not before. I say that's false -- you can have datasets on the range of 1GB that you estimate will only have 10MB 
left after the filters. Starting from a materialized input node we can then materialize all of the nodes that depend on this input, ending in a binary node with an unmaterialized operand. 
In this case, if the operand has a unary version, e.g. join -> broadcast join, we will rewrite the graph then and there. If not, we will convert it to a cached dataset in memory and use InputCachedNode

6) Join order selection. The join node will now figure out the order in which to do the joins. If some of its inputs are materialized it will join them in the main thread first. We are going to 
restrict ourselves to left deep plans and enumerate all n! possible plans. We will use all the nice independence assumptions to compute the join cost of each ordering. If some of the tables have already
been materialized, they will be joined first to produce bigger/smaller materialized tables. This could potentially lead to a size blowup, which we don't address.

"""
class Column:
    def __init__(self, frame, name) -> None:
        self.frame = frame
        self.name = name

    def __hash__(self):
        return hash((self.frame,self.name))
    
    def __lt__(self, other):
        return FilterCondition((self, "<", other))
    def __le__(self, other):
        return FilterCondition((self, "<=", other))
    def __gt__(self, other):
        return FilterCondition((self, ">", other))
    def __ge__(self, other):
        return FilterCondition((self, ">=", other))
    def __eq__(self, other):
        return FilterCondition((self, "==", other))
    def __ne__(self, other): 
        return FilterCondition((self, "!=", other))
    def isin(self, key):
        assert type(key) == set
        return FilterCondition((self, "isin", key))
    


class FilterCondition:
    # add better parsing strategies in the future, possibly by overloading __gt__ etc. in Column class
    def __init__(self, tup, negated= False) -> None:
        self.lhs = tup[0]
        self.cond = tup[1]
        self.rhs = tup[2]
        self.columns = set()
        if type(self.lhs) == Column:
            self.columns.add(self.lhs)
        if type(self.rhs) == Column:
            self.columns.add(self.rhs)
        if (not type(self.lhs) == Column) and (not type(self.rhs) == Column):
            raise Exception("One side of the filter condition must be a column!")
        self.negated = negated
    def __str__(self):
        return ("!" if self.negated else "") + self.lhs.name + " " + self.cond + " " + self.rhs.name
    def all_cols(self):
        return self.columns
    
    def __and__(self, other):
        z = Predicate()
        z._and(self)
        z._and(other)
        return z
    
    def __or__(self, other):
        z = Predicate()
        z._and(self)
        z._or(other)
        return z
    
    def apply_mapping(self, mapping):
        if type(self.lhs) == Column and  self.lhs.name in mapping:
            self.lhs.name = mapping[self.lhs.name]
        if type(self.rhs) == Column and self.rhs.name in mapping:
            self.rhs.name = mapping[self.rhs.name]

class Predicate: # a boolean tree of filter conditions
    def __init__(self) -> None:
        self.state = True
    
    def _and(self, operand):
        assert type(operand) == FilterCondition or type(operand) == Predicate, operand
        
        if type(operand) == FilterCondition:
            self.state = [self.state, "and", operand]
        elif type(operand) == Predicate:
            self.state = [self.state, "and", operand.state]
        else:
            raise Exception
    def _or(self, operand):
        assert type(operand) == FilterCondition or type(operand) == Predicate
        
        if type(operand) == FilterCondition:
            self.state = [self.state, "or", operand]
        elif type(operand) == Predicate:
            self.state = [self.state, "or", operand.state]
        else:
            raise Exception

    # mapping is a dict from current column names to new column names
    def apply_mapping(self, mapping): 
        def do(item):
            if type(item) == FilterCondition:
                item.apply_mapping(mapping)
            elif type(item) == list:
                [do(i) for i in item]
        do(self.state)

    def specialize(self, cols): # set all the predicates related to columns in cols to True
        # we are going to give up when we see a disjunction. Therefore the only predicates that will be pushed down are ones that MUST be true for the 
        # parent predicate to be true. This neglects subtrees that can be pushed down as a unit which might contain disjunctions, which is probably quite 
        # common. 
        
        pushed_down = Predicate()
        assert type(cols) == set, "cols must be a set"

        def do(item, cols):
            if type(item) == FilterCondition:
                if {k.name for k in item.columns}.issubset(cols):
                    pushed_down._and(copy(item))
                    return True
                else:
                    return item
            elif type(item) == list:
                if item[1] == "and":
                    return [do(i, cols) for i in item]
                elif item[1] == "or":
                    return item # stop the recursion here, nothing below this will be pushed down
                else:
                    raise Exception
            else:
                return item
        self.state = do(self.state,cols)
        self.simplify()
        #print(self.state)

        return pushed_down

    def __str__(self) -> str:
        if self.state is False:
            return "False"
        elif self.state is True:
            return "True"
        elif type(self.state) == FilterCondition:
            return self.state.__str__()
        else:
            return self.state[0].__str__() + " " + self.state[1] + " " + self.state[2].__str__()

    def simplify(self):
        def do(item):
            if type(item) == list:
                if item[1] == "or":
                    if item[0] == True or item[2] == True:
                        return True
                    if item[0] == False and item[2] == False:
                        return False
                    if item[0] == False:
                        return do(item[2])
                    if item[2] == False:
                        return do(item[0])
                    # some galaxy brain shit here, figure out how to do it properly later
                    if do(item[0]) == item[0] and do(item[2]) == item[2]:
                        return item
                    return do([do(i) for i in item])
                elif item[1] == "and" :
                    if item[0] == False or item[2] == False:
                        return False
                    if item[0] == True and item[2] == True:
                        return True
                    if item[0] == True:
                        return do(item[2])
                    if item[2] == True:
                        return do(item[0])
                    if do(item[0]) == item[0] and do(item[2]) == item[2]:
                        return item
                    return do([do(i) for i in item])
                else:
                    raise Exception
            else:
                return item
        #print("PRE SIMP", self.state)
        self.state = do(self.state)
        #print("POST SIMP", self.state)

    def __copy__(self):
        def do(item):
            if type(item) == list:
                return [do(i) for i in item]
            else:
                return copy(item)
        new_state = do(self.state)
        p = Predicate()
        p.state = new_state
        return p
        
    def all_cols(self):
        cols = set()
        def do(item):
            if type(item) == FilterCondition:
                [cols.add(col) for col in item.columns]
            elif type(item) == list:
                [do(i) for i in item]
        do(self.state)
        return cols
        

class SQLContext:
    def __init__(self) -> None:
        pass
    def __push_filter__(self, node):
        node.filters.simplify()
        if issubclass(type(node), UnaryNode):
            node_parent = node.parents[0]
            assert len(node_parent.targets) > 0
            # if the parent only has one target, which is you, you will delete yourself and and your filter predicate to the parents.
            if len(node_parent.targets) == 1:
                node_parent.filters._and(node.filters)
                if issubclass(type(node), FilterNode):
                    node_parent.targets = node.targets
                    for target in node.targets:
                        target.parents.remove(node)
                        target.parents.append(node_parent)
                    del node
                    self.__push_filter__(node_parent)
                else:
                    node.filters = Predicate()
                    self.__push_filter__(node_parent)
            else:
                node_parent.filters._or(node.filters)
                self.__push_filter__(node_parent)
        
        elif issubclass(type(node), BinaryNode):
            # you need to push the right part of the Predicate to each parent depending on the schema. the way you are going to do that
            # is by converting all the other filter conditions to True and relying on simplify at the end. 
            for i in range(2):
                parent = node.parents[i]
                # some of the columns might be renamed. System guarantees that in each dataframe, all column names are unique.
                # however col X in parent might be X_suffix in this node.
                schema_mapping = node.schema_mapping[i]
                parent_filters = node.filters.specialize(set(schema_mapping.keys()))
                parent_filters.apply_mapping(schema_mapping)
                if len(parent.targets) == 1:
                    parent.filters._and(parent_filters)
                    self.__push_filter__(parent)
                else:
                    parent.filters._or(parent_filters)
                    self.__push_filter__(parent)

        elif issubclass(type(node), InputNode):
            return

sql = SQLContext()

class Node:
    def __init__(self, schema) -> None:
        self.filters = Predicate()
        self.required_columns = set() # the columns that are required to execute this node, including the filters!!
        self.schema = schema # dict of column name to data type, this is the output schema.
        self.targets = []
    
    def __getattr__(self, name):
        if name in self.schema:
            return Column(self, name)
        else:
            raise Exception("Can't find column ", name)

    def add_target(self, node):
        self.targets.append(node)

    def filter(self, cond):
        
        assert type(cond) == FilterCondition or type(cond) == Predicate
        
        for col in cond.all_cols():
            assert col.frame == self, "must filter based on your own columns"

        node = FilterNode(self.schema, cond)
        node.set_parents(self)
        self.targets.append(node)
        return node
    
    def compute(self):
        node = ComputeNode(self.schema)
        node.set_parents(self)
        self.targets.append(node)
        return node
    
    def select(self, columns):
        for column in columns:
            assert column in self.schema, "column " + column + " not in dataframe"

    def __str__(self):
        return str(type(self)) + "\nFilters:" + str(self.filters) 

    def walk(self):
        print(self)
        for parent in self.parents:
            parent.walk()

class InputNode(Node):
    def __init__(self, schema) -> None:
        super().__init__(schema)
    
    def walk(self):
        print(self)

class InputCSVNode(InputNode):
    def __init__(self, filename, schema, sep) -> None:
        super().__init__(schema)
        self.filename = filename
        self.sep = sep

class UnaryNode(Node):
    def __init__(self, schema) -> None:
        super().__init__(schema)

    def set_parents(self, node):
        self.parents = [node]

    

class FilterNode(UnaryNode):
    def __init__(self, schema, cond):
        super().__init__(schema)
        for col in cond.all_cols():
            self.required_columns.add(col.name)
        self.filters._and(cond)
        
    
    def __str__(self):
        return "FilterNode: " + self.filters.__str__()

class ComputeNode(UnaryNode):
    def __init__(self, schema):
        super().__init__(schema)

class SelectNode(UnaryNode):
    def __init__(self, schema, columns):
        try:
            new_schema = {col: schema[col] for col in columns}
        except:
            raise Exception("a column was not found")
        super().__init__(new_schema)

class BinaryNode(Node):
    def __init__(self, schema, schema_mapping) -> None:
        super().__init__(schema)
        # if suffixes are used to arrive at new names, we need to keep track.
        self.schema_mapping = schema_mapping
    
    def set_parents(self, parent1, parent2):
        self.parents = (parent1, parent2)
    

class JoinNode(BinaryNode):
    def __init__(self, schema, schema_mapping, left_on, right_on) -> None:
        super().__init__(schema, schema_mapping)
        self.left_on = left_on
        self.right_on = right_on

def read_csv(filename, schema, sep=","):
    node = InputCSVNode(filename, schema, sep)
    return node

def join(node1, node2, on = None, left_on = None, right_on = None, suffix = "_2"):
    if on is None:
        assert left_on is not None and right_on is not None
        assert left_on in node1.schema, "join key not found in first table"
        assert right_on in node2.schema, "join key not found in second table"
    else:
        assert on in node1.schema, "join key not found in first table"
        assert on in node2.schema, "join key not found in second table"
        left_on = on
        right_on = on
    
    assert node1.schema[left_on] == node2.schema[right_on], "join column has different schema in tables"
    new_schema = node1.schema.copy()
    schema_mapping = [{col:(col) for col in node1.schema}]
    schema_mapping1 = {}
    for col in node2.schema:
        if col == right_on:
            continue
        if col in new_schema:
            assert col + suffix not in new_schema, "the suffix was not enough to guarantee unique col names"
            new_schema[col + suffix] = node2.schema[col]
            schema_mapping1[col + suffix] = col
        else:
            new_schema[col] = node2.schema[col]
            schema_mapping1[col] = col
        
    schema_mapping.append(schema_mapping1)
    # the order in the schema mapping must correspond to the order of nodes in set_parents!!!

    node = JoinNode(new_schema, schema_mapping, left_on, right_on)
    node1.add_target(node)
    node2.add_target(node)
    node.set_parents(node1, node2)
    return node

a = read_csv("test", {"l_orderkey": pyarrow.uint64(), 
    "l_partkey": pyarrow.uint64(),
    "l_suppkey": pyarrow.uint64(),
    "l_linenumber": pyarrow.uint64(),
    "l_quantity": pyarrow.float64(),
    "l_extendedprice": pyarrow.float64(),
    "l_discount": pyarrow.float64(),
    "l_tax": pyarrow.float64(),
    "l_returnflag": pyarrow.string(),
    "l_linestatus": pyarrow.string(),
    "l_shipdate": pyarrow.date32(),
    "l_commitdate":pyarrow.date32(),
    "l_receiptdate":pyarrow.date32(),
    "l_shipinstruct":pyarrow.string(),
    "l_shipmode":pyarrow.string(),
    "l_comment":pyarrow.string()
})
b = read_csv("test2",{
    "o_orderkey": pyarrow.uint64(),
    "o_custkey": pyarrow.uint64(),
    "o_orderstatus": pyarrow.string(),
    "o_totalprice": pyarrow.float64(),
    "o_orderdate": pyarrow.date32(),
    "o_orderpriority": pyarrow.string(),
    "o_clerk": pyarrow.string(),
    "o_shippriority": pyarrow.int32(),
    "o_comment": pyarrow.string()
})
c = read_csv("test3",{
    "c_custkey": pyarrow.uint64(),
    "c_name": pyarrow.string(),
    "c_address": pyarrow.string(),
    "c_nationkey": pyarrow.uint64(),
    "c_phone": pyarrow.string(),
    "c_acctbal": pyarrow.float64(),
    "c_mktsegment": pyarrow.string(),
    "c_comment": pyarrow.string()
})

testa = read_csv("test",{
    "key" : pyarrow.uint64(),
    "val1": pyarrow.uint64(),
    "val2": pyarrow.uint64(),
    "val3": pyarrow.uint64(),
})

testb = read_csv("test",{
    "key" : pyarrow.uint64(),
    "val1": pyarrow.uint64(),
    "val2": pyarrow.uint64(),
    "val3": pyarrow.uint64()
})


def do_12():

    d = join(a,b,left_on="l_orderkey", right_on="o_orderkey")
    d = d.filter(a.l_shipmode.isin({"MAIL","SHIP"}) & a.l_commitdate < a.l_receiptdate & a.l_shipdate < a.l_commitdate & a.l_receiptdate >= compute.strptime("1994-01-01",format="%Y-%m-%d",unit="s") &
    a.l_receiptdate < compute.strptime("1995-01-01",format="%Y-%m-%d",unit="s"))
    d["high"] = d.batch_apply(lambda x: ((x["o_orderpriority"] == "1-URGENT") | (x["o_orderpriority"] == "2-HIGH")).astype(int))
    d["low"] = d.batch_apply(lambda x: ((x["o_orderpriority"] != "1-URGENT") & (x["o_orderpriority"] != "2-HIGH")).astype(int))
    d.groupby_agg(groupby=["l_shipmode"], agg={'high':['sum'], 'low':['sum']})
    d.compute()

def test1():
    d = join(a,b,left_on="l_orderkey", right_on="o_orderkey")
    d = join(c,d,left_on="c_custkey", right_on="o_custkey")

    d = d.filter(d.l_commitdate < d.l_receiptdate)
    d = d.filter(d.l_commitdate > d.l_shipdate)
    d = d.filter(d.o_clerk > d.o_comment)
    d = d.filter(d.l_tax >d.o_totalprice)
    d = d.compute()
    sql.__push_filter__(d)
    d.walk()

def suffix_test():
    d = join(testa,testb, on="key")
    d = d.filter(d.val1 > d.val1_2)
    d = d.filter(d.val2_2 > d.val3_2)
    d = d.filter(d.val1_2 == d.val3_2)
    d = d.filter(d.val1 > d.val2)
    d = d.compute()
    sql.__push_filter__(d)
    d.walk()

def self_join_test():
    d = join(testa,testa, on="key")
    d = d.filter(d.val1 > d.val2)
    d = d.filter(d.val1 > d.val1_2)
    d = d.filter(d.val1_2 > d.val2_2)
    d = d.compute()
    sql.__push_filter__(d)
    d.walk()

#test1()
#suffix_test()
self_join_test()