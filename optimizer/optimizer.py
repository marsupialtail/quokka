from numpy import alltrue
from utils import * 

"""
Here is what we are going to do for a simple query of the form
SELECT cols
FROM
table1, table2, ... 
WHERE
cond AND cond AND cond (where cond can be col BOOL col or col BOOL const) Important: join conditions must be listed here too!
GROUPBY
cols
ORDERBY
cols
LIMIT
num

We are going to:
1) go to the SELECT, WHERE and GROUPBY/ORDERBY clause and figure out all the columns you need from each table for the query. some of them could be present in columns argument others in filter argument
    - if input is CSV/JSON: then you need all the columns that are mentioned. You need to generate a filter that uses pa.compute that does everything, and then project out the columns you need later in 
    SELECT, the join keys and GROUPBY/ORDERBY
    - if input is parquet: if a column is only involved in comparison with a constant, you can put it in the filter string. otherwise, you need to add it to the columns projection, and then 
    generate the pa.compute thing as in CSV input
    
    now you can add the input nodes to the quokka graph.

2) add the join to the input nodes. You will pick the appropriate join operator depending on the number of operators
    - now you need to generate the batch func for the join. this depends on the groupby. First you will project out the things you still need for SELECT and GROUPBY. Then you will generate the 
    local aggregation

3) generate the final aggregation for the groupby, maintain in sorted order, now project out only the things you need for SELECT

4) add an optional limit operator
"""

class SQLContext:
    def __init__(self) -> None:
        self.catalog = {} # dict of table name to tuple( table_location, table_type, schema, which is a dict of column name to data type)
        self.column_to_table = {}

    def register_table(self, table_name, table_location, schema, table_type = "parquet"):
        self.catalog[table_name] = {"table_location" : table_location , "table_type" : table_type, "schema" : schema}
        for col in schema: # no duplicate column names in our system, in the future we can rename stuff right here by appending table name
            assert col not in self.column_to_table
            self.column_to_table[col] = table_name

    def get_conditions(self, x, all_true, join_key_pairs, prefilter_pairs):
        if x.key == "paren":
            return self.get_conditions( x.this, all_true, join_key_pairs, prefilter_pairs)

        if x.key == "in":
            columns_found = set([i[0].this for i in x.walk() if type(i[1]) == sqlglot.expressions.Column])
            # join condtiions will always be True. Any queries with "or" join conditions require an outer join, which we don't implement.
            tables_they_belong = set(self.column_to_table[col] for col in columns_found)
            assert(len(columns_found) == 1 and len(tables_they_belong) == 1)
            table = tables_they_belong.pop()
            if self.catalog[table]["table_type"] == "parquet" and all_true:
                prefilter_pairs[table].append((x.this.this.this, x.key, [i.this for i in x.args['expressions']]))
                return True
            else:
                return "compute.is_in(x['" + x.this.this.this + "'],value_set = pyarrow.array([" + ",".join([i.this for i in x.args['expressions']]) + "]))"
            
        if x.key == "eq" or x.key == "neq" or x.key == "gt" or x.key == "gte" or x.key == "lt" or x.key == "lte":
            lhs = stringify(x.this)
            rhs = stringify(x.args["expression"])
            columns_found = set([i[0].this for i in x.walk() if type(i[1]) == sqlglot.expressions.Column])
            # join condtiions will always be True. Any queries with "or" join conditions require an outer join, which we don't implement.
            tables_they_belong = set(self.column_to_table[col] for col in columns_found)
            
            assert len(columns_found) > 0

            if len(columns_found) == 2 and len(tables_they_belong) == 2:
                if type(x.this) == sqlglot.expressions.Column and type(x.args["expression"]) == sqlglot.expressions.Column:
                    join_key_pairs.append((x.this.this.this, x.args["expression"].this.this))
                    return True
                else:
                    raise Exception("does not support complicated join predicates")
            
            # can only put stuff in the prefilter string if they are not too complicated and it must be true all the way down the tree
            elif len(columns_found) == 1 and (type(x.this) == sqlglot.expressions.Column or  type(x.args["expression"]) == sqlglot.expression.Column) and  all_true:
                assert len(tables_they_belong) == 1
                table = tables_they_belong.pop()
                if self.catalog[table]["table_type"] == "parquet" and all_true:
                    if type(x.this) == sqlglot.expressions.Column:
                        prefilter_pairs[table].append((x.this.this.this, key_to_symbol(x.key), rhs))
                        return True
                    else:
                        prefilter_pairs[table].append((lhs, key_to_symbol(x.key), x.args["expression"].this.this))
                        return True
                
            if x.key == "gt":
                return "compute.greater(" + lhs + "," + rhs + ")"
            elif x.key == "gte":
                return "compute.greater_equal(" + lhs + "," + rhs + ")"
            elif x.key == "lt":
                return "compute.less(" + lhs + "," + rhs + ")"
            elif x.key == "lte":
                return "compute.less_equal(" + lhs + "," + rhs + ")"
            elif x.key == "eq":
                return "compute.equal(" + lhs + "," + rhs + ")"
            else:
                return "compute.not_equal(" + lhs + "," + rhs + ")"

        if x.key == "and":
            lhs = self.get_conditions(x.this, True * all_true, join_key_pairs, prefilter_pairs)
            rhs = self.get_conditions( x.args["expression"], True * all_true, join_key_pairs, prefilter_pairs)
            if lhs == True and rhs == True:
                return True
            elif lhs == True:
                return rhs
            elif rhs == True:
                return lhs
            else:
                return "compute.and(" + lhs + "," + rhs + ")"
        elif x.key == "or":
            lhs = self.get_conditions( x.this, False, join_key_pairs, prefilter_pairs)
            rhs = self.get_conditions( x.args["expression"],False, join_key_pairs, prefilter_pairs)
            if lhs == True or rhs == True:
                return True
            else:
                return "compute.or(" + lhs + "," + rhs + ")"

    def get_graph_from_sql(self, sql):

        x = sqlglot.parse_one(sql)

        tables_involved = set([i[1].this for i in x.args['from'].walk() if i[1] is not None and type(i[1]) == sqlglot.expressions.Identifier])
        assert tables_involved.issubset(set(self.catalog.keys()))

        table_prefilter = {table: [] for table in tables_involved if self.catalog[table]["table_type"] == "parquet"}

        all_identifiers = set([i[1].this for i in x.walk() if i[1] is not None and type(i[1]) == sqlglot.expressions.Identifier])
        
        x1 = x.copy()
        x1.args["where"] = None
        identifiers_not_in_where = set([i[1].this for i in x1.walk() if i[1] is not None and type(i[1]) == sqlglot.expressions.Identifier])

        # for parquet files, this is the set of columns between pre-filter and post-filer
        print("we are not going to take out the columns that are only used in the parquet.read_table filter because I am lazy. Do this at some point.")
        table_inter_columns = {table: set(self.catalog[table].keys()).intersection(all_identifiers) for table in tables_involved} 

        # for both parquet and csv files, this is the set of columns after post-filter
        table_columns = {table: set(self.catalog[table].keys()).intersection(identifiers_not_in_where) for table in tables_involved} 

        # get where conditions
        join_key_pairs = []
        table_postfilter = self.get_conditions(x.args["where"].this, True, join_key_pairs, table_prefilter)

        print(table_postfilter)
        print(join_key_pairs)
        print(table_prefilter)