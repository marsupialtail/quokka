import sqlglot
import sqlglot.expressions as exp
import pyarrow.compute as compute
from datetime import datetime
from pyarrow import compute
from functools import partial, reduce
import operator
import polars


def is_cast_to_date(x):
    return type(x) == exp.Cast and type(x.args['to']) == exp.DataType

def required_columns_from_exp(node):
    return set(i.name for i in node.find_all(sqlglot.expressions.Column))

def apply_conditions_to_batch(funcs, batch):
    for func in funcs:
        batch = batch[func]
    return batch

def label_sample_table_names(predicate):
    """
    Replace all table name references in the given predicate with 'sample'.
    Args:
    predicate (SQLGLot exp): precondition that predicate should only have one unique table name. If it doesn't, return an error.
    """
    w = predicate.copy()
    columns = w.find_all(exp.Column)
    tables = set()
    for c in columns:
        tables.add(c.table)
        c.replace(sqlglot.parse_one('sample.' + c.name))
    assert(len(tables) <= 1)
    return w

def filters_to_expression(filters):
    """
    Check if filters are well-formed.

    See _DNF_filter_doc above for more details.
    """
    import pyarrow.dataset as ds

    if isinstance(filters, ds.Expression):
        return filters

    #filters = _check_filters(filters, check_null_strings=False)

    def convert_single_predicate(col, op, val):
        field = ds.field(col)

        if op == "=" or op == "==":
            return field == val
        elif op == "!=":
            return field != val
        elif op == '<':
            return field < val
        elif op == '>':
            return field > val
        elif op == '<=':
            return field <= val
        elif op == '>=':
            return field >= val
        elif op == 'in':
            return field.isin(val)
        elif op == 'not in':
            return ~field.isin(val)
        else:
            raise ValueError(
                '"{0}" is not a valid operator in predicates.'.format(
                    (col, op, val)))

    conjunction_members = [convert_single_predicate(col, op, val) for col, op, val in filters]

    return reduce(operator.and_, conjunction_members)


def evaluate(node):
    node = node.unnest()
    if issubclass(type(node), sqlglot.expressions.AggFunc):
        arg = evaluate(node.this)
        if type(node) == sqlglot.expressions.Sum:
            return arg.sum()
        elif type(node) == sqlglot.expressions.Count:
            return polars.count()
        elif type(node) == sqlglot.expressions.Avg:
            return arg.mean()
        elif type(node) == sqlglot.expressions.Min:
            return arg.min()
        elif type(node) == sqlglot.expressions.Max:
            return arg.max()
        elif type(node) == sqlglot.expressions.Std:
            return arg.std()
        elif type(node) == sqlglot.expressions.Variance:
            return arg.var()
        else:
            raise Exception("Unsupported aggregation function")
            
    elif issubclass(type(node) , sqlglot.expressions.Binary) and not issubclass(type(node), sqlglot.expressions.Connector):
        lf = evaluate(node.left)
        rf = evaluate(node.right)
        if type(node) == sqlglot.expressions.Div:    
            return lf / rf
        elif type(node) == sqlglot.expressions.Mul:
            return lf * rf
        elif type(node) == sqlglot.expressions.Add:
            return lf + rf
        elif type(node) == sqlglot.expressions.Sub:
            return lf - rf
        elif type(node) == sqlglot.expressions.EQ:
            return lf == rf
        elif type(node) == sqlglot.expressions.NEQ:
            return lf != rf
        elif type(node) == sqlglot.expressions.GT:
            return lf > rf
        elif type(node) == sqlglot.expressions.GTE:
            return lf >= rf
        elif type(node) == sqlglot.expressions.LT:
            return lf < rf
        elif type(node) == sqlglot.expressions.LTE:
            return lf <= rf
        elif type(node) == sqlglot.expressions.Like:
            assert node.expression.is_string
            filter = node.expression.this
            if filter[0] == '%' and filter[-1] == '%':
                filter = filter[1:-1]
                stuff = filter.split("%")
                return reduce(operator.and_, [lf.str.contains(i) for i in stuff])
            elif filter[0] != '%' and filter[-1] == '%':
                filter = filter[:-1]
                return lf.str.starts_with(filter)
            elif filter[0] == '%' and filter[-1] != '%':
                filter = filter[1:]
                return lf.str.ends_with(filter)
            elif filter[0] != '%' and filter[-1] != '%':
                return lf == filter
        else:
            print(type(node))
            raise Exception("making predicate failed")
    elif type(node) == sqlglot.expressions.And:   
        lf = evaluate(node.left)
        rf = evaluate(node.right)
        return lf & rf
    elif type(node) == sqlglot.expressions.Or: 
        lf = evaluate(node.left)
        rf = evaluate(node.right)
        return lf | rf
    elif type(node) == sqlglot.expressions.Not: 
        lf = evaluate(node.this)
        return ~ lf
    elif type(node) == sqlglot.expressions.Case:
        default = evaluate(node.args["default"])
        if len(node.args["ifs"]) > 1:
            raise Exception("only support single when in case statement for now")
        when = node.args["ifs"][0]
        predicate = evaluate(when.this)
        if_true = evaluate(when.args['true'])
        return predicate * if_true + (~predicate) * default
        
    elif type(node) == sqlglot.expressions.In:

        # the types should work out even without conversion, is_in with polars work if the list is string and the type is int.
        lf = evaluate(node.this)
        l = [k.name for k in node.args['expressions']]
        return lf.is_in(l)

    elif type(node) == sqlglot.expressions.Between:
        pred = evaluate(node.this)
        low = evaluate(node.args['low'])
        high = evaluate(node.args['high'])
        return ((pred >= low) & (pred <= high))
    elif type(node) == sqlglot.expressions.Literal:
        if node.is_string:
            return node.this
        else:
            if "." in node.this:
                return float(node.this)
            else:
                return int(node.this)
    elif type(node) == sqlglot.expressions.Column:
        return polars.col(node.name)
    elif is_cast_to_date(node):
        # If a column is being casted to date, then just return the column
        if isinstance(node.this, sqlglot.expressions.Column):
          return evaluate(node.this)
        try:
            d = datetime.strptime(node.name, "%Y-%m-%d")
        except:
            raise Exception("failed to parse date object, currently only accept strs of YY-mm-dd")
        return d
    elif type(node) == sqlglot.exp.Boolean:
        if node.this:
            return True
        else:
            return False
    else:
        print(node)
        print(type(node))
        raise Exception("making predicate failed")

def parquet_condition_decomp(condition):
    
    def key_to_symbol(k):
        mapping = {"eq":"==","neq":"!=","lt":"<","lte":"<=","gt":">","gte":">=","in":"in"}
        return mapping[k]
    
    def handle_literal(node):
        if node.is_string:
            return node.this
        else:
            if "." in node.this:
                return float(node.this)
            else:
                return int(node.this)
    
    conjuncts = list(
                        condition.flatten()
                        if isinstance(condition, sqlglot.exp.And)
                        else [condition]
                    )

    filters = []
    remaining_predicate = sqlglot.exp.TRUE
    for node in conjuncts:
        if type(node) in {exp.GT, exp.GTE, exp.LT, exp.LTE, exp.EQ, exp.NEQ}:
            if type(node.left) == exp.Column:
                if type(node.right) == exp.Literal:
                    filters.append((node.left.name, key_to_symbol(node.key), handle_literal(node.right)))
                    continue
                # don't handle other types of casts
                elif is_cast_to_date(node.right):
                    filters.append((node.left.name, key_to_symbol(node.key), compute.strptime(node.right.name,format="%Y-%m-%d",unit="s")))
                    continue
            elif type(node.right) == exp.Column: 
                if type(node.left) == exp.Literal:
                    filters.append((handle_literal(node.left), key_to_symbol(node.key), node.right.name))
                    continue
                # don't handle other types of casts
                elif is_cast_to_date(node.left):
                    filters.append((compute.strptime(node.left.name,format="%Y-%m-%d",unit="s"),  key_to_symbol(node.key), node.right.name))
                    continue
        elif type(node) == exp.In:
            if type(node.this) == exp.Column:
                if all([type(i) == exp.Literal for i in node.args["expressions"]]):
                    filters.append((node.this.name, "in", [handle_literal(i) for i in node.args["expressions"]]))
                    continue
                elif all([is_cast_to_date(i) for i in node.args["expressions"]]):
                    filters.append((node.this.name, "in", [compute.strptime(i.name,format="%Y-%m-%d",unit="s") for i in node.args["expressions"]]))
                    continue
                else:
                    raise Exception("Incongrent types in IN clause")
            else:
                raise Exception("left operand of IN clause must be column")
        elif type(node) == exp.Between:
            if type(node.this) == exp.Column:
                if type(node.args["low"]) == exp.Literal and type(node.args["high"]) == exp.Literal:
                    filters.append((node.this.name, ">=", handle_literal(node.args["low"])))
                    filters.append((node.this.name, "<=", handle_literal(node.args["high"])))
                    continue
                elif is_cast_to_date(node.args["low"]) and is_cast_to_date(node.args["high"]):
                    filters.append((node.this.name, ">=", compute.strptime(node.args["low"].name,format="%Y-%m-%d",unit="s")))
                    filters.append((node.this.name, "<=", compute.strptime(node.args["high"].name,format="%Y-%m-%d",unit="s")))
                    continue
                else:
                    raise Exception("Incogruent types for Between clause")
            raise Exception("left operand of Between clause must be column")
        
        #print("I cannot become a predicate!", node.sql(pretty=True))
        remaining_predicate = sqlglot.exp.and_(remaining_predicate, node)
    
    return filters, remaining_predicate

def csv_condition_decomp(condition):
    conjuncts = list(sqlglot.parse_one(condition).flatten())

    batch_funcs = []

    for node in conjuncts:
        batch_funcs.append(evaluate(node))
    
    return partial(apply_conditions_to_batch, batch_funcs),  required_columns_from_exp(condition)

def parse_aggregations(expr):
    """
    For parsing complex aggregation expressions.

    Args:
        e (sqlglot expression): Expression representing aggregation

    Returns: 
        agg_list (list): List of strings representing simple aggregations (root node is sum, count, etc).
        expr (sqlglot expression): Original expression written in terms of aggs.
        If there is no aggregation, return ([], e).

    Examples:
        Input: 2 * COUNT(*)
        Output: (["COUNT(*) as agg_0"], 2*agg_0)

        Input: SUM(l_partkey + 1) / SUM(l_orderkey)
        Output: (['SUM(l_partkey + 1) as agg_0', 'SUM(l_orderkey) as agg_1'], agg_0 / agg_1)

        Intput: 1/(MIN(l_orderkey) * SUM(l_partkey) + COUNT(*))
        Output: (['COUNT(*) as agg_0', 'MIN(l_orderkey) as agg_1', 'SUM(l_partkey) as agg_2'], 1/(agg_1 * agg_2 + agg_0))
    """
    e = expr.copy()
    aggregations = [i for i in e.find_all(exp.AggFunc)]
    if len(aggregations) == 0: return [], e

    agg_list = []
    for i, a in enumerate(aggregations):
        new_name = "agg_" + str(i)
        agg_list.append(a.sql() + " as " + new_name)
        if a == e: e = e.replace(sqlglot.parse_one(new_name))
        else: a.replace(sqlglot.parse_one(new_name))
    return agg_list, e
