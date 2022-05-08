import sqlglot
from pyarrow import compute

def stringify(x):
    if type(x) == sqlglot.expressions.Literal:
        return x.this
    elif type(x) == sqlglot.expressions.Column:
        return "x['" + x.this.this + "']"
    elif type(x) == sqlglot.expressions.Cast:
        # this will work for now, when there is a date in the predicate, it's never going to be added to another string, and will always be pushe into the prefiler list.
        if type(x.args["to"]) == sqlglot.expressions.DataType:
            return compute.strptime(x.this.this, format='%Y-%m-%d',unit='s')
            #return "compute.strptime('" + x.this.this + "',format='%Y-%m-%d',unit='s')"
        else:
            raise Exception("cast not supported")
    elif type(x) == sqlglot.expressions.Add:
        raise Exception("no math yet")

def key_to_symbol(k):
    if k == "eq":
        return "=="
    elif k == "neq":
        return "!="
    elif k == "lt":
        return "<"
    elif k == "lte":
        return "<="
    elif k == "gt":
        return ">"
    elif k == "gte":
        return ">="