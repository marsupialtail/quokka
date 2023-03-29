from sqlglot.dataframe.sql import functions as F
import sqlglot
import pyquokka.sql_utils as sql_utils

class Expression:
    def __init__(self, sqlglot_expr) -> None:
        # the sqlglot_expr here is not a sqlglot.exp but rather a sqlglot.dataframe.sql.column.Column, to make programming easier
        # you can get the corresponding sqlglot.exp by calling the expression attribute
        self.sqlglot_expr = sqlglot_expr

    def sql(self) -> str:
        def dfs(node):
            for k, v in node.iter_expressions():
                dfs(v)
            if isinstance(node, sqlglot.exp.Binary):
                node.replace(sqlglot.exp.Paren(this = node.copy()))
        
        dfs(self.sqlglot_expr.expression)
        return self.sqlglot_expr.expression.sql(dialect = "duckdb")
    
    def required_columns(self) -> set:
        return sql_utils.required_columns_from_exp(self.sqlglot_expr.expression)

    def __repr__(self):
        return "Expression({})".format(self.sql())
    
    def __str__(self):
        return "Expression({})".format(self.sql())

    def __eq__(self, other):  # type: ignore
        if isinstance(other, Expression):
            return Expression(self.sqlglot_expr == other.sqlglot_expr)
        else:
            return Expression(self.sqlglot_expr == F.lit(other))

    def __ne__(self, other):  # type: ignore
        if isinstance(other, Expression):
            return Expression(self.sqlglot_expr != other.sqlglot_expr)
        else:
            return Expression(self.sqlglot_expr != F.lit(other))

    def __gt__(self, other):
        if isinstance(other, Expression):
            return Expression(self.sqlglot_expr > other.sqlglot_expr)
        else:
            return Expression(self.sqlglot_expr > F.lit(other))

    def __ge__(self, other):
        if isinstance(other, Expression):
            return Expression(self.sqlglot_expr >= other.sqlglot_expr)
        else:
            return Expression(self.sqlglot_expr >= F.lit(other))

    def __lt__(self, other):
        if isinstance(other, Expression):
            return Expression(self.sqlglot_expr < other.sqlglot_expr)
        else:
            return Expression(self.sqlglot_expr < F.lit(other))

    def __le__(self, other):
        if isinstance(other, Expression):
            return Expression(self.sqlglot_expr <= other.sqlglot_expr)
        else:
            return Expression(self.sqlglot_expr <= F.lit(other))

    def __and__(self, other):
        if isinstance(other, Expression):
            return Expression(self.sqlglot_expr & other.sqlglot_expr)
        else:
            return Expression(self.sqlglot_expr & F.lit(other))

    def __or__(self, other):
        if isinstance(other, Expression):
            return Expression(self.sqlglot_expr | other.sqlglot_expr)
        else:
            return Expression(self.sqlglot_expr | F.lit(other))

    def __mod__(self, other):
        if isinstance(other, Expression):
            return Expression(self.sqlglot_expr % other.sqlglot_expr)
        else:
            return Expression(self.sqlglot_expr % F.lit(other))

    def __add__(self, other):
        if isinstance(other, Expression):
            return Expression(self.sqlglot_expr + other.sqlglot_expr)
        else:
            return Expression(self.sqlglot_expr + F.lit(other))

    def __sub__(self, other):
        if isinstance(other, Expression):
            return Expression(self.sqlglot_expr - other.sqlglot_expr)
        else:
            return Expression(self.sqlglot_expr - F.lit(other))

    def __mul__(self, other):
        if isinstance(other, Expression):
            return Expression(self.sqlglot_expr * other.sqlglot_expr)
        else:
            return Expression(self.sqlglot_expr * F.lit(other))

    def __truediv__(self, other):
        if isinstance(other, Expression):
            return Expression(self.sqlglot_expr / other.sqlglot_expr)
        else:
            return Expression(self.sqlglot_expr / F.lit(other))

    def __div__(self, other):
        if isinstance(other, Expression):
            return Expression(self.sqlglot_expr / other.sqlglot_expr)
        else:
            return Expression(self.sqlglot_expr / F.lit(other))
    
    def __neg__(self):
        return Expression(-self.sqlglot_expr)

    def __radd__(self, other):
        if isinstance(other, Expression):
            return Expression(other.sqlglot_expr + self.sqlglot_expr)
        else:
            return Expression(F.lit(other) + self.sqlglot_expr)

    def __rsub__(self, other):
        if isinstance(other, Expression):
            return Expression(other.sqlglot_expr - self.sqlglot_expr)
        else:
            return Expression(F.lit(other) - self.sqlglot_expr)

    def __rmul__(self, other):
        if isinstance(other, Expression):
            return Expression(other.sqlglot_expr * self.sqlglot_expr)
        else:
            return Expression(F.lit(other) * self.sqlglot_expr)

    def __rdiv__(self, other):
        if isinstance(other, Expression):
            return Expression(other.sqlglot_expr / self.sqlglot_expr)
        else:
            return Expression(F.lit(other) / self.sqlglot_expr)

    def __rtruediv__(self, other):
        if isinstance(other, Expression):
            return Expression(other.sqlglot_expr / self.sqlglot_expr)
        else:
            return Expression(F.lit(other) / self.sqlglot_expr)

    def __rmod__(self, other):
        if isinstance(other, Expression):
            return Expression(other.sqlglot_expr % self.sqlglot_expr)
        else:
            return Expression(F.lit(other) % self.sqlglot_expr)

    def __pow__(self, power):
        if isinstance(power, Expression):
            return Expression(self.sqlglot_expr ** power.sqlglot_expr)
        else:
            return Expression(self.sqlglot_expr ** power)

    def __rpow__(self, power):
        if isinstance(power, Expression):
            return Expression(power.sqlglot_expr ** self.sqlglot_expr)
        else:
            return Expression(power ** self.sqlglot_expr)

    def __invert__(self):
        return Expression(~self.sqlglot_expr)

    def __rand__(self, other):
        if isinstance(other, Expression):
            return Expression(other.sqlglot_expr & self.sqlglot_expr)
        else:
            return Expression(F.lit(other) & self.sqlglot_expr)

    def __ror__(self, other):
        if isinstance(other, Expression):
            return Expression(other.sqlglot_expr | self.sqlglot_expr)
        else:
            return Expression(F.lit(other) | self.sqlglot_expr)
    
    @property
    def str(self):
        return ExprStringNameSpace(self)
    
    @property
    def dt(self):
        return ExprDateTimeNameSpace(self)
    
class ExprStringNameSpace:
    def __init__(self, Expression) -> None:
        self.expr = Expression
    
    def to_uppercase(self):

        """
        Same as polars.Expr.str.to_uppercase
        """

        return Expression(F.upper(self.expr.sqlglot_expr))
    
    def to_lowercase(self):

        """
        Same as polars.Expr.str.to_lowercase
        """

        return Expression(F.lower(self.expr.sqlglot_expr))
    
    def contains(self, s):

        """
        Same as polars.Expr.str.contains

        Args:
            s (str): The string to check if the expression contains
        """

        assert type(s) == str
        return Expression(self.expr.sqlglot_expr.like("*{}*".format(s)))
    
    def starts_with(self, s):

        """
        Same as polars.Expr.str.starts_with

        Args:
            s (str): The string to check if the expression starts with
        """

        assert type(s) == str
        return Expression(self.expr.sqlglot_expr.like("{}*".format(s)))
    
    def ends_with(self, s):

        """
        Same as polars.Expr.str.ends_with

        Args:
            s (str): The string to check if the expression ends with
        """

        assert type(s) == str
        return Expression(self.expr.sqlglot_expr.like("*{}".format(s)))
    
    def length(self):

        """
        Same as polars.Expr.str.length
        """

        return Expression(F.length(self.expr.sqlglot_expr))
    
    def json_extract(self, field):

        """
        Similar polars.Expr.str.json_extract, however you can only extract one field! You must specify the field.
        If the field is not in the json, the value will be null.

        Args:
            field (str): The field to extract from the json
        
        Examples:

            >>> qc = QuokkaContext()
            >>> a = polars.from_dict({"a":[1,1,2,2], "c": ["A","B","C","D"], "b":['{"my_field": "quack"}','{"my_field": "quack"}','{"my_field": "quack"}','{"my_field": "quack"}']})
            >>> a = qc.from_polars(a)
            >>> a.with_columns({"my_field": a["b"].str.json_extract("my_field")}).collect()
            
        """
        assert type(self.expr.sqlglot_expr.expression) == sqlglot.exp.Column, "json_extract can only be applied to an untransformed column"
        col_name = self.expr.sqlglot_expr.expression.name
        return Expression(sqlglot.dataframe.sql.Column(sqlglot.parse_one("json_extract_string({}, '{}')".format(col_name, field))))
        

    def strptime(self, format = "datetime"):
        """
        Parse the string expression to a datetime/date/time type. The string must have format "%Y-%m-%d %H:%M:%S.%f"

        You should specify a time format, otherwise it will default to datetime.

        Args:
            format (str): "datetime" (default) | "date" | "time"
        """
        return Expression(self.expr.sqlglot_expr.cast(format))

    def hash(self):
        return Expression(F.hash(self.expr.sqlglot_expr))
    

class ExprDateTimeNameSpace:
    def __init__(self, Expression) -> None:
        self.expr = Expression

    def hour(self):

        """
        Same as polars.Expr.dt.hour
        """

        return Expression(F.hour(self.expr.sqlglot_expr))
    
    def minute(self):

        """
        Same as polars.Expr.dt.minute
        """

        return Expression(F.minute(self.expr.sqlglot_expr))
    
    def second(self):

        """
        Same as polars.Expr.dt.second
        """

        return Expression(F.second(self.expr.sqlglot_expr))
    
    def millisecond(self):

        """
        Same as polars.Expr.dt.millisecond
        """

        return Expression(sqlglot.dataframe.sql.Column(sqlglot.exp.Anonymous(this = "millisecond", expressions = [self.expr.sqlglot_expr.expression])))
    
    def microsecond(self):

        """
        Same as polars.Expr.dt.microsecond
        """

        return Expression(sqlglot.dataframe.sql.Column(sqlglot.exp.Anonymous(this = "microsecond", expressions = [self.expr.sqlglot_expr.expression])))
    
    def weekday(self):

        """
        Same as polars.Expr.dt.weekday
        """

        return Expression(sqlglot.dataframe.sql.Column(sqlglot.exp.Anonymous(this = "dayofweek", expressions = [self.expr.sqlglot_expr.expression])))

    def week(self):

        """
        Same as polars.Expr.dt.week
        """

        return Expression(sqlglot.dataframe.sql.Column(sqlglot.exp.Anonymous(this = "weekofyear", expressions = [self.expr.sqlglot_expr.expression])))

    def month(self):

        """
        Same as polars.Expr.dt.month
        """

        return Expression(F.month(self.expr.sqlglot_expr))
    
    def year(self):

        """
        Same as polars.Expr.dt.year
        """

        return Expression(F.year(self.expr.sqlglot_expr))

    def offset_by(self, num, unit):

        """
        Functionally same as polars.Expr.dt.offset_by. However arguments are different.

        Args:
            num (int or float): number of units to offset by
            unit (str): unit of offset. One of "ms", "s", "m", "h", "d", "M", "y"
        """

        assert type(unit) == str and unit in {"ms", "s", "m", "h", "d",  "M", "y"}, "unit must be one of 'ms', 's', 'm', 'h', 'd', 'M', 'y'"
        mapping = {"ms": "millisecond", "s": "second", "m": "minute", "h": "hour", "d": "day", "M": "month", "y": "year"}
        if type(num) == int or type(num) == float:
            return Expression(self.expr.sqlglot_expr + sqlglot.parse_one("interval {} {}".format(num, mapping[unit])))
        else:
            raise Exception("num must be an int or float. Offseting by a column is not supported yet")
    
    def strftime(self):

        """
        Same as polars.Expr.dt.strftime, except no need to pass in a format string, because the only format accepted is "%Y-%m-%d %H:%M:%S.%f"
        """

        return Expression(self.expr.sqlglot_expr.cast("string"))
    