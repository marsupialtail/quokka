from sqlglot.dataframe.sql import functions as F
import sqlglot

class Expression:
    def __init__(self, sqlglot_expr) -> None:
        # the sqlglot_expr here is not a sqlglot.exp but rather a sqlglot.dataframe.sql.column.Column, to make programming easier
        # you can get the corresponding sqlglot.exp by calling the expression attribute
        self.sqlglot_expr = sqlglot_expr

    def sql(self) -> str:
        return self.sqlglot_expr.sql()

    def __repr__(self):
        return self.sql()
    
    def __str__(self):
        return self.sql()

    def __eq__(self, other):  # type: ignore
        return Expression(self.sqlglot_expr == other.sqlglot_expr)

    def __ne__(self, other):  # type: ignore
        return Expression(self.sqlglot_expr != other.sqlglot_expr)

    def __gt__(self, other):
        return Expression(self.sqlglot_expr > other.sqlglot_expr)

    def __ge__(self, other):
        return Expression(self.sqlglot_expr >= other.sqlglot_expr)

    def __lt__(self, other):
        return Expression(self.sqlglot_expr < other.sqlglot_expr)

    def __le__(self, other):
        return Expression(self.sqlglot_expr <= other.sqlglot_expr)

    def __and__(self, other):
        return Expression(self.sqlglot_expr & other.sqlglot_expr)

    def __or__(self, other):
        return Expression(self.sqlglot_expr | other.sqlglot_expr)

    def __mod__(self, other):
        return Expression(self.sqlglot_expr % other.sqlglot_expr)

    def __add__(self, other):
        return Expression(self.sqlglot_expr + other.sqlglot_expr)

    def __sub__(self, other):
        return Expression(self.sqlglot_expr - other.sqlglot_expr)

    def __mul__(self, other):
        return Expression(self.sqlglot_expr * other.sqlglot_expr)

    def __truediv__(self, other):
        return Expression(self.sqlglot_expr / other.sqlglot_expr)

    def __div__(self, other):
        return Expression(self.sqlglot_expr / other.sqlglot_expr)
    
    def __neg__(self):
        return Expression(-self.sqlglot_expr)

    def __radd__(self, other):
        return Expression(other.sqlglot_expr + self.sqlglot_expr)

    def __rsub__(self, other):
        return Expression(other.sqlglot_expr - self.sqlglot_expr)

    def __rmul__(self, other):
        return Expression(other.sqlglot_expr * self.sqlglot_expr)

    def __rdiv__(self, other):
        return Expression(other.sqlglot_expr / self.sqlglot_expr)

    def __rtruediv__(self, other):
        return Expression(other.sqlglot_expr / self.sqlglot_expr)

    def __rmod__(self, other):
        return Expression(other.sqlglot_expr % self.sqlglot_expr)

    def __pow__(self, power):
        return Expression(self.sqlglot_expr ** power.sqlglot_expr)

    def __rpow__(self, power):
        return Expression(power.sqlglot_expr ** self.sqlglot_expr)

    def __invert__(self):
        return Expression(~self.sqlglot_expr)

    def __rand__(self, other):
        return Expression(other.sqlglot_expr & self.sqlglot_expr)

    def __ror__(self, other):
        return Expression(other.sqlglot_expr | self.sqlglot_expr)
    
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
        return Expression(F.upper(self.expr.sqlglot_expr))
    
    def to_lowercase(self):
        return Expression(F.lower(self.expr.sqlglot_expr))
    
    def contains(self, s):
        assert type(s) == str
        return Expression(self.expr.sqlglot_expr.like("*{}*".format(s)))
    
    def starts_with(self, s):
        assert type(s) == str
        return Expression(self.expr.sqlglot_expr.like("{}*".format(s)))
    
    def ends_with(self, s):
        assert type(s) == str
        return Expression(self.expr.sqlglot_expr.like("*{}".format(s)))
    
    def length(self):
        return Expression(F.length(self.expr.sqlglot_expr))
    
    def json_extract(self, field):
        """
        If the field is not in the json, it will return null
        """
        assert type(self.expr.sqlglot_expr.expression) == sqlglot.exp.Column, "json_extract can only be applied to an untransformed column"
        col_name = self.expr.sqlglot_expr.expression.name
        return Expression(sqlglot.dataframe.sql.Column(sqlglot.parse_one("json_extract_string({}, '{}')".format(col_name, field))))
        

    def strptime(self, format = "datetime"):
        """
       Parse the string expression to a datetime/date/time type

        Args:
            format (str): "datetime" (default) | "date" | "time"
        """
        return self.expr.sqlglot_expr.cast(format)

    def hash(self):
        return Expression(F.hash(self.expr.sqlglot_expr))
    

class ExprDateTimeNameSpace:
    def __init__(self, Expression) -> None:
        self.expr = Expression

    def hour(self):
        return Expression(F.hour(self.expr.sqlglot_expr))
    
    def minute(self):
        return Expression(F.minute(self.expr.sqlglot_expr))
    
    def second(self):
        return Expression(F.second(self.expr.sqlglot_expr))
    
    def millisecond(self):
        return Expression(sqlglot.dataframe.sql.Column(sqlglot.exp.Anonymous(this = "millisecond", expressions = [self.expr.sqlglot_expr.expression])))
    
    def microsecond(self):
        return Expression(sqlglot.dataframe.sql.Column(sqlglot.exp.Anonymous(this = "microsecond", expressions = [self.expr.sqlglot_expr.expression])))
    
    def weekday(self):
        return Expression(F.dayofweek(self.expr.sqlglot_expr))

    def week(self):
        return Expression(F.weekofyear(self.expr.sqlglot_expr))

    def month(self):
        return Expression(F.month(self.expr.sqlglot_expr))

    def offset_by(self, num, unit):
        assert type(unit) == str and unit in {"ms", "s", "m", "h", "d", "w", "M", "y"}, "unit must be one of 'ms', 's', 'm', 'h', 'd', 'w', 'M', 'y'"
        if type(num) == int or type(num) == float:
            return Expression(self.expr.sqlglot_expr + F.lit(num).cast("interval {}".format(unit)))
        elif type(num) == Expression:
            return Expression(self.expr.sqlglot_expr + num.sqlglot_expr.cast("interval {}".format(unit)))
        else:
            raise Exception("num must be int, float or Expression")
    