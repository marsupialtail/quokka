from df import * 
f1 = FilterCondition((a.l_orderkey, '>' , b.o_orderkey))
f2 = FilterCondition((a.l_shipmode, '>' , a.l_commitdate))
f3 = FilterCondition((a.l_returnflag, '>' , a.l_commitdate))
f4 = FilterCondition((b.o_orderdate, '>' , b.o_orderkey))
p = Predicate()
p._and(f1)
p._and(f2)
p._or(f3)
p._and(f4)
#p.specialize(set(a.schema.keys()))

