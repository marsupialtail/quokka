from pyquokka import QuokkaContext
import polars

qc = QuokkaContext()

trades = qc.read_sorted_csv("test_trade2.csv", has_header = True, sorted_by = "time")
quotes = qc.read_sorted_csv("test_quote2.csv", has_header = True, sorted_by = "time")

result = trades.join_asof(quotes, on = "time", by = "symbol")
result.explain()
result = result.collect().sort('symbol').drop_nulls()

quotes = polars.read_csv("test_quote2.csv")
trades = polars.read_csv("test_trade2.csv")
ref = trades.join_asof(quotes,by="symbol",on="time").sort("symbol").drop_nulls()
print(ref.sort("symbol"))

print((result["size"] - ref["size"]).sum())
