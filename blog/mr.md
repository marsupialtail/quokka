# How to do pattern recognition in 2023?
SQL introduced the MATCH_RECOGNIZE (abbreviated to MR) syntax a while ago. Now Oracle, Snowflake and Trino all support MR for offline time series analytics; Azure Stream Analytics and Flink support MR for online analytics use cases. If you don't know what it is, I recommend checking it out [here](https://docs.snowflake.com/en/sql-reference/constructs/match_recognize#troubleshooting).

The pattern syntax is very general. It allows you to define almost any pattern based on what I call **anchor events**, which can be any arbitrary SQL predicate, and a **pattern** based on these events. For example, give stock price data, you can define event **A**: `price > lag(price)` and **B**: `price < lag(price)` and a pattern `A+B`, which will find all patterns where a sequence of price increases are followed by a price decrease. 

However, it seems to me the MR syntax design is quite flawed. I will name two main issues. 

The first is that the MR syntax finds all matches that **starts** at a certain row instead of **ending** at a certain row. For example, you can use the MR syntax to find all patterns of stock movements as demonstrated above, but the query will return to you by default *the first row in each matched pattern*. After a pattern is matched, it will typically move on to the next row *after the end of the current pattern*. This is a bit off-putting, as a stock trader, I typically care about getting all the rows where a valid pattern match *ends*, not where a valid pattern match *starts*, since I can only trade after the pattern ends!

I have yet to find a way using MR to reliably compute this. If someone knows how to do it, I'd appreciate a comment. 

The second 
