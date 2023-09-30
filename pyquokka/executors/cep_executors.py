from pyquokka.executors.base_executor import * 
import polars, sqlglot, duckdb
import sqlglot.optimizer as optimizer
from collections import deque

def add_qualifier(query, name):
    return sqlglot.parse_one(query).transform(lambda node: sqlglot.exp.column(node.this, node.table or name) if isinstance(node, sqlglot.exp.Column) else node).sql()

def replace_qualifier(query, a, b):
    return sqlglot.parse_one(query).transform(lambda node: sqlglot.exp.column(node.this, (b if node.table == a else node.table)) if isinstance(node, sqlglot.exp.Column) else node).sql()


class CEPExecutor(Executor):
    def __init__(self, schema, events, time_col, max_span) -> None:

        """
        
        Arguments:
            schema {list} -- the schema of the stream
            events {list} -- a list of events, each event is a tuple
            time_col {str} -- the name of the time column
            max_span {int} -- the maximum span of the pattern
        
        """
        self.row_count_col = "__row_nr__"
        assert self.row_count_col not in schema
        assert "__ats__" not in schema
        assert "__cts__" not in schema
        assert time_col in schema
        self.time_col = time_col
        self.events = events
        self.max_span = max_span

        self.touched_columns = set()
        self.prefilter = sqlglot.exp.FALSE
        seen_events = set()
        self.event_prefilters = {}
        self.event_dependent_filters = {}
        
        # we need this for the filter condition in the range join. We can only include the part of the 
        # last event's event_dependent_filter that's dependent on the first event
        last_event_dependent_on_first_event_filter = sqlglot.exp.TRUE
        
        # we need this for the selection of the range join.
        for i in range(len(events)):
            event = events[i]
            event_prefilter = sqlglot.exp.TRUE
            event_dependent_filter = sqlglot.exp.TRUE
            event_name, event_filter = event

            if i != 0:
                assert event_filter is not None and event_filter != sqlglot.exp.TRUE, "only the first event can have no filter"

            assert event_name not in seen_events, "repeated event names not allowed"
            event_filter = sqlglot.parse_one(event_filter)
            conjuncts = list(event_filter.flatten() if isinstance(event_filter, sqlglot.exp.And) else [event_filter])
            for conjunct in conjuncts:
                conjunct_dependencies = set(i.table for i in conjunct.find_all(sqlglot.expressions.Column))
                conjunct_columns = set(i.name for i in conjunct.find_all(sqlglot.expressions.Column))
                for column in conjunct_columns:
                    assert column in schema
                self.touched_columns = self.touched_columns.union(conjunct_columns)
                conjunct_dependencies.remove('')
                assert conjunct_dependencies.issubset(seen_events), "later events can only depend on prior event"
                if len(conjunct_dependencies) == 0:
                    event_prefilter = sqlglot.exp.and_(event_prefilter, conjunct)
                else:
                    event_dependent_filter = sqlglot.exp.and_(event_dependent_filter, conjunct)
                    if len(conjunct_dependencies) == 1 and list(conjunct_dependencies)[0] == events[0][0]:
                        if i == len(events) - 1:
                            last_event_dependent_on_first_event_filter = sqlglot.exp.and_(
                                last_event_dependent_on_first_event_filter, conjunct)
                    
            event_prefilter = optimizer.simplify.simplify(event_prefilter)
            self.event_prefilters[event_name] = event_prefilter.sql()
            self.event_dependent_filters[event_name] = event_dependent_filter.sql()
            if event_prefilter != sqlglot.exp.TRUE:
                self.prefilter = sqlglot.exp.or_(self.prefilter, event_prefilter)
            seen_events.add(event_name)
        
        self.prefilter = optimizer.simplify.simplify(self.prefilter).sql()
        self.last_event_dependent_on_first_event_filter = optimizer.simplify.simplify(last_event_dependent_on_first_event_filter).sql()
        print(self.prefilter)
        print(self.touched_columns)

    def execute(self,batches,stream_id, executor_id):
        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        if len(batches) == 0:
            return
        df = polars.concat(batches)
        assert df[self.time_col].is_sorted, "data must be presorted on time column"
        con = duckdb.connect()
        con.execute("PRAGMA threads=8")
        # this is no longer necessary since we will push down the prefilter.
        # df = con.execute("select * from df where {} order by {}".format(self.prefilter, self.time_col)).arrow()
        df = df.with_row_count(self.row_count_col).to_arrow()

        first_event_name, first_event_filter = self.events[0]
        last_event_name, last_event_filter = self.events[-1]
        # start = time.time()
        first_event = con.execute("select * from df where {}".format(first_event_filter)).arrow()
        last_event = con.execute("select * from df where {}".format(self.event_prefilters[last_event_name])).arrow()

        processed_last_event_filter = replace_qualifier(add_qualifier(self.last_event_dependent_on_first_event_filter, "last_event"), first_event_name, "first_event")
                   
        range_join = """select first_event.{} as __arc__, first_event.{} as __ats__, 
        last_event.{} as __crc__, last_event.{} as __cts__, 
        from first_event, last_event
        where first_event.{} < last_event.{} and last_event.{} <= first_event.{} + {}
        and {} """.format(self.row_count_col, self.time_col, self.row_count_col, self.time_col, self.time_col, self.time_col, 
                            self.time_col, self.time_col, self.max_span, processed_last_event_filter)
        # start = time.time()
        result = con.execute(range_join).arrow()
        # print(time.time() - start)
                
        df = polars.from_arrow(df)
        matched_ends = []
        total_exec_times = []
        total_section_lengths = []
        
        overhead = 0

        matched_events = []
            
        # startt2 = time.time()
        for bound in polars.from_arrow(result).iter_rows(named = True):

            start_nr = bound["__arc__"]
            end_nr = bound["__crc__"]
            start_ts = bound["__ats__"]
            end_ts = bound["__cts__"]
            # print(start_nr)
            # match recognize default is one pattern per start. we can change it to one pattern per end too
            # if end_nr in matched_ends:
            if start_nr in matched_ends or end_nr <= start_nr:
                continue
            
            # start3 = time.time()
            my_section = df[start_nr: end_nr + 1] #.to_arrow()
            fate = {first_event_name + "." + col : my_section[col][0] for col in my_section.columns}
            # overhead += time.time() - start3
            stack = deque([(0, my_section[0], [fate], [start_ts])])
            
            while stack:
                marker, vertex, path, matched_event = stack.pop()
            
                remaining_df = my_section[marker:]# .to_arrow()
                next_event_name, next_event_filter = self.events[len(path)]
                
                # now fill in the next_event_filter based on the previous path
                for fate in path:
                    for fixed_col in fate:
                        next_event_filter = next_event_filter.replace(fixed_col, str(fate[fixed_col]))

                # startt = time.time()
                matched = polars.SQLContext(frame=remaining_df).execute("select {}, {} from frame where {}".format(self.row_count_col, self.time_col, next_event_filter)).collect()
                # total_exec_times.append(time.time() - startt)
                total_section_lengths.append(len(remaining_df))
                
                if len(matched) > 0:
                    if next_event_name == last_event_name:
                        matched_ends.append(start_nr)
                        matched_events.append(matched_event + [end_ts])
                        break
                    else:
                        for matched_row in matched.iter_rows(named = True):
                            my_fate = {next_event_name + "." + col : matched_row[col] for col in matched_row}
                            # the row count of the matched row in the section is it's row count col value minus
                            # the start row count of the section
                            stack.append((matched_row[self.row_count_col] - start_nr, matched_row, path + [my_fate], matched_event + [matched_row[self.time_col]]))
        
        # print("Total loop time", time.time() - startt2)
        # print(total_section_lengths)
        # print(sum(total_section_lengths))
        # print("Total filtering time in loop", sum(total_exec_times))
        
        # print("overhead", overhead)
        return matched_ends

    def done(self,executor_id):
        raise NotImplementedError

from collections import namedtuple
from tqdm import tqdm

def repeat_row(row, n):
    assert len(row) == 1
    # repeat a polars dataframe row n times
    return row.select([polars.col(col).repeat_by(n).explode() for col in row.columns])

def replace_with_dict(string, d):
    for k in d:
        string = string.replace(k, d[k])
    return string

def nfa_cep(batch, events, time_col, max_span):
    
    assert type(batch) == polars.DataFrame, "batch must be a polars DataFrame"
    assert batch[time_col].is_sorted(), "batch must be sorted by time_col"

    batch = batch.with_row_count("__row_count__")

    event_names = [event for event, predicate in events]
    event_predicates = [predicate for event, predicate in events]
    # event_dependent_columns = {event: [(k.table, k.sql()) for k in sqlglot.parse_one(predicate).find_all(sqlglot.exp.Column) if k.table != ''] 
    #                            for event, predicate in events if predicate is not None}
    event_independent_columns = {event: [k.name for k in sqlglot.parse_one(predicate).find_all(sqlglot.exp.Column) if k.table == event] 
                               for event, predicate in events if predicate is not None}

    event_rename_dicts = {event: {k.sql() : k.table + "_" + k.name for k in sqlglot.parse_one(predicate).find_all(sqlglot.exp.Column)} for event, predicate in events if predicate is not None}
    
    # we need to rename the predicates to use _ instead of . because polars doesn't support . in column names
    event_predicates = [replace_with_dict(predicate, event_rename_dicts[event]) if predicate is not None else None for event, predicate in events]

    rename_dicts = {event: {col: event + "_" + col for col in batch.columns} for event in event_names}
    
    assert all([predicate is not None for predicate in event_predicates[1:]]), \
        "only first event can be None"

    total_len = len(batch)
    total_events = len(events)

    # sequences = polars.from_dicts([{"seq_id": 0, "start_time": batch[time_col][0]}])
    matched_sequences = {i: None for i in range(total_events)}
    matched_sequences[0] = batch[0].rename(rename_dicts[event_names[0]])
    
    first_rows = None
    if events[0][1] is not None:
        first_rows = set(polars.SQLContext().register(event_names[0], batch).execute("select __row_count__ from {} where {}".
                                                                                     format(event_names[0], events[0][1])).collect()["__row_count__"])

    for row in tqdm(range(total_len)):
        current_time = batch[time_col][row]
        # drop all the sequences that are too old, this doesn't have to be done every time at the cost of some useless compute
        if row % 100 == 0:
            for seq_len in range(total_events - 1):
                # you are not going to filter the last matched_sequence, will stores all the matched results!
                if matched_sequences[seq_len] is not None:
                    matched_sequences[seq_len] = matched_sequences[seq_len].filter(polars.col(event_names[seq_len] + "_" + time_col) >= current_time - max_span)

        # now for each partial sequences of length X check if you can append this event
        # TODO: filter this by indepdent predicates
        for seq_len in range(1, total_events):
            predicate = event_predicates[seq_len]
            assert predicate is not None
            for col in event_independent_columns[event_names[seq_len]]:
                predicate = predicate.replace(event_names[seq_len] + "_" + col, str(batch[col][row]))
            predicate = optimizer.simplify.simplify(sqlglot.parse_one(predicate))
            # evaluate the predicate against matched_sequences[seq_len - 1]
            if predicate != sqlglot.exp.FALSE and matched_sequences[seq_len - 1] is not None and len(matched_sequences[seq_len - 1]) > 0:
                # print(len(matched_sequences[seq_len - 1]))                    
                matched = polars.SQLContext(frame=matched_sequences[seq_len - 1]).execute(
                    "select * from frame where {}".format(predicate)).collect()
                # now horizontally concatenate your table against the matched
                if len(matched) > 0:
                    # print(len(matched))
                    matched = matched.hstack(repeat_row(batch[row].rename(rename_dicts[event_names[seq_len]]), len(matched)))
                    if matched_sequences[seq_len] is None:
                        matched_sequences[seq_len] = matched
                    else:
                        if seq_len == 5:
                            print("select * from frame where {}".format(predicate))
                            print(matched.select(["a_close", "b_close", "c_close", "d_close", "e_close", "f_close"]))
                        matched_sequences[seq_len].vstack(matched, in_place=True)
        
        if first_rows is None:
            matched_sequences[0].vstack(batch[row].rename(rename_dicts[event_names[0]]), in_place=True)
        else:
            if row in first_rows:
                matched_sequences[0].vstack(batch[row].rename(rename_dicts[event_names[0]]), in_place=True)
    
    return matched_sequences[total_events - 1]
            

# crimes = polars.read_parquet("../../../sigmod/crimes.parquet").with_row_count().to_arrow()
# executor = CEPExecutor(crimes.column_names, [('a', "primary_category_id = 27"), 
#      ('b', """primary_category_id = 1 and LATITUDE - a.LATITUDE >= -0.025
#     and LATITUDE - a.LATITUDE <= 0.025
#     and LONGITUDE - a.LONGITUDE >= -0.025
#     and LONGITUDE - a.LONGITUDE <= 0.025"""),
#     ('c', """primary_category_id = 24 and LATITUDE - a.LATITUDE >= -0.025
#     and LATITUDE - a.LATITUDE <= 0.025
#     and LONGITUDE - a.LONGITUDE >= -0.025
#     and LONGITUDE - a.LONGITUDE <= 0.025""")], "TIMESTAMP_LONG",  20000000)

# start = time.time()
# results = executor.execute([crimes], 0, 0)
# print("End to end time", time.time() - start)
# print(len(results))

# crimes = polars.read_parquet("../../../sigmod/crimes.parquet")
# nfa_cep(crimes.select(["primary_category_id", "TIMESTAMP_LONG", "LONGITUDE", "LATITUDE"]), [('a', None),
#                   ('b', """b.primary_category_id = 1 and b.LATITUDE - a.LATITUDE >= -0.025
#                     and b.LATITUDE - a.LATITUDE <= 0.025
#                     and b.LONGITUDE - a.LONGITUDE >= -0.025
#                     and b.LONGITUDE - a.LONGITUDE <= 0.025"""),
#                     ('c', """c.primary_category_id = 24 and c.LATITUDE - a.LATITUDE >= -0.025
#                     and c.LATITUDE - a.LATITUDE <= 0.025
#                     and c.LONGITUDE - a.LONGITUDE >= -0.025
#                     and c.LONGITUDE - a.LONGITUDE <= 0.025""")], "TIMESTAMP_LONG",  20000000)

# qqq = polars.read_parquet("../../../sigmod/2021.parquet").with_row_count("row_count").with_columns(polars.col("row_count").cast(polars.Int64()))
# qqq = qqq.with_columns(((polars.col("date").str.strptime(polars.Date).dt.timestamp('ms') // 1000) + polars.col("candle") * 60).cast(polars.UInt64()).alias("timestamp"))
# qqq = qqq.groupby_rolling("row_count", period = "10i", offset = "-5i").agg([
#         polars.col("close").min().alias("min_close"),
#         polars.col("close").max().alias("max_close"),
#     ])\
#     .hstack(qqq.select(["timestamp", "close"]))\
#     .with_columns([(polars.col("close") == polars.col("min_close")).alias("is_local_bottom"),
#                   (polars.col("close") == polars.col("max_close")).alias("is_local_top")])

# ascending_triangles = nfa_cep(
#     qqq.select(["timestamp", "close", "is_local_top", "is_local_bottom"]), [('a', "a.is_local_bottom"), # first bottom 
#      ('b', """b.is_local_top and b.close > a.close * 1.005"""), # first top
#      ('c', """c.is_local_bottom and c.close < b.close * 0.995 and c.close > a.close * 1.005"""), # second bottom, must be higher than first bottom
#      ('d', """d.is_local_top and d.close > c.close * 1.005 and abs(d.close / b.close) < 1.005"""), # second top, must be similar to first top
#      ('e', """e.is_local_bottom and e.close < d.close * 0.995 and e.close > (c.close - a.close) / (c.timestamp - a.timestamp) * (e.timestamp - a.timestamp) + a.close"""), # third bottom, didn't break support
#      ('f', """f.close > d.close * 1.005""") #breakout resistance
#     ], "timestamp", 60 * 120
# )

# heads_and_shoulders = nfa_cep(
#     qqq, [('a', "a.is_local_top"), # first shoulder
#         ('b', """b.is_local_bottom and b.close < a.close * 0.995"""), # first bottom
#         ('c', "c.is_local_top and c.close > a.close * 1.005"), # head
#         ('d', "d.is_local_bottom and d.close < a.close * 0.995"), # second bottom
#         ("e", "e.is_local_top and e.close > d.close * 1.005 and e.close < c.close * 0.995"), # second shoulder
#         ("f", "f.close < ((d.close - b.close) / (d.timestamp - b.timestamp) * (f.timestamp - b.timestamp) + b.close) * 0.995"), # neckline
#     ], "timestamp", 60 * 120
# )
