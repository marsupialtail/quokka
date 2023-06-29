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
        for event in events:
            event_prefilter = sqlglot.exp.TRUE
            event_dependent_filter = sqlglot.exp.TRUE
            event_name, event_filter = event
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
                        if event == events[-1]:
                            last_event_dependent_on_first_event_filter = sqlglot.exp.and_(
                                last_event_dependent_on_first_event_filter, conjunct)
                    
            event_prefilter = optimizer.simplify.simplify(event_prefilter)
            self.event_prefilters[event_name] = event_prefilter.sql()
            self.event_dependent_filters[event_name] = event_dependent_filter.sql()
            self.prefilter = sqlglot.exp.or_(self.prefilter, event_prefilter)
            seen_events.add(event_name)
        
        self.prefilter = optimizer.simplify.simplify(self.prefilter).sql()
        self.last_event_dependent_on_first_event_filter = optimizer.simplify.simplify(last_event_dependent_on_first_event_filter).sql()
        print(self.prefilter)
        print(self.touched_columns)

    def execute(self,batches,stream_id, executor_id):
        df = polars.concat(pa.concat_tables(batches))
        assert df[self.time_col].is_sorted, "data must be presorted on time column"
        con = duckdb.connect("PRAGMA threads=8;")
        # this is no longer necessary since we will push down the prefilter.
        # df = con.execute("select * from df where {} order by {}".format(self.prefilter, self.time_col)).arrow()
        df = df.with_row_count(self.row_count_col).to_arrow()

        first_event_name, first_event_filter = self.events[0]
        last_event_name, last_event_filter = self.events[-1]
        # start = time.time()
        first_event = con.execute("select * from df where {}".format(first_event_filter)).arrow()
        last_event = con.execute("select * from df where {}".format(self.event_prefilters[last_event_name])).arrow()

        processed_last_event_filter = replace_qualifier(add_qualifier(self.last_event_dependent_on_first_event_filter, "last_event"), first_event_name, "first_event")
                   
        range_join = """select first_event.{} as __ats__, last_event.{} as __cts__
        from first_event, last_event
        where first_event.{} <= last_event.{} and last_event.{} <= first_event.{} + {}
        and {} """.format(self.row_count_col, self.row_count_col, self.time_col, self.time_col, 
                        self.time_col, self.time_col, self.max_span, processed_last_event_filter)
        # start = time.time()
        result = con.execute(range_join).arrow()
        # print(time.time() - start)
                
        df = polars.from_arrow(df)
        matched_ends = []
        total_exec_times = []
        total_section_lengths = []
        
        overhead = 0
            
        # startt2 = time.time()
        for bound in polars.from_arrow(result).iter_rows(named = True):

            start_nr = bound["__ats__"]
            end_nr = bound["__cts__"]
            # print(start_nr)
            # match recognize default is one pattern per start. we can change it to one pattern per end too
            # if end_nr in matched_ends:
            if start_nr in matched_ends or end_nr <= start_nr:
                continue
            
            # start3 = time.time()
            my_section = df[start_nr: end_nr + 1] #.to_arrow()
            fate = {first_event_name + "." + col : my_section[col][0] for col in my_section.columns}
            # overhead += time.time() - start3
            stack = deque([(0, my_section[0], [fate])])
            
            while stack:
                marker, vertex, path = stack.pop()
            
                remaining_df = my_section[marker:]# .to_arrow()
                next_event_name, next_event_filter = self.events[len(path)]
                
                # now fill in the next_event_filter based on the previous path
                for fate in path:
                    for fixed_col in fate:
                        next_event_filter = next_event_filter.replace(fixed_col, str(fate[fixed_col]))
                

                # startt = time.time()
                matched = polars.SQLContext(frame=remaining_df).execute("select __row_nr__ from frame where {}".format(next_event_filter)).collect()
                # total_exec_times.append(time.time() - startt)
                total_section_lengths.append(len(remaining_df))
                
                if len(matched) > 0:
                    if next_event_name == last_event_name:
                        matched_ends.append(start_nr)
                        break
                    else:
                        for matched_row in matched.iter_rows(named = True):
                            my_fate = {next_event_name + "." + col : matched_row[col] for col in matched_row}
                            # the row count of the matched row in the section is it's row count col value minus
                            # the start row count of the section
                            stack.append((matched_row[self.row_count_col] - start_nr, matched_row, path + [my_fate]))
        
        # print("Total loop time", time.time() - startt2)
        # print(total_section_lengths)
        # print(sum(total_section_lengths))
        # print("Total filtering time in loop", sum(total_exec_times))
        
        # print("overhead", overhead)
        return matched_ends

    def done(self,executor_id):
        raise NotImplementedError
    
    