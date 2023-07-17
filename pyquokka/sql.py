import polars
import duckdb
import pyquokka
import json
from collections import deque
import re # for fixing filter formatting

from pyquokka.df import * 

supported_aggs = ['count', 'sum', 'avg', 'min', 'max'] 

date_regex = '[0-9]{4}-[0-9]{2}-[0-9]{2}'
def format_date(matchobj):
    """
    DuckDB generated plans sometimes include filters with incorrect formatting that aren't valid SQL, ex. n_nation=BRAZIL or l_shipdate<1994-01-01. This function is a helper function for reformatting these filters, in particular replacing a substring of the format ####-##-## (matching the date_regex) with date '####-##-##'.
    """
    return "date '" + matchobj.group(0) + "'"

def get_new_var_name(var_names):
    """
    Args:
        var_names: variable names that have already been used.

    During code generation each node in the plan is assigned a variable whose value will hold the result of computing the query up to that node. This function should return a new variable name that differs from all of the previously used names. Currently it is hardcoded to return v_1, v_2, etc., but in the future can be modified to generate a more descriptive name based on node type.
    """
    return 'v_'+str(len(var_names)+1)

def get_new_col_name(schema):
    i = 0
    while 'col_'+str(i) in schema:
        i += 1
    return 'col_' + str(i)

def rewrite_aliases(cols, aliases):
    """
    Args:
        cols (string): list of columns
        aliases (dict): mapping of column aliases
    """
    for alias in aliases:
        cols = cols.replace(alias, aliases[alias])
    return cols

def is_agg(col):
    for agg_name in supported_aggs:
        if agg_name + '(' in col: 
            return True
    return False

def name_aliases(cols, aliases, schema, indices=None):
    """
    Args:
        cols (list): list of columns
        aliases (dict): dict to update with alias mappings
        schema (string)
        indices (list): Pass this argument in when we want to update numbered ordering of columns (i.e. when computing a projection or scan node).

    Given a list of columns, give an alias to each column. Update aliases with #index->alias, formula->alias. Used for adding new columns and computing aggregations.
    """
    # As long as we update the Datastream object after this function call, changes to given schema don't need to persist
    new_schema = schema
    col_aliases = []
    for i in range(len(cols)):
        new_col_name = get_new_col_name(new_schema)
        new_schema.append(new_col_name)
        cols[i] = cols[i] + ' as ' + new_col_name
        aliases[cols[i]] = new_col_name
        col_aliases.append(new_col_name)
        if indices:
            indexed_name = '#' + str(indices[i])
            aliases[indexed_name] = new_col_name
    return cols, col_aliases

def emit_code(node, var_names, var_values, aliases, tables, table_prefixes, qc, print_code=True):
    """
    This function emits code to compute a single node.

    Args:
        var_names (dict): node id number -> name of variable
        var_values (dict): name of variable -> value
        aliases (dict): string->string
            #int -> name ('#3' -> 'n_nationkey'),
            formula -> name (e.g. 'l_partkey + 1' -> 'col1'),
            'count_star()' -> 'count(*)'

            The first mapping is needed because DuckDB plan generation sometimes represents columns via a numbered ordering in projection nodes, but since this makes the plan difficult to read and parse (and may lead to producing an incorrect result) we plan to try to get rid of this behavior in the future. For now, we update this mapping in projection and scan nodes.

        tables (dict): mapping of table name (string) to Quokka Datastream
        table_prefixes (dict): prefix of columns that identifies which table they belong to, e.g.
       {
        'l': 'lineitem',
        'o': 'orders',
        'c': 'customer',
        'p': 'part',
        'ps': 'partsupp',
        's': 'supplier',
        'r': 'region',
        'n': 'nation'
        }

        This is needed because DuckDB plans currently don't include the table name in the READ_PARQUET nodes. Clearly this fix may not extend to other schemas besides TPC-H, so we will find a better solution in the future. 
        qc: QuokkaContext
        print_code: print generated code if this flag is set, otherwise don't print anything. 
    Returns:
        values
    """
    var_name = get_new_var_name(var_values.keys())
    code = ""
    result = None
    
    if print_code: 
        # Label each code block with the node they correspond to
        print('#' + node['name'])
        
    # Parquet scan nodes have different names in the json depending on DuckDB version
    if node['name'] in ['PARQUET_SCAN', 'READ_PARQUET '] or node['name'] == 'PROJECTION':
        if node['name'] in ['PARQUET_SCAN', 'READ_PARQUET ']:
            extra_info = node['extra_info'].split('[INFOSEPARATOR]')
            cols = rewrite_aliases(extra_info[0].strip(), aliases).split('\n')

            ####### TEMPORARY FOR TPC-H #######
            # Since DuckDB plans currently don't include the table name in scan nodes
            table = table_prefixes[cols[0].split('_')[0]]
            ###################################
            child = tables[table]
            child_name = table
            
        else:
            extra_info = node['extra_info'].split('[INFOSEPARATOR]')
            cols = rewrite_aliases(extra_info[0].strip(), aliases).split('\n')
            child_name = var_names[node['children'][0]['id']]
            child = var_values[child_name]
        
        code += var_name + ' = ' + child_name + '\n'
        result = child
        with_columns = []
        with_column_indices = []
        selected_columns = []
        index = 0
        schema = child.schema
        for i in range(len(cols)):
            c = cols[i]
            indexed_name = '#' + str(index)
            if c in child.schema:
                selected_columns.append(c)
                aliases[indexed_name] = c
            else: 
                with_column_indices.append(index)
                with_columns.append(c)
            index += 1
        with_columns, col_aliases = name_aliases(with_columns, aliases, child.schema, with_column_indices)
        
        # Filters may depend on columns not in the select, so filter first
        if node['name'] in ['PARQUET_SCAN', 'READ_PARQUET ']:
            if len(extra_info) >= 2 and 'Filters' in extra_info[1]:
                filters = extra_info[1].strip()
                filters = re.sub(date_regex, format_date, filters) #reformat dates in filters
                if 'EC' in extra_info[1]: # if there's no infoseparator
                    filters = filters.split('EC')[0].strip()
                filters = filters.split('\n')
                filters[0] = filters[0].split('Filters: ')[1]
                for f in filters:
                    if print_code:
                        filter_stmt = var_name + ' = ' + var_name + '.filter_sql("' + f + '")'
                        code += filter_stmt + '\n'
                    # (TODO) there are some formatting issues for filter where 'r_name = ASIA', etc. won't work, so we can comment this out for now. Because of this generated code of queries involving filters may not run (but can be easily manually fixed). We will fix the formatting in the future.
                    if not print_code: result = result.filter_sql(f)
        if print_code and len(selected_columns) > 0:
            select_stmt = var_name + ' = ' + var_name + '.select(' + str(selected_columns) + ')'
            code += select_stmt + '\n'
        result = result.select(selected_columns)
        
        # Parquet scan node won't introduce new columns
        if node['name'] == 'PROJECTION':
            if len(with_columns) > 0:
                if print_code:
                    with_col_stmt = var_name + ' = ' + var_name + '.with_columns_sql("' + ','.join(with_columns) + '")'
                    code += with_col_stmt + '\n'
                result = result.with_columns_sql(','.join(with_columns))
                
                # If we are only creating new columns, need to make sure we throw away other ones
                if print_code and len(selected_columns) == 0:
                    select_stmt = var_name + ' = ' + var_name + '.select(' + str(col_aliases) + ')'
                    code += select_stmt
                result = result.select(col_aliases)
                
    elif node['name'] == 'FILTER':
        filters = node['extra_info'].split('[INFOSEPARATOR]')[0].strip().split('\n')

        filters = re.sub(date_regex, format_date, filters) #reformat dates in filters
        child_name = var_names[node['children'][0]['id']]
        child = var_values[child_name]
        result = child
        
        first_filter = True  # for whether to use child_name or var_name in assignment
        for f in filters:
            if print_code:
                if first_filter:
                    filter_stmt = var_name + ' = ' + child_name + '.filter_sql("' + f + '")'
                else:
                    filter_stmt = var_name + ' = ' + var_name + '.filter_sql("' + f + '")'
                code += filter_stmt + '\n'
            result = result.filter_sql(f)
            first_filter = False
        
    elif node['name'] == 'HASH_JOIN':
        extra_info = node['extra_info'].split('\n')
        how = extra_info[0].lower()
        # TODO Only allow inner joins for now, there's a bug with semi joins in the plan
        assert how in ['inner'], how+ ' joins not supported'
        
        left_key = extra_info[1].split(' = ')[0]
        right_key = extra_info[1].split(' = ')[1]
        
        left_table_name = var_names[node['children'][0]['id']]
        right_table_name = var_names[node['children'][1]['id']]
        
        left_table = var_values[left_table_name]
        right_table = var_values[right_table_name]
        
        suffix = '_' + str(node['id'])
        # Need to add back left key to table in case it is referenced later in query
        alias_left_key = left_key + ' as ' + right_key
        if print_code:
            join_stmt = var_name + ' = ' + left_table_name + '.join(' + right_table_name \
                    + ', left_on = "' + left_key + '", right_on = "'  + right_key \
                    + ', suffix = ' + '"' + suffix + '"' + '", how = "' + how + '")'
            code += join_stmt + '\n'
            with_column_stmt = var_name + ' = ' + var_name + '.with_columns_sql("' + alias_left_key + '")'
            code += with_column_stmt
        result = left_table.join(right_table, left_on = left_key, right_on = right_key, suffix = suffix, how = how)
        result = result.with_columns_sql(alias_left_key)
        
    elif node['name'] == 'UNGROUPED_AGGREGATE' or node['name'] == 'HASH_GROUP_BY':
        child_name = var_names[node['children'][0]['id']]
        child = var_values[child_name]
        result = child
        
        code += var_name + ' = ' + child_name + '\n'
        if node['name'] == 'HASH_GROUP_BY':
            extra_info = node['extra_info'].split('[INFOSEPARATOR]')
            cols = rewrite_aliases(extra_info[0].strip(), aliases).split('\n')
            group = []
            with_columns = []
            aggregates = []
            for c in cols:
                if c in child.schema:
                    group.append(c)
                elif is_agg(c):
                    aggregates.append(c)
                else:
                    with_columns.append(c)
            if len(with_columns) > 0:
                with_columns, _ = name_aliases(with_columns, aliases, result.schema)
                if print_code:
                    with_col_stmt = var_name + ' = ' + var_name + '.with_columns_sql("' + ','.join(with_columns) + '")'
                    code += with_col_stmt + '\n'
                result = result.with_columns_sql(','.join(with_columns))
                
            # Quokka GroupedDatastream does not have schema attribute, so do this before calling the groupby
            aggregates, _ = name_aliases(aggregates, aliases, result.schema)
            if len(group) > 0:
                if print_code:
                    groupby_stmt = var_name + ' = ' + var_name + '.groupby(' + str(group) + ')'
                    code += groupby_stmt + '\n'
                result = result.groupby(group)
        else:
            # All columns should be aggregates in an ungrouped aggregate
            aggregates = rewrite_aliases(node['extra_info'].strip(), aliases).split('\n')
            aggregates, _ = name_aliases(aggregates, aliases, result.schema)

        if print_code:
            agg_stmt = var_name + ' = ' + var_name + '.agg_sql("' + ','.join(aggregates) + '")'
            code += agg_stmt
    else:
        code += '# not supported yet'

    if print_code: print(code + '\n')
    var_names[node['id']] = var_name
    var_values[var_name] = result
    
def generate_code_from_plan(plan, tables, qc, table_prefixes = {
        'l': 'lineitem',
        'o': 'orders',
        'c': 'customer',
        'p': 'part',
        'ps': 'partsupp',
        's': 'supplier',
        'r': 'region',
        'n': 'nation'
        }):
    
    #Reverse topologically sort and assign ids to nodes
    node = plan[0]
    nodes = deque([node])
    i = 0
    node['id'] = i
    reverse_sorted_nodes = [node]
    while len(nodes) > 0:
        new_node = nodes.popleft()
        for child in new_node['children']:
            i += 1
            child['id'] = i
            reverse_sorted_nodes.append(child)
            nodes.append(child)
    reverse_sorted_nodes = reverse_sorted_nodes[::-1]
    var_names = {}
    var_values = {}
    aliases = {'count_star()': 'count(*)'}
    
    for node in reverse_sorted_nodes:
        emit_code(node, var_names, var_values, aliases, tables, table_prefixes, qc)
                            
def generate_code(query, data_path, table_prefixes = {
        'l': 'lineitem',
        'o': 'orders',
        'c': 'customer',
        'p': 'part',
        'ps': 'partsupp',
        's': 'supplier',
        'r': 'region',
        'n': 'nation'
        }, mode='DISK', format_="parquet"):
    """
    Args:
       query (string): String representing SQL query
       data_path (string): path to the disk storage location, e.g. "/home/ziheng/tpc-h/"
       table_prefixes (dict): prefix of columns that identifies which table they belong to, e.g.
       {
        'l': 'lineitem',
        'o': 'orders',
        'c': 'customer',
        'p': 'part',
        'ps': 'partsupp',
        's': 'supplier',
        'r': 'region',
        'n': 'nation'
        }
        This is needed because DuckDB plans currently don't include the table name in the READ_PARQUET nodes. Clearly this fix may not extend to other schemas besides TPC-H, so we will find a better solution in the future.
        mode (string): Only 'DISK' is allowed for now. Support for 'S3' will be added soon.
        format_ (string): The format of the data, 'csv' or 'parquet'. The default value is 'parquet'.
    """
    if mode == "DISK":
        cluster = LocalCluster()
    else:
        raise Exception

    qc = QuokkaContext(cluster,2,2)
    qc.set_config("fault_tolerance", True)

    if mode == "DISK":
        disk_path = data_path
        if format_ == "csv":
            lineitem = qc.read_csv(disk_path + "lineitem.tbl", sep="|", has_header=True)
            orders = qc.read_csv(disk_path + "orders.tbl", sep="|", has_header=True)
            customer = qc.read_csv(disk_path + "customer.tbl",sep = "|", has_header=True)
            part = qc.read_csv(disk_path + "part.tbl", sep = "|", has_header=True)
            supplier = qc.read_csv(disk_path + "supplier.tbl", sep = "|", has_header=True)
            partsupp = qc.read_csv(disk_path + "partsupp.tbl", sep = "|", has_header=True)
            nation = qc.read_csv(disk_path + "nation.tbl", sep = "|", has_header=True)
            region = qc.read_csv(disk_path + "region.tbl", sep = "|", has_header=True)
        elif format_ == "parquet":
            lineitem = qc.read_parquet(disk_path + "lineitem.parquet")
            orders = qc.read_parquet(disk_path + "orders.parquet")
            customer = qc.read_parquet(disk_path + "customer.parquet")
            part = qc.read_parquet(disk_path + "part.parquet")
            supplier = qc.read_parquet(disk_path + "supplier.parquet")
            partsupp = qc.read_parquet(disk_path + "partsupp.parquet")
            nation = qc.read_parquet(disk_path + "nation.parquet")
            region = qc.read_parquet(disk_path + "region.parquet")
        else:
            raise Exception
    else:
        raise Exception
        
    tables = {
        "lineitem": lineitem,
        "orders": orders,
        "customer": customer,
        "part": part,
        "supplier": supplier,
        "partsupp": partsupp,
        "nation": nation,
        "region": region
    }
    
    # Need this argument to know which parquet files to read
    TABLES = tables.keys()
    # Table files should be in the form [DATA_PATH][table name].parquet
    DATA_PATH = data_path

    print("Start plan generation")
    con = duckdb.connect()
    con.execute("SET explain_output='ALL';")

    CREATE_SCRIPT = '\n'.join(['CREATE VIEW ' + table + " AS SELECT * FROM read_"+format_+"('" + DATA_PATH + table + "."+format_+"');" for table in TABLES])
    con.execute(CREATE_SCRIPT)
    con.execute("PRAGMA explain_output = 'OPTIMIZED_ONLY'; PRAGMA enable_profiling = json; ")
    result = con.execute('explain analyze ' + query).fetchall()[0][1]
    obj = json.loads(result)

    plan = obj['children'][0]['children']
    
    print("Finished plan generation, beginning computation")
    
    generate_code_from_plan(plan, tables, qc, table_prefixes)
