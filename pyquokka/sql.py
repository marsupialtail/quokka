import polars
import duckdb
import pyquokka
import json
from collections import deque

from pyquokka.df import * 

def get_new_var_name(var_names):
    return 'v_'+str(len(var_names)+1)

def get_new_col_name(schema):
    i = 0
    while 'col_'+str(i) in schema:
        i += 1
    return 'col_' + str(i)

def rewrite_aliases(cols, aliases):
    for alias in aliases:
        cols = cols.replace(alias, aliases[alias])
    return cols

def is_agg(col):
    for agg_name in supported_aggs:
        if agg_name + '(' in col: 
            return True
    return False

# Given a list of columns, give an alias to each column 
# Update aliases with #index->alias, formula->alias
def name_aliases(cols, aliases, schema, indices=None):
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

def emit_code(node, var_names, var_values, aliases, no_exec=True):
    """
    Args:
        var_names: id number -> name
        var_values: name -> value
        aliases: 
            #int -> name ('#3' -> 'n_nationkey'),
            formula -> name ('l_partkey + 1' -> 'col1')
            'count_star()' -> 'count(*)'
    Returns:
        values
    """
    var_name = get_new_var_name(var_values.keys())
    code = ""
    result = None
    
    print('#' + node['name'])
    if node['name'] == 'PARQUET_SCAN' or node['name'] == 'PROJECTION':
        if node['name'] == 'PARQUET_SCAN':
            extra_info = node['extra_info'].split('[INFOSEPARATOR]')
            cols = rewrite_aliases(extra_info[0].strip(), aliases).split('\n')

            ####### TEMPORARY FOR TPC-H #######
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
        if node['name'] == 'PARQUET_SCAN':
            if len(extra_info) >= 2 and 'Filters' in extra_info[1]:
                filters = extra_info[1].strip()
                if 'EC' in extra_info[1]: # if there's no infoseparator
                    filters = filters.split('EC')[0].strip()
                filters = filters.split('\n')
                filters[0] = filters[0].split('Filters: ')[1]
                for f in filters:
                    if no_exec:
                        filter_stmt = var_name + ' = ' + var_name + '.filter_sql("' + f + '")'
                        code += filter_stmt + '\n'
                    # there are some formatting issues for filter where 'r_name = ASIA', etc. won't work, so we'll comment this out for now as filtering doesn't change the schema
                    if not no_exec: result = result.filter_sql(f)
        if no_exec and len(selected_columns) > 0:
            select_stmt = var_name + ' = ' + var_name + '.select(' + str(selected_columns) + ')'
            code += select_stmt + '\n'
        result = result.select(selected_columns)
        
        # Parquet scan node won't introduce new columns
        if node['name'] == 'PROJECTION':
            if len(with_columns) > 0:
                if no_exec:
                    with_col_stmt = var_name + ' = ' + var_name + '.with_columns_sql("' + ','.join(with_columns) + '")'
                    code += with_col_stmt + '\n'
                result = result.with_columns_sql(','.join(with_columns))
                
                # If we are only creating new columns, need to make sure we throw away other ones
                if no_exec and len(selected_columns) == 0:
                    select_stmt = var_name + ' = ' + var_name + '.select(' + str(col_aliases) + ')'
                    code += select_stmt
                result = result.select(col_aliases)
                
    elif node['name'] == 'FILTER':
        filters = node['extra_info'].split('\n')
        child_name = var_names[node['children'][0]['id']]
        child = var_values[child_name]
        result = child
        
        first_filter = True  # for whether to use child_name or var_name in assignment
        for f in filters:
            if no_exec:
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
        left_key = extra_info[1].split(' = ')[0]
        right_key = extra_info[1].split(' = ')[1]
        
        left_table_name = var_names[node['children'][0]['id']]
        right_table_name = var_names[node['children'][1]['id']]
        
        left_table = var_values[left_table_name]
        right_table = var_values[right_table_name]
        
        suffix = '_' + str(node['id'])
        # Need to add back left key to table in case it is referenced later in query
        alias_left_key = left_key + ' as ' + right_key
        if no_exec:
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
                if no_exec:
                    with_col_stmt = var_name + ' = ' + var_name + '.with_columns_sql("' + ','.join(with_columns) + '")'
                    code += with_col_stmt + '\n'
                result = result.with_columns_sql(','.join(with_columns))
                
            # Quokka GroupedDatastream does not have schema attribute, so do this before calling the groupby
            aggregates, _ = name_aliases(aggregates, aliases, result.schema)
            if len(group) > 0:
                if no_exec:
                    groupby_stmt = var_name + ' = ' + var_name + '.groupby(' + str(group) + ')'
                    code += groupby_stmt + '\n'
                result = result.groupby(group)
        else:
            # All columns should be aggregates in an ungrouped aggregate
            aggregates = rewrite_aliases(node['extra_info'].strip(), aliases).split('\n')
            aggregates, _ = name_aliases(aggregates, aliases, result.schema)

        if no_exec:
            agg_stmt = var_name + ' = ' + var_name + '.agg_sql("' + ','.join(aggregates) + '")'
            code += agg_stmt
    else:
        code += '# not supported yet'

    if no_exec: print(code + '\n')
    var_names[node['id']] = var_name
    var_values[var_name] = result

def generate_code(json_plan, tables, qc):
    """
        json_plan (string): name of json file with DuckDB plan
        tables (dict): looks like
           {
                "lineitem": lineitem,
                "orders": orders,
                "customer": customer,
                "part": part,
                "supplier": supplier,
                "partsupp": partsupp,
                "nation": nation,
                "region": region
            }
          where table name is mapped to Quokka datastream
       qc (QuokkaContext)
    """
    
    with open(json_plan) as f:
        obj = json.load(f)
    plan = obj['children'][0]['children']

    #Reverse topologically sort
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
        emit_code(node, var_names, var_values, aliases)