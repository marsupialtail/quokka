from pyquokka.sql import generate_code
mode = 'DISK'
format_ = 'parquet'

query_no = 1
query_path = '' + str(query_no) + '.sql'
with open(query_path, 'r') as f:
    query = f.read()
data_path=""

generate_code(query, data_path, table_prefixes = {
        'l': 'lineitem',
        'o': 'orders',
        'c': 'customer',
        'p': 'part',
        'ps': 'partsupp',
        's': 'supplier',
        'r': 'region',
        'n': 'nation'
        }, mode=mode, format_=format_)
