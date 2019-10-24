import csv
from csv import DictReader

from datetime import datetime

import json
from clickhouse_driver import Client

from lakeweed import clickhouse as j2r


def iter_csv(filename):
    converters = {
        'qty': int,
        'time': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    }

    with open(filename, 'r') as f:
        reader = DictReader(f)
        for line in reader:
            kv = {k: v for (k, v) in line.items() if not k.startswith("__")}
            (types, values) = j2r.json2type_value(json.dumps(kv))

            yield values


# ---------------------------------------------------------------------

def query_create_data_table(column_types_map, data_table_name):
    columns_def_string = ", ".join(["\"{}\" {}".format(c, t) for c, t in column_types_map.items()])
    return "CREATE TABLE IF NOT EXISTS {}({}, __create_at DateTime DEFAULT now(), __collected_at DateTime, __uid UUID DEFAULT generateUUIDv4()) ENGINE = MergeTree PARTITION BY toYYYYMM(__create_at) ORDER BY(__create_at)".format(data_table_name, columns_def_string)


def query_insert_data_table_without_value(column_names, data_table_name):
    columns_str = ", ".join(['"' + c + '"' for c in column_names])
    # TODO Need to remove startswith" __"
    return "INSERT INTO {} ({}) VALUES".format(data_table_name, columns_str)

 # ---------------------------------------------------------------------


def create_data_table(client, types, new_table_name):
    query = query_create_data_table(types, new_table_name)
    client.execute(query)


def insert_data(client, data_table_name, values):
    query = query_insert_data_table_without_value(values.keys(), data_table_name)
    client.execute(query, [values])

 # ---------------------------------------------------------------------


types = None
Values = None

with open('sample.csv', 'r') as f:
    reader = DictReader(f, quoting=csv.QUOTE_NONNUMERIC)
    for line in reader:
        # print(line)
        kv = {k: v for (k, v) in line.items() if not k.startswith("__")}
        print(kv)

        (types, values) = j2r.json2type_value(json.dumps(kv))

        break

print(types)
print("----------------------------------")
print(values)

client = Client('localhost')

data_table_name = "csv_test"
create_data_table(client, types, data_table_name)
insert_data(client, data_table_name, values)

client.execute(f"INSERT INTO {data_table_name} VALUES", iter_csv('sample.csv'))

# client.execute(
#     'CREATE TABLE IF NOT EXISTS data_csv '
#     '('
#     'time DateTime, '
#     'order String, '
#     'qty Int32'
#     ') Engine = Memory'
# )
# []
# client.execute('INSERT INTO data_csv VALUES', iter_csv('/tmp/data.csv'))
