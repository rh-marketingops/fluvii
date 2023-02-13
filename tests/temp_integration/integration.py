from os import environ, path
from sys import argv

# a lazy way of setting the .env to a desired path, most likely "tests/temp_integration/integration_vars.env"
# if executing from fluvii root dir
if len(argv) > 1:
    environ['FLUVII_CONFIG_DOTENV'] = argv[1]

import json
from shutil import rmtree
from fluvii import FluviiToolbox
from multiprocessing import Process
from time import sleep
from fluvii import FluviiTableAppFactory
from fluvii.components.sqlite import SqliteFluvii, SqliteConfig

my_schema = {
    "name": "CoolSchema",
    "type": "record",
    "fields": [
        {"name": "my_data_dict", "type": {"type": "map", "values": "string"}, "default": {}}
    ]
}

test_unique_partition_keys = 3
test_partition_key_repeats = 1
partition_count = 3
total_records = test_unique_partition_keys * test_partition_key_repeats * partition_count

topics = {
    'fluvii_testing_input': {'partitions': partition_count, 'replication.factor': 3},
    'fluvii_testing_output': {'partitions': partition_count, 'replication.factor': 3},
    'fluvii_testing_app__changelog': {'partitions': partition_count, 'replication.factor': 3, 'cleanup.policy': 'compact'}
}


def blank_value():
    return {"my_data_dict": {}}


def get_data_set(key_name='test--key', unique_partition_keys=test_unique_partition_keys, partition_key_repeats=test_partition_key_repeats, partitions=partition_count):
    msgs = []
    keyset = [f'{key_name}_{i}' for i in range(unique_partition_keys)]
    for r in range(partition_key_repeats):
        for p in range(partitions):
            for k in keyset:
                key_out = f'{k}-p{p}'
                value_out = blank_value()
                value_out['my_data_dict'] = {key_out: f'{key_out}-r{r}'}
                msgs.append({"key": key_out, "value": value_out, "partition": p, "topic": "fluvii_testing_input"})
    return msgs


def table_app_func(transaction):
    old_data = transaction.read_table_entry()
    if not old_data:
        old_data = ''
        iter_count = 0
    else:
        iter_count = len(old_data.split(','))
    data_out = {k: v+f'-t{iter_count}' for k, v in transaction.value()['my_data_dict'].items()}
    new_entry = list(data_out.values())[0]
    if old_data:
        new_entry = old_data + ',' + new_entry
    transaction.update_table_entry(new_entry)
    transaction.produce({"value": data_out, "partition": transaction.partition()})


def delete_tables():
    if path.exists(SqliteConfig().table_directory):
        print('deleting table data')
        rmtree(SqliteConfig().table_directory)


def consume_transform(transaction):
    return [{'key': msg.key(), 'value': msg.value()} for msg in transaction.messages()]


def setup(toolbox):
    teardown(toolbox)
    toolbox.create_topics(topics)
    toolbox.produce_messages({'fluvii_testing_input': my_schema}, get_data_set(), use_given_partitions=True)


def run_test_app():
    app = FluviiTableAppFactory(
        app_function=table_app_func,
        consume_topics_list=['fluvii_testing_input'],
        produce_topic_schema_dict={'fluvii_testing_output': my_schema},
    )
    app_thread = Process(target=app.run)
    app_thread.start()
    sleep(30)
    app_thread.terminate()


def validate(toolbox):
    messages = toolbox.consume_messages(consume_topics_dict={'fluvii_testing_output': {}},
                                        transform_function=consume_transform)

    success = True
    if len(messages) != total_records:
        print(len(messages))
        success = False
        print('Messages missing from downstream topic')
    for t in [f'p{n}' for n in range(3)]:
        table = SqliteFluvii(t, SqliteConfig())
        table_partition_data = [json.loads(v) for k, v in table.db.items() if 'test--key' in k]
        if len(table_partition_data) != test_unique_partition_keys:
            print(f'Table {t} has missing keys')
            success = False
        missing_change_counts = [v for v in table_partition_data if len(v.split(',')) != test_partition_key_repeats]
        if missing_change_counts:
            print(f'Table {t} is missing tables record changes')
            success = False
        table.close()

    if success:
        print('YAY IT WORKS')
    else:
        print('Uh oh, it is broken!')


def teardown(toolbox):
    toolbox.delete_topics([k for k in topics])
    delete_tables()


fluvii_toolbox = FluviiToolbox()

setup(fluvii_toolbox)
run_test_app()
validate(fluvii_toolbox)
teardown(fluvii_toolbox)
