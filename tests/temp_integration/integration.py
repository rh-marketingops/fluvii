import json
from fluvii.kafka_tools.fluvii_toolbox import FluviiToolbox
from multiprocessing import Process
from time import sleep
from fluvii.fluvii_app import FluviiTableAppFactory

my_schema = {
    "name": "CoolSchema",
    "type": "record",
    "fields": [
        {"name": "my_data_dict", "type": {"type": "map", "values": "string"}, "default": {}}
    ]
}

partition_count = 3


def blank_value():
    return {"my_data_dict": {}}


def get_data_set(key_name='test--key', unique_partition_keys=3, partition_key_repeats=1, partitions=partition_count):
    messages = []
    keyset = [f'{key_name}_{i}' for i in range(unique_partition_keys)]
    for r in range(partition_key_repeats):
        for p in range(partitions):
            for k in keyset:
                key_out = f'{k}-p{p}'
                value_out = blank_value()
                value_out['my_data_dict'] = {key_out: f'{key_out}-r{r}'}
                messages.append({"key": key_out, "value": value_out, "partition": p, "topic": "fluvii_testing_input"})
    return messages


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


def consume_transform(transaction):
    return [{'key': msg.key(), 'value': msg.value()} for msg in transaction.messages()]


app = FluviiTableAppFactory(
    app_function=table_app_func,
    consume_topics_list=['fluvii_testing_input'],
    produce_topic_schema_dict={'fluvii_testing_output': my_schema},
)


# toolbox = FluviiToolbox()
# toolbox.create_topics({
#     'fluvii_testing_input': {'partitions': partition_count, 'replication.factor': 3},
#     'fluvii_testing_output': {'partitions': partition_count, 'replication.factor': 3},
#     'fluvii_testing_app__changelog': {'partitions': partition_count, 'replication.factor': 3, 'cleanup.policy': 'compact'}
# })
# toolbox.produce_messages({'fluvii_testing_input': my_schema}, get_data_set())
#
app_thread = Process(target=app.run)
app_thread.start()
sleep(60)
app_thread.terminate()
#

# msgs = toolbox.consume_messages(consume_topics_dict={'fluvii_testing_input': {}}, transform_function=consume_transform)
# with open('/tmp/fluvii_integration.json', 'w') as f:
#     json.dump(msgs, f)
