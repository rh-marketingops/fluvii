import click
from .base import fluvii_cli
from fluvii.kafka_tools import FluviiToolbox
from fluvii.general_utils import parse_headers
import json


@fluvii_cli.group("topics")
def topics_group():
    pass


@topics_group.command(name="list")
@click.option("--include-configs", is_flag=True)
def list_topics(include_configs):
    click.echo(json.dumps(FluviiToolbox().list_topics(include_configs=include_configs), indent=4))


@topics_group.command(name="create")
@click.option("--topic-config-dict", type=str, required=False)
@click.option("--topic-list", type=str, required=False)
@click.option("--config-dict", type=str, required=False)
def create_topics(topic_config_dict, topic_list, config_dict):
    if topic_config_dict:
        topic_config_dict = json.loads(topic_config_dict)
    else:
        if ', ' in topic_list:
            topic_list = topic_list.split(', ')
        else:
            topic_list = topic_list.split(',')

        if config_dict:
            config_dict = json.loads(config_dict)
        else:
            click.echo('There were no configs defined; using defaults of {partitions=3, replication.factor=3}')
            config_dict = {'partitions': 3, 'replication.factor': 3}
        topic_config_dict = {topic: config_dict for topic in topic_list}
    click.echo(f'Creating topics {list(topic_config_dict.keys())}')
    FluviiToolbox().create_topics(topic_config_dict)


@topics_group.command(name="delete")
@click.option("--topic-config-dict", type=str, required=False)
@click.option("--topic-list", type=str, required=False)
def delete_topics(topic_config_dict, topic_list):
    if topic_list:
        if ', ' in topic_list:
            topic_list = topic_list.split(', ')
        else:
            topic_list = topic_list.split(',')
    else:
        topic_list = list(json.loads(topic_config_dict).keys())
    click.echo(f'Deleting topics {topic_list}')
    FluviiToolbox().delete_topics(topic_list)


@topics_group.command(name="consume")
@click.option("--topic-offset-dict", type=str, required=True)
@click.option("--output-filepath", type=click.File("w"), required=True)
def consume_topics(topic_offset_dict, output_filepath):
    def transform(transaction):
        msgs = [{k: msg.__getattribute__(k)() for k in ['key', 'value', 'headers', 'topic', 'partition', 'offset', 'timestamp']} for msg in transaction.messages()]
        for msg in msgs:
            msg['headers'] = parse_headers(msg['headers'])
        return msgs
    topic_offset_dict = json.loads(topic_offset_dict)
    messages = FluviiToolbox().consume_messages(topic_offset_dict, transform)
    click.echo('Messages finished consuming, now outputting to file...')
    json.dump(messages, output_filepath, indent=4, sort_keys=True)


@topics_group.command(name="produce")
@click.option("--topic-schema-dict", type=str, required=True)
@click.option("--input-filepath", type=click.File("r"), required=True)
@click.option("--topic-override", type=str, required=False)
@click.option("--use-given-partitions", is_flag=True)
def produce_message(topic_schema_dict, input_filepath, topic_override, use_given_partitions):
    FluviiToolbox().produce_messages(
        json.loads(topic_schema_dict),
        json.loads(input_filepath.read()),
        topic_override=topic_override,
        use_given_partitions=use_given_partitions
    )
