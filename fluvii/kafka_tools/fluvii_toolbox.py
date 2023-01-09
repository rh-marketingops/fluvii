from fluvii.general_utils import Admin
from fluvii.producer import Producer
from fluvii.fluvii_app import FluviiConfig
from fluvii.auth import SaslPlainClientConfig
from fluvii.schema_registry import SchemaRegistry
from confluent_kafka.admin import NewTopic, ConfigResource
from fluvii.kafka_tools.topic_dumper import TopicDumperApp
import logging


LOGGER = logging.getLogger(__name__)


class FluviiToolbox:
    """
    Helpful functions for interacting with Kafka.
    """

    def __init__(self, fluvii_config=None):
        if not fluvii_config:
            fluvii_config = FluviiConfig()
        self._config = fluvii_config
        admin_auth = self._config.client_auth_config
        if self._config.client_auth_config:
            admin_auth = SaslPlainClientConfig(username=admin_auth.username, password=admin_auth.password)
        self.admin = Admin(self._config.client_urls, admin_auth)

    def list_topics(self, valid_only=True, include_configs=False):
        def _valid(topic):
            if valid_only:
                return not topic.startswith('__') and 'schema' not in topic
            return True
        topics = sorted([t for t in self.admin.list_topics().topics if _valid(t)])
        if include_configs:
            futures_dict = self.admin.describe_configs([ConfigResource(2, topic) for topic in topics])
            topics = {config_resource.name: {c.name: c.value for c in configs.result().values()} for config_resource, configs in futures_dict.items()}
        return topics

    def create_topics(self, topic_config_dict, ignore_existing_topics=True):
        """
        {'topic_a': {'partitions': 1, 'replication_factor': 1, 'segment.ms': 10000}, 'topic_b': {etc}},
        """
        if ignore_existing_topics:
            existing = set(self.list_topics())
            remove = set(topic_config_dict.keys()) & existing
            if remove:
                LOGGER.info(f'These topics already exist, ignoring: {remove}')
                for i in remove:
                    topic_config_dict.pop(i)
        for topic in topic_config_dict:
            topic_config_dict[topic] = NewTopic(
                topic=topic,
                num_partitions=topic_config_dict[topic].pop('partitions'),
                replication_factor=topic_config_dict[topic].pop('replication_factor'),
                config=topic_config_dict[topic])
        if topic_config_dict:
            futures = self.admin.create_topics(list(topic_config_dict.values()), operation_timeout=10)
            for topic in topic_config_dict:
                futures[topic].result()
        LOGGER.info(f'Created topics: {list(topic_config_dict.keys())}')

    def alter_topics(self, topic_config_dict, retain_configs=True, ignore_missing_topics=True, protected_configs=[]):
        """
        {'topic_a': {'partitions': 1, 'replication_factor': 1, 'segment.ms': 10000}, 'topic_b': {etc}}
        """
        current_configs = {}
        if retain_configs:
            current_configs = self.list_topics(include_configs=True)
            for topic in current_configs.items():
                current_configs[topic] = {k: v for k, v in current_configs[topic].items() if k not in protected_configs}
            topics = current_configs.keys()
        else:
            topics = self.list_topics()
        if ignore_missing_topics:
            existing = set(topics)
            missing = set(topic_config_dict.keys()) - existing
            if missing:
                LOGGER.info(f'These topics dont exist, ignoring: {missing}')
                for i in missing:
                    topic_config_dict.pop(i)
        for topic in topic_config_dict:
            current_configs[topic].update(topic_config_dict[topic])
            topic_config_dict[topic] = current_configs[topic]
        if topic_config_dict:
            futures = self.admin.alter_configs([ConfigResource(2, topic, set_config=configs) for topic, configs in topic_config_dict.items()])
            for topic in topic_config_dict:
                futures[topic].result()
        LOGGER.info(f'Altered topics: {list(topic_config_dict.keys())}')

    def delete_topics(self, topics, ignore_missing=True):
        if ignore_missing:
            existing = set(self.list_topics())
            missing = set(topics) - existing
            if missing:
                LOGGER.info(f'These topics dont exist, ignoring: {missing}')
                topics = [i for i in topics if i not in missing]
        if topics:
            futures = self.admin.delete_topics(topics)
            for topic in topics:
                futures[topic].result()
        LOGGER.info(f'Deleted topics: {topics}')

    def produce_messages(self, topic_schema_dict, message_list, topic_override=None):
        producer = Producer(
            urls=self._config.client_urls,
            client_auth_config=self._config.client_auth_config,
            topic_schema_dict=topic_schema_dict,
            schema_registry=SchemaRegistry(self._config.schema_registry_url, auth_config=self._config.schema_registry_auth_config).registry
        )
        LOGGER.info('Producing messages...')
        poll = 0
        if topic_override:
            LOGGER.info(f'A topic override was passed; ignoring the topic provided in each message body and using topic {topic_override} instead')
            for msg in message_list:
                msg['topic'] = topic_override
        for message in message_list:
            message = {k: message.get(k) for k in ['key', 'value', 'headers', 'topic']}
            producer.produce(message.pop('value'), **message)
            poll += 1
            if poll >= 1000:
                producer.poll(0)
                poll = 0
        producer.flush(30)
        LOGGER.info('Produce finished!')

    def consume_messages(self, consume_topics_dict, transform_function=None):
        return TopicDumperApp(consume_topics_dict, app_function=transform_function, fluvii_config=self._config).run()
