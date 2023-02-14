from fluvii.components.admin import Admin, AdminConfig
from fluvii.components import (
    ConsumerFactory,
    ProducerFactory,
    ProducerConfig,
    ConsumerConfig,
    SchemaRegistryConfig
)
from confluent_kafka.admin import NewTopic, ConfigResource
import logging


LOGGER = logging.getLogger(__name__)


class FluviiToolbox(ProducerFactory, ConsumerFactory):
    """
    Helpful functions for interacting with Kafka.
    """
    def __new__(cls, *args, **kwargs):
        toolbox = object.__new__(cls)
        toolbox.__init__(*args, **kwargs)
        return toolbox

    def __init__(self, admin_config=None, producer_config=None, consumer_config=None, schema_registry_config=None):
        # producer
        self._producer_config = producer_config
        self._topic_schema_dict = None

        # consumer
        self._consumer_config = consumer_config
        self._consume_topics_list = None

        self._auto_start = True
        self._schema_registry_config = schema_registry_config
        self._metrics_manager_config = None
        self._schema_registry = None
        self._metrics_manager = None
        self.admin = None
        self._admin_config = admin_config
        if self._admin_config:
            self._set_admin()

    def _return_class(self):
        return self

    def _set_admin(self):
        if not self.admin:
            if not self._admin_config:
                self._admin_config = AdminConfig()
            self.admin = Admin(config=self._admin_config).admin_client

    def _set_schema_registry(self):
        if not self._schema_registry_config:
            self._schema_registry_config = SchemaRegistryConfig()
        if not self._schema_registry:
            self._schema_registry = super()._make_schema_registry()

    def list_topics(self, valid_only=True, include_configs=False):
        def _valid(topic):
            if valid_only:
                return not topic.startswith('__') and 'schema' not in topic
            return True
        self._set_admin()
        topics = sorted([t for t in self.admin.list_topics().topics if _valid(t)])
        if include_configs:
            futures_dict = self.admin.describe_configs([ConfigResource(2, topic) for topic in topics])
            topics = {config_resource.name: {c.name: c.value for c in configs.result().values()} for config_resource, configs in futures_dict.items()}
        return topics

    def create_topics(self, topic_config_dict, ignore_existing_topics=True):
        """
        {'topic_a': {'partitions': 1, 'replication.factor': 1, 'segment.ms': 10000}, 'topic_b': {etc}},
        """
        self._set_admin()
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
                replication_factor=topic_config_dict[topic].pop('replication.factor'),
                config=topic_config_dict[topic])
        if topic_config_dict:
            futures = self.admin.create_topics(list(topic_config_dict.values()), operation_timeout=10)
            for topic in topic_config_dict:
                futures[topic].result()
        LOGGER.info(f'Created topics: {list(topic_config_dict.keys())}')

    def alter_topics(self, topic_config_dict, retain_configs=True, ignore_missing_topics=True, protected_configs=None):
        """
        {'topic_a': {'partitions': 1, 'replication.factor': 1, 'segment.ms': 10000}, 'topic_b': {etc}}
        """
        self._set_admin()
        if not protected_configs:
            protected_configs = []
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
        self._set_admin()
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

    def _prepare_producer(self, topic_schema_dict):
        self._set_schema_registry()
        if not self._producer_config:
            self._producer_config = ProducerConfig()
        self._topic_schema_dict = topic_schema_dict

    def _prepare_consumer(self, consume_topics_list):
        self._set_schema_registry()
        if not self._consumer_config:
            self._consumer_config = ConsumerConfig()
        self._consume_topics_list = consume_topics_list

    def produce_messages(self, topic_schema_dict, message_list, topic_override=None, use_given_partitions=False):
        self._prepare_producer(topic_schema_dict)
        producer = self._make_producer()
        LOGGER.info('Producing messages...')
        poll = 0
        if topic_override:
            LOGGER.info(f'A topic override was passed; ignoring the topic provided in each message body and using topic {topic_override} instead')
            for msg in message_list:
                msg['topic'] = topic_override
        for message in message_list:
            keyset = ['key', 'value', 'headers', 'topic']
            if use_given_partitions:
                keyset.append('partition')
            message = {k: message.get(k) for k in keyset}
            producer.produce(message.pop('value'), **message)
            poll += 1
            if poll >= 1000:
                producer.poll(0)
                poll = 0
        producer.flush(30)
        LOGGER.info('Produce finished!')

    def consume_messages(self, consume_topics_dict, transform_function=None):
        # TODO: convert this to just use a consumer
        from fluvii.kafka_tools.topic_dumper import TopicDumperAppFactory
        return TopicDumperAppFactory(consume_topics_dict, app_function=transform_function).run()
