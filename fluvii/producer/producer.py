from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from fluvii.general_utils import parse_headers, Admin
from fluvii.exceptions import ProducerTimeoutFailure
import json
import mmh3
import uuid
import logging

LOGGER = logging.getLogger(__name__)


class Producer:
    def __init__(self, urls, schema_registry=None, topic_schema_dict=None, metrics_manager=None, client_auth_config=None, settings_config=None):
        self._urls = ','.join(urls) if isinstance(urls, list) else urls
        self._auth = client_auth_config
        self._settings = settings_config
        self._producer = None
        self._topic_partition_metadata = {}
        self._schema_registry = schema_registry
        self._admin = None

        self.topic_schemas = {}
        self.metrics_manager = metrics_manager

        self._init_producer()

        if not topic_schema_dict:
            topic_schema_dict = {}
        for topic, schema in topic_schema_dict.items():
            self.add_topic(topic, schema)

    def __getattr__(self, attr):
        """Note: this includes methods as well!"""
        try:
            return self.__getattribute__(attr)
        except AttributeError:
            return self._producer.__getattribute__(attr)

    @staticmethod
    def _callback(error, message):
        """
        Logs the returned message from the Broker after producing
        NOTE: Headers not supported on the message for callback for some reason.
        NOTE: Callback requires these args
        """
        if error:
            LOGGER.critical(error)
        else:
            LOGGER.debug(f'Message produced successfully!')

    def _make_config(self):
        settings = {
            "bootstrap.servers": self._urls,

            "on_delivery": self._callback,
            "enable.idempotence": "true",
            "acks": "all",

            # Registry Serialization Settings
            "key.serializer": AvroSerializer(self._schema_registry, schema_str='{"type": "string"}'),
            "value.serializer": lambda: None,
        }

        if self._settings:
            settings.update(self._settings.as_client_dict())
        if self._auth:
            settings.update(self._auth.as_client_dict())
        return settings

    def _init_producer(self):
        LOGGER.info('Initializing producer...')
        self._producer = SerializingProducer(self._make_config())
        self._producer.poll(0)  # allows topic metadata querying to work immediately after init
        LOGGER.info('Producer initialized successfully!')

    def _get_topic_metadata(self, topic):
        partitions = self._producer.list_topics().topics[topic].partitions
        LOGGER.debug(partitions)
        self._topic_partition_metadata.update({topic: len(partitions)})

    def _get_topic_partition_count(self, topic):
        try:
            return self._topic_partition_metadata[topic]
        except KeyError:
            self._get_topic_metadata(topic)
            return self._topic_partition_metadata[topic]

    def _partitioner(self, key, topic):
        return mmh3.hash(key) % self._get_topic_partition_count(topic)

    def _generate_guid(self):
        return str(uuid.uuid1())

    def _add_serializer(self, topic, schema):
        LOGGER.info(f'Adding serializer for producer topic {topic}')
        self.topic_schemas.update({topic: AvroSerializer(self._schema_registry, json.dumps(schema))})

    def add_topic(self, topic, schema, overwrite=False):
        """For adding topics at runtime"""
        if topic in self.topic_schemas and not overwrite:
            LOGGER.debug("producer already has a schema registered for that topic...")
        else:
            LOGGER.info(f'Adding schema for topic {topic}')
            self._get_topic_metadata(topic)
            self._add_serializer(topic, schema)

    def _format_produce(self, value, key, topic, headers, partition, message_passthrough):
        headers_out = {}
        if message_passthrough:
            headers_out = parse_headers(message_passthrough.headers())
            if not key:
                key = message_passthrough.key()

        headers_out.update(headers if headers else {})
        headers_out = {key: value for key, value in headers_out.items() if value is not None}
        if 'guid' not in headers_out:
            headers_out['guid'] = self._generate_guid()

        if not topic:
            topics = [topic for topic in self.topic_schemas if '__changelog' not in topic]
            if len(topics) == 1:
                topic = topics[0]
            else:
                raise Exception('Topic must be defined if managing more than 1 topic')
        if partition is None:
            partition = self._partitioner(key, topic)
        self._producer._value_serializer = self.topic_schemas[topic]
        if '__changelog' not in topic:  # TODO: add a separate logger for changelog stuff, but for now it just clutters things
            LOGGER.debug(f'Adding message to the produce queue for [topic, partition, key] - [{topic}, {partition}, {repr(key)}]')
            LOGGER.info(f'Producing message with guid {headers_out["guid"]}')
        return dict(topic=topic, key=key, value=value, headers=headers_out, partition=partition)

    def produce(self, value, key=None, topic=None, headers=None, partition=None, message_passthrough=None):
        produce_dict = self._format_produce(value, key, topic, headers, partition, message_passthrough)
        self._producer.poll(0)
        self._producer.produce(**produce_dict)
        if self.metrics_manager:
            self.metrics_manager.inc_metric('messages_produced', label_dict={'topic': produce_dict['topic']})

    def _confirm_produce(self, attempts=3, timeout=20):
        """
        Ensure that messages are actually produced by forcing synchronous processing. Must manually check the events queue
        and see if it's truly empty since flushing timeouts do not actually raise an exception for some reason.

        NOTE: Only used for synchronous producing, which is dramatically slower than asychnronous.
        """
        attempt = 1
        LOGGER.debug("Sending/confirming the leftover messages in producer message queue")
        while self._producer.__len__() > 0:
            if attempt <= attempts:
                LOGGER.debug(f"Produce flush attempt: {attempt} of {attempts}")
                self._producer.flush(timeout=timeout)
                attempt += 1
            else:
                raise ProducerTimeoutFailure

    def close(self):
        self._confirm_produce()


class TransactionalProducer(Producer):
    def __init__(self, urls, transactional_id, **kwargs):
        self._transactional_id = transactional_id
        self.active_transaction = False
        super().__init__(urls, **kwargs)

    def _make_config(self):
        config = super()._make_config()
        config.update({"transactional.id": self._transactional_id})
        return config

    def _init_producer(self):
        super()._init_producer()
        self._producer.init_transactions()

    def begin_transaction(self, *args, **kwargs):
        LOGGER.debug('Initializing a transaction...')
        self._producer.begin_transaction(*args, **kwargs)
        self.active_transaction = True
    
    def produce(self, value, key=None, topic=None, headers=None, partition=None, message_passthrough=None):
        if not self.active_transaction:
            self.begin_transaction()
        super().produce(value, key=key, topic=topic, headers=headers, partition=partition, message_passthrough=message_passthrough)
        
    def abort_transaction(self, *args, **kwargs):
        LOGGER.debug('Aborting the transaction...')
        self._producer.abort_transaction(*args, **kwargs)
        self.active_transaction = False
        
    def commit_transaction(self, *args, **kwargs):
        self._producer.commit_transaction(*args, **kwargs)
        self._producer.poll(0)
        self.active_transaction = False

    def close(self):
        pass
