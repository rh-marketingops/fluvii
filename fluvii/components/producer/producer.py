import os
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.avro import load as avro_load
from fluvii.general_utils import parse_headers
from fluvii.exceptions import ProducerTimeoutFailure
import json
import mmh3
import uuid
import logging
import importlib
import importlib.util
import sys

LOGGER = logging.getLogger(__name__)


class Producer:
    def __init__(self, config, schema_registry, topic_schema_dict=None, metrics_manager=None, auto_start=True):
        self._config = config
        self._schema_registry = schema_registry
        self.metrics_manager = metrics_manager
        
        self._producer = None
        self._topic_partition_metadata = {}
        self.topic_schemas = {}
        if not topic_schema_dict:
            topic_schema_dict = {}  # allows for adding during runtime
        self.topic_schema_dict = topic_schema_dict

        self._started = False

        if auto_start:
            self.start()
            
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

    def _make_client_config(self):
        settings = {
            "on_delivery": self._callback,
            "enable.idempotence": "true",
            "acks": "all",

            # Registry Serialization Settings
            "key.serializer": AvroSerializer(self._schema_registry.registry, schema_str='{"type": "string"}'),
            "value.serializer": lambda: None,
        }
        settings.update(self._config.as_client_dict())
        LOGGER.info(f'\nProducer Component Configuration:\n{self._config}')
        return settings

    def _init_producer(self):
        LOGGER.info('Initializing producer...')
        self._producer = SerializingProducer(self._make_client_config())
        self._producer.poll(0)  # allows topic metadata querying to work immediately after init
        for topic, schema in self.topic_schema_dict.items():
            self.add_topic(topic, schema)
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

    def _import_schema_library_from_fp(self):
        LOGGER.debug('Importing schema library from fp')
        schema_library_name = self._settings.schema_library_root.split("/")[-1]
        library_root = self._settings.schema_library_root
        if '__init__' in schema_library_name:
            schema_library_name = self._settings.schema_library_root.split("/")[-2]
        else:
            if [_ for _ in os.listdir(self._settings.schema_library_root) if _ == '__init__.py']:
                library_root = f'{library_root}/__init__.py'  # spec requires a file, not a folder
        if schema_library_name not in sys.modules.keys():
            spec = importlib.util.spec_from_file_location(schema_library_name, library_root)
            module = importlib.util.module_from_spec(spec)
            sys.modules[schema_library_name] = module
            spec.loader.exec_module(module)

    def _import_schema_from_library(self, schema_str):
        LOGGER.debug(f'Importing schema from library path {schema_str}')
        components = schema_str.split(".")
        module = importlib.import_module(".".join(components[:-1]))
        return getattr(module, components[-1])

    def _load_schema_from_str(self, schema_str):
        LOGGER.debug(f'Loading schema from string {schema_str}')
        if schema_str.endswith(('.avro', '.json')):
            try:
                return avro_load(schema_str).to_json()
            except:
                pass
        if self._settings.schema_library_root:
            if schema_str.endswith(('.avro', '.json')):
                try:
                    return avro_load('/'.join([self._settings.schema_library_root, schema_str])).to_json()
                except:
                    pass
            try:
                self._import_schema_library_from_fp()
                return self._import_schema_from_library(schema_str)
            except:
                pass
        return json.loads(schema_str)

    def _add_serializer(self, topic, schema):
        LOGGER.info(f'Adding serializer for producer topic {topic}')
        if isinstance(schema, str):
            schema = self._load_schema_from_str(schema)
        self.topic_schemas.update({topic: AvroSerializer(self._schema_registry.registry, json.dumps(schema))})

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
        
    def start(self):
        if not self._started:
            for obj in [self.metrics_manager, self._schema_registry]:
                if obj:
                    obj.start()
            self._init_producer()
            self._started = True

    def reset(self):
        self._init_producer()
