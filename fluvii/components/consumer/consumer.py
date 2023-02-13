from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from fluvii.exceptions import NoMessageError, ConsumeMessageError
from fluvii.general_utils import parse_headers, get_guid_from_message
from copy import deepcopy
import logging
import datetime

LOGGER = logging.getLogger(__name__)


class Consumer:
    def __init__(self, group_id, consume_topics_list, config, schema_registry, metrics_manager=None, auto_start=True, auto_subscribe=True):
        self._group_id = group_id
        self.topics = consume_topics_list if isinstance(consume_topics_list, list) else consume_topics_list.split(',')
        self._config = config
        self.metrics_manager = metrics_manager
        self._schema_registry = schema_registry

        self._tz = datetime.datetime.utcnow().astimezone().tzinfo
        self._consumer = None
        self._topic_metadata = None
        self.message = None
        self._poll_timeout = self._config.poll_timeout_seconds
        self._auto_subscribe = auto_subscribe
        self._started = False

        if auto_start:
            self.start()

    def __getattr__(self, attr):
        """Note: this includes methods as well!"""
        try:
            return self.__getattribute__(attr)
        except AttributeError:
            return self._consumer.__getattribute__(attr)

    @staticmethod
    def _consume_message_callback(error, partitions):
        """
        Logs the info returned when a successful commit is performed
        NOTE: Callback requires these args
        """
        if error:
            LOGGER.critical(error)
        else:
            LOGGER.debug('Consumer Callback - Message consumption committed successfully')

    def _make_client_config(self):
        settings = {
            "group.id": self._group_id,
            "on_commit": self._consume_message_callback,
            "enable.auto.offset.store": False,  # ensures auto-committing doesn't happen before the consumed message is actually finished
            "partition.assignment.strategy": 'cooperative-sticky',

            # Registry Serialization Settings
            "key.deserializer": AvroDeserializer(self._schema_registry.registry, schema_str='{"type": "string"}'),
            "value.deserializer": AvroDeserializer(self._schema_registry.registry) if self._schema_registry else None,
        }
        settings.update(self._config.as_client_dict())
        LOGGER.info(f'\nConsumer Component Configuration:\n{self._config}')
        return settings

    def _init_consumer(self):
        LOGGER.info('Initializing Consumer...')
        self._consumer = DeserializingConsumer(self._make_client_config())
        LOGGER.info('Consumer Initialized!')
        if self._auto_subscribe:
            self._consumer.subscribe(topics=self.topics)
            LOGGER.info(f'Consumer subscribed to topics {self.topics}!')
        else:
            LOGGER.info(f'No topics assigned; awaiting a manual "subscribe" or "assign" command')

    def _poll_for_message(self, timeout=None):
        """Is a separate method to isolate communication interface with Kafka"""
        if not timeout:
            timeout = self._poll_timeout
        message = self._consumer.poll(timeout)
        if message is None:
            raise NoMessageError
        return message

    def _handle_consumed_message(self):
        """
        Handles a consumed message to check for errors and log the consumption as a metric.
        If the message is returned with a breaking error, raises a ConsumeMessageError.
        """
        try:
            guid = get_guid_from_message(self.message)
            if '__changelog' not in self.message.topic():
                LOGGER.info(f"Message consumed from topic {self.message.topic()} partition {self.message.partition()}, offset {self.message.offset()}; GUID {guid}")
                LOGGER.debug(f"Consumed message key: {repr(self.message.key())}")
                if self.metrics_manager:
                    self.metrics_manager.set_metric('seconds_behind', round(datetime.datetime.timestamp(datetime.datetime.utcnow())) - self.message.timestamp()[1] // 1000)
                    self.metrics_manager.inc_metric('messages_consumed', label_dict={'topic': self.message.topic()})
        except AttributeError:
            if "object has no attribute 'headers'" in str(self.message.error()):
                raise ConsumeMessageError("Headers were inaccessible on the message. Potentially a corrupt message?")

    def key(self):
        return deepcopy(self.message.key())

    def value(self):
        return deepcopy(self.message.value())

    def headers(self):
        return deepcopy(parse_headers(self.message.headers()))

    def messages(self):
        return [self.message]

    def consume(self, timeout=None):
        """
        Consumes a message from the broker while handling errors.
        If the message is valid, then the message is returned.
        """
        self.message = self._poll_for_message(timeout)
        self._handle_consumed_message()
        return self.message

    def commit(self):
        self._consumer.store_offsets(self.message)
        self.message = None

    def start(self):
        if not self._started:
            for obj in [self.metrics_manager, self._schema_registry]:
                if obj:
                    obj.start()
            self._init_consumer()
            self._started = True
