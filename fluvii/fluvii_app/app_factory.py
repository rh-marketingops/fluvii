from fluvii.consumer import TransactionalConsumer, ConsumerConfig
from fluvii.producer import TransactionalProducer, ProducerConfig
from fluvii.schema_registry import SchemaRegistry, SchemaRegistryConfig
from fluvii.metrics import MetricsManager, MetricsManagerConfig
from .config import FluviiConfig
import logging

LOGGER = logging.getLogger(__name__)


class AppFactory:
    fluvii_app_cls = None
    producer_cls = TransactionalProducer
    consumer_cls = TransactionalConsumer
    metrics_cls = MetricsManager
    registry_cls = SchemaRegistry

    def __new__(cls, *args, **kwargs):
        factory = object.__new__(cls)
        factory.__init__(*args, **kwargs)
        return factory.obj_out

    def __init__(
            self, app_function, consume_topics_list, produce_topic_schema_dict=None, app_function_arglist=None,
            fluvii_config=FluviiConfig(), producer_config=ProducerConfig(), consumer_config=ConsumerConfig(),
            schema_registry_config=SchemaRegistryConfig(), metrics_manager_config=MetricsManagerConfig()):
        self._fluvii_config = fluvii_config
        self._producer_config = producer_config
        self._consumer_config = consumer_config
        self._schema_registry_config = schema_registry_config
        self._metrics_manager_config = metrics_manager_config
        self._schema_registry = self._set_schema_registry()
        self._metrics_manager = self._set_metrics_manager()
        producer = self._set_producer(produce_topic_schema_dict)
        consumer = self._set_consumer(consume_topics_list)

        self.obj_out = self.fluvii_app_cls(
            app_function, consume_topics_list, self._fluvii_config, producer, consumer, self._schema_registry,
            app_function_arglist=app_function_arglist, metrics_manager=self._metrics_manager
        )

    def _set_metrics_manager(self):
        LOGGER.info("Generating the MetricsManager component...")
        return self.metrics_cls(self._metrics_manager_config)

    def _set_schema_registry(self):
        LOGGER.info("Generating the SchemaRegistry component...")
        return self.registry_cls(self._schema_registry_config)

    def _set_producer(self, topic_schema_dict):
        # TODO: make a "restart" function on the producer
        LOGGER.info("Generating the Producer component...")
        return self.producer_cls(
            self._fluvii_config.hostname,
            self._producer_config,
            self._schema_registry,
            topic_schema_dict=topic_schema_dict,
            metrics_manager=self._metrics_manager
        )

    def _set_consumer(self, consume_topics_list):
        LOGGER.info("Generating the Consumer component")
        return self.consumer_cls(
            self._fluvii_config.name,
            consume_topics_list,
            self._consumer_config,
            self._schema_registry,
            metrics_manager=self._metrics_manager
        )
