from fluvii.consumer import TransactionalConsumer, ConsumerConfig
from fluvii.producer import TransactionalProducer, ProducerConfig
from fluvii.schema_registry import SchemaRegistry, SchemaRegistryConfig
from fluvii.metrics import MetricsManager, MetricsManagerConfig
from . import FluviiApp, FluviiConfig
import logging

LOGGER = logging.getLogger(__name__)


class FluviiAppFactory:
    def __new__(cls, *args, **kwargs):
        factory = object.__new__(cls)
        factory.__init__(*args, **kwargs)
        return factory.obj_out

    def __init__(
            self, app_function, consume_topics_list, produce_topic_schema_dict=None, app_function_arglist=None,
            fluvii_config=FluviiConfig(), fluvii_app_cls=FluviiApp,
            producer_config=ProducerConfig(), consumer_config=ConsumerConfig(), schema_registry_config=SchemaRegistryConfig(),
            metrics_manager_config=MetricsManagerConfig(), alternative_component_dict=None):
        self._config = fluvii_config
        self._component_dict = self._set_component_dict(alternative_component_dict)
        self._schema_registry = self._set_schema_registry(schema_registry_config)
        self._metrics_manager = self._set_metrics_manager(metrics_manager_config)
        producer = self._set_producer(producer_config, produce_topic_schema_dict)
        consumer = self._set_consumer(consumer_config, consume_topics_list)

        self.obj_out = fluvii_app_cls(
            app_function, consume_topics_list, self._config, producer, consumer, self._schema_registry,
            app_function_arglist=app_function_arglist, metrics_manager=self._metrics_manager
        )

    def _set_component_dict(self, component_dict):
        """
        Allows an easy way to override components with your own, if you so need. Not needed for the average user.
        """
        if not component_dict:
            component_dict = {}
        default_component_dict = {
            'producer': TransactionalProducer,
            'consumer': TransactionalConsumer,
            'metrics_manager': MetricsManager,
            'schema_registry': SchemaRegistry,
        }
        default_component_dict.update(component_dict)
        return default_component_dict

    def _set_metrics_manager(self, metrics_manager_config):
        LOGGER.info("Generating the MetricsManager component...")
        return self._component_dict['metrics'](metrics_manager_config)

    def _set_schema_registry(self, schema_registry_config):
        LOGGER.info("Generating the SchemaRegistry component...")
        return self._component_dict['registry'](schema_registry_config)

    def _set_producer(self, producer_config, topic_schema_dict):
        # TODO: make a "restart" function on the producer
        LOGGER.info("Generating the Producer component...")
        return self._component_dict['producer'](
            self._config.hostname,
            producer_config,
            self._schema_registry,
            topic_schema_dict=topic_schema_dict,
            metrics_manager=self._metrics_manager
        )

    def _set_consumer(self, consumer_config, consume_topics_list):
        LOGGER.info("Generating the Consumer component")
        return self._component_dict['consumer'](
            self._config.name,
            consume_topics_list,
            consumer_config,
            self._schema_registry,
            metrics_manager=self._metrics_manager
        )
