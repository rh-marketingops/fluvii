from fluvii.consumer import TransactionalConsumer, ConsumerConfig
from fluvii.producer import TransactionalProducer, ProducerConfig
from fluvii.schema_registry import SchemaRegistry, SchemaRegistryConfig
from fluvii.metrics import MetricsManager, MetricsManagerConfig
from .config import FluviiAppConfig
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
            fluvii_config=None, schema_registry_config=None,
            producer_config=None, consumer_config=None, metrics_manager_config=None):
        if not fluvii_config:
            fluvii_config = FluviiAppConfig()
        if not schema_registry_config:
            schema_registry_config = SchemaRegistryConfig()
        if not producer_config:
            producer_config = ProducerConfig()
        if not consumer_config:
            consumer_config = ConsumerConfig()

        self._app_function = app_function
        self._fluvii_config = fluvii_config
        self._schema_registry_config = schema_registry_config
        self._producer_config = producer_config
        self._consumer_config = consumer_config
        self._metrics_manager_config = metrics_manager_config
        self._schema_registry = self._make_schema_registry()
        self._metrics_manager = self._make_metrics_manager()
        self._app_function_arglist = app_function_arglist if isinstance(app_function_arglist, list) else []
        self._consume_topics_list = consume_topics_list.split(',') if isinstance(consume_topics_list, str) else consume_topics_list
        self._produce_topic_schema_dict = produce_topic_schema_dict if isinstance(produce_topic_schema_dict, dict) else {}
        self.obj_out = self._return_class()

    def _make_metrics_manager(self):
        LOGGER.info("Generating the MetricsManager component...")
        if not self._metrics_manager_config:
            self._metrics_manager_config = MetricsManagerConfig(app_name=self._fluvii_config.name, hostname=self._fluvii_config.hostname)
        if self._metrics_manager_config.enable_metrics:
            return self.metrics_cls(self._metrics_manager_config, auto_start=False)
        return None

    def _make_schema_registry(self):
        LOGGER.info("Generating the SchemaRegistry component...")
        return self.registry_cls(self._schema_registry_config, auto_start=False)

    def _make_producer(self):
        LOGGER.info("Generating the Producer component...")
        return self.producer_cls(
            self._fluvii_config.hostname,
            self._producer_config,
            self._schema_registry,
            topic_schema_dict=self._produce_topic_schema_dict,
            metrics_manager=self._metrics_manager,
            auto_start=False,
        )

    def _make_consumer(self, auto_subscribe=True):
        LOGGER.info("Generating the Consumer component...")
        return self.consumer_cls(
            self._fluvii_config.name,
            self._consume_topics_list,
            self._consumer_config,
            self._schema_registry,
            metrics_manager=self._metrics_manager,
            auto_start=False,
            auto_subscribe=auto_subscribe,
        )

    def _return_class(self):
        LOGGER.info(f'initializing app class {self.fluvii_app_cls}')
        return self.fluvii_app_cls(
            self._app_function, self._consume_topics_list, self._fluvii_config, self._make_producer(), self._make_consumer(), self._schema_registry,
            app_function_arglist=self._app_function_arglist, metrics_manager=self._metrics_manager, produce_topic_schema_dict=self._produce_topic_schema_dict
        )
