from . import Consumer, ConsumerConfig
from fluvii.metrics.manager import MetricsManager, MetricsManagerConfig
from fluvii.schema_registry import SchemaRegistry, SchemaRegistryConfig
import logging

LOGGER = logging.getLogger(__name__)


class ConsumerFactory:
    consumer_cls = Consumer
    registry_cls = SchemaRegistry
    metrics_cls = MetricsManager

    def __new__(cls, *args, **kwargs):
        factory = object.__new__(cls)
        factory.__init__(*args, **kwargs)
        return factory.obj_out

    def __init__(
            self, group_name, consume_topics_list, auto_start=True,
            consumer_config=ConsumerConfig(), schema_registry_config=SchemaRegistryConfig(), metrics_manager_config=None):
        self._group_name = group_name
        self._consume_topics_list = consume_topics_list.split(',') if isinstance(consume_topics_list, str) else consume_topics_list
        self._schema_registry_config = schema_registry_config
        self._metrics_manager_config = metrics_manager_config
        self._consumer_config = consumer_config
        self._schema_registry = self._set_schema_registry()
        self._metrics_manager = self._set_metrics_manager()
        self.obj_out = self._set_consumer()
        if auto_start:
            self.obj_out.start()

    def _set_metrics_manager(self):
        LOGGER.info("Generating the MetricsManager component...")
        if not self._metrics_manager_config:
            try:
                self._metrics_manager_config = MetricsManagerConfig()
            except:
                return None
        if self._metrics_manager_config.enable_metrics:
            return self.metrics_cls(self._metrics_manager_config)
        return None

    def _set_schema_registry(self):
        LOGGER.info("Generating the SchemaRegistry component...")
        return self.registry_cls(self._schema_registry_config)

    def _set_consumer(self):
        LOGGER.info("Generating the Consumer component")
        return self.consumer_cls(
            self._group_name,
            self._consume_topics_list,
            self._consumer_config,
            self._schema_registry,
            metrics_manager=self._metrics_manager,
        )