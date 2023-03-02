from . import Consumer, ConsumerConfig
from fluvii.components.metrics import MetricsManager, MetricsManagerConfig
from fluvii.components.schema_registry import SchemaRegistry, SchemaRegistryConfig
import logging

LOGGER = logging.getLogger(__name__)


class ConsumerFactory:
    consumer_cls = Consumer
    consumer_config_cls = ConsumerConfig
    registry_cls = SchemaRegistry
    registry_config_cls = SchemaRegistryConfig
    metrics_cls = MetricsManager
    metrics_config_cls = MetricsManagerConfig

    def __new__(cls, *args, **kwargs):
        factory = object.__new__(cls)
        factory.__init__(*args, **kwargs)
        return factory.obj_out

    def __init__(
            self, group_name, consume_topics_list,
            consumer_config=None, schema_registry_config=None, metrics_manager_config=None,
            auto_start=True, auto_subscribe=True
    ):
        if not consumer_config:
            consumer_config = self.consumer_config_cls()
        if not schema_registry_config:
            schema_registry_config = self.registry_config_cls()

        self._group_name = group_name
        self._consume_topics_list = consume_topics_list.split(',') if isinstance(consume_topics_list, str) else consume_topics_list
        self._auto_start = auto_start
        self._auto_subscribe = auto_subscribe
        self._schema_registry_config = schema_registry_config
        self._metrics_manager_config = metrics_manager_config
        self._consumer_config = consumer_config
        self._schema_registry = self._make_schema_registry()
        self._metrics_manager = self._make_metrics_manager()
        self.obj_out = self._make_consumer()
        if self._auto_start:
            self.obj_out.start()

    def _make_metrics_manager(self):
        LOGGER.info("Generating the MetricsManager component...")
        if not self._metrics_manager_config:
            try:
                self._metrics_manager_config = self.metrics_config_cls()
            except:
                return None
        if self._metrics_manager_config.enable_metrics:
            return self.metrics_cls(self._metrics_manager_config, auto_start=False)
        return None

    def _make_schema_registry(self):
        LOGGER.info("Generating the SchemaRegistry component...")
        return self.registry_cls(self._schema_registry_config, auto_start=False)

    def _make_consumer(self):
        LOGGER.info("Generating the Consumer component")
        return self.consumer_cls(
            self._group_name,
            self._consume_topics_list,
            self._consumer_config,
            self._schema_registry,
            metrics_manager=self._metrics_manager,
            auto_start=self._auto_start,
            auto_subscribe=self._auto_subscribe
        )
