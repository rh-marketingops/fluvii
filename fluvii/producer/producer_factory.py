from . import Producer, ProducerConfig
from fluvii.metrics.manager import MetricsManager, MetricsManagerConfig
from fluvii.schema_registry import SchemaRegistry, SchemaRegistryConfig
import logging

LOGGER = logging.getLogger(__name__)


class ProducerFactory:
    producer_cls = Producer
    registry_cls = SchemaRegistry
    metrics_cls = MetricsManager

    def __new__(cls, *args, **kwargs):
        factory = object.__new__(cls)
        factory.__init__(*args, **kwargs)
        return factory.obj_out

    def __init__(
            self, topic_schema_dict=None,
            producer_config=None, schema_registry_config=None, metrics_manager_config=None,
            auto_start=True
    ):
        if not producer_config:
            producer_config = ProducerConfig()
        if not schema_registry_config:
            schema_registry_config = SchemaRegistryConfig()

        self._topic_schema_dict = topic_schema_dict if isinstance(topic_schema_dict, dict) else {}
        self._auto_start = auto_start
        self._schema_registry_config = schema_registry_config
        self._metrics_manager_config = metrics_manager_config
        self._producer_config = producer_config
        self._schema_registry = self._make_schema_registry()
        self._metrics_manager = self._make_metrics_manager()
        self.obj_out = self._make_producer()

    def _make_metrics_manager(self):
        LOGGER.info("Generating the MetricsManager component...")
        if not self._metrics_manager_config:
            try:
                self._metrics_manager_config = MetricsManagerConfig()
            except:
                return None
        if self._metrics_manager_config.enable_metrics:
            return self.metrics_cls(self._metrics_manager_config, auto_start=self._auto_start)
        return None

    def _make_schema_registry(self):
        LOGGER.info("Generating the SchemaRegistry component...")
        return self.registry_cls(self._schema_registry_config, auto_start=self._auto_start)

    def _make_producer(self):
        LOGGER.info("Generating the Producer component...")
        return self.producer_cls(
            self._producer_config,
            self._schema_registry,
            topic_schema_dict=self._topic_schema_dict,
            metrics_manager=self._metrics_manager,
            auto_start=self._auto_start
        )
