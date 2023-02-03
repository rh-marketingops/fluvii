from .producer import Producer
from .config import ProducerConfig
from fluvii.metrics.manager import MetricsManager, MetricsManagerConfig
from fluvii.schema_registry import SchemaRegistry, SchemaRegistryConfig


class ProducerFactory:
    def __new__(cls, *args, **kwargs):
        factory = object.__new__(cls)
        factory.__init__(*args, **kwargs)
        return factory.obj_out

    def __init__(
            self, topic_schema_dict=None, producer_class=Producer, auto_start=True,
            producer_config=ProducerConfig(), schema_registry_config=SchemaRegistryConfig(), metrics_manager_config=MetricsManagerConfig()):
        metrics_manager = MetricsManager(config=metrics_manager_config)
        schema_registry = SchemaRegistry(config=schema_registry_config)
        self.obj_out = producer_class(producer_config, schema_registry, topic_schema_dict=topic_schema_dict, metrics_manager=metrics_manager)
        if auto_start:
            self.obj_out.start()
