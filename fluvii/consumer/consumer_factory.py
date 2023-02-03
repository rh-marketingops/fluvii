from . import Consumer
from . import ConsumerConfig
from fluvii.metrics.manager import MetricsManager, MetricsManagerConfig
from fluvii.schema_registry import SchemaRegistry, SchemaRegistryConfig


class ConsumerFactory:
    def __new__(cls, *args, **kwargs):
        factory = object.__new__(cls)
        factory.__init__(*args, **kwargs)
        return factory.obj_out

    def __init__(
            self, group_id, consume_topics_list, consumer_class=Consumer, auto_start=True,
            consumer_config=ConsumerConfig(), schema_registry_config=SchemaRegistryConfig(), metrics_manager_config=MetricsManagerConfig()):
        metrics_manager = MetricsManager(config=metrics_manager_config)
        schema_registry = SchemaRegistry(config=schema_registry_config)
        self.obj_out = consumer_class(group_id, consume_topics_list, consumer_config, schema_registry, metrics_manager)
        if auto_start:
            self.obj_out.start()


# class Factory:
#     def __new__(cls, *args, **kwargs):
#         factory = object.__new__(cls)
#         factory.__init__(*args, **kwargs)
#         return factory.woo
#
#     def __init__(self, woo):
#         self.woo = woo

