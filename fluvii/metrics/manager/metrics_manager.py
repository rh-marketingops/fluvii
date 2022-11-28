"""
Container for standardized application monitoring gauges
"""
from prometheus_client import Gauge, CollectorRegistry
from .config import MetricsManagerConfig
from fluvii.metrics.pusher import MetricsPusher, MetricsPusherConfig


class Metric:
    def __init__(self, metric_name, registry, metrics_config=None, host=None, app=None, description='', additional_labels=None):
        if not metrics_config:
            metrics_config = MetricsManagerConfig()

        if not app:
            app = metrics_config.app_name
        if not host:
            host = metrics_config.hostname
        if not additional_labels:
            additional_labels = []

        self.name = metric_name
        self.app = app
        self.host = host
        self.additional_labels = list(sorted(additional_labels))
        self.all_labels = ['app', 'job'] + self.additional_labels
        self.store = Gauge(self.name, description, labelnames=self.all_labels, registry=registry)

    def _labels(self, label_dict):
        if not self.additional_labels:
            label_dict = {}
        return self.store.labels(self.app, self.host, *[i[1] for i in sorted(label_dict.items())])

    def inc(self, number=1, label_dict=None):
        self._labels(label_dict).inc(number)

    def set(self, number, label_dict=None):
        self._labels(label_dict).set(number)


class MetricsManager:
    """
    Coordinates Prometheus monitoring for a Kafka client application

    Creates and manages Metric instances and pushes their metrics
    """

    def __init__(self, metrics_config=MetricsManagerConfig(), registry=CollectorRegistry(), pusher_cls=MetricsPusher, pusher_config=MetricsPusherConfig()):
        """
        Initializes monitor and Metric classes
        """
        self._config = metrics_config
        self.registry = registry
        self._metrics = {}

        self.new_metric('messages_consumed', description='Messages consumed since application start', additional_labels=['topic']),
        self.new_metric('messages_produced', description='Messages produced since application start', additional_labels=['topic']),
        self.new_metric('message_errors', description='Exceptions caught when processing messages', additional_labels=['exception']),
        self.new_metric('external_requests', description='Network calls to external services', additional_labels=['request_to', 'request_endpoint', 'request_type', 'is_bulk', 'status_code']),
        self.new_metric('seconds_behind', description='Elapsed time since the consumed message was originally produced')

        self.pusher = pusher_cls(self.registry, pusher_config)

    def __getattr__(self, name):
        try:
            return self._metrics[name]
        except:
            return super().__getattribute__(name)

    @property
    def metric_names(self):
        return [metric.name for metric in self._metrics.values()]

    def new_metric(self, metric_name, additional_labels=None, **kwargs):
        self._metrics[metric_name] = Metric(metric_name, self.registry, additional_labels=additional_labels, metrics_config=self._config, **kwargs)

    def inc_metric(self, metric_name, number=1, label_dict=None):
        self._metrics[metric_name].inc(number=number, label_dict=label_dict)

    def set_metric(self, metric_name, number, label_dict=None):
        self._metrics[metric_name].set(number, label_dict=label_dict)
