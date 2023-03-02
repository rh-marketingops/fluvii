"""
Container for standardized application monitoring gauges
"""
from prometheus_client import Gauge, CollectorRegistry
from .config import MetricsManagerConfig
from fluvii.components.metrics.pusher import MetricsPusher, MetricsPusherConfig
from pydantic.error_wrappers import ValidationError


class Metric:
    def __init__(self, metric_name, registry, metrics_config=None, description='', additional_labels=None):
        if not metrics_config:
            metrics_config = MetricsManagerConfig()
        self._config = metrics_config

        if not additional_labels:
            additional_labels = []

        self.name = metric_name
        self.app = self._config.app_name
        self.host = self._config.hostname
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
    pusher_config_cls = MetricsPusherConfig

    def __init__(self, config, pusher=None, auto_start=True):
        """
        Initializes monitor and Metric classes
        """
        self._config = config
        self.registry = CollectorRegistry()
        self._metrics = {}
        self.pusher = self._generate_pusher(pusher)
        self._started = False

        self.new_metric('messages_consumed', description='Messages consumed since application start', additional_labels=['topic']),
        self.new_metric('messages_produced', description='Messages produced since application start', additional_labels=['topic']),
        self.new_metric('message_errors', description='Exceptions caught when processing messages', additional_labels=['exception']),
        self.new_metric('external_requests', description='Network calls to external services', additional_labels=['request_to', 'request_endpoint', 'request_type', 'is_bulk', 'status_code']),
        self.new_metric('seconds_behind', description='Elapsed time since the consumed message was originally produced')

        if auto_start:
            self.start()

    def __getattr__(self, name):
        try:
            return self._metrics[name]
        except:
            return super().__getattribute__(name)

    def _generate_pusher(self, pusher):
        if not pusher and self._config.enable_pushing:
            try:
                cfg = self.pusher_config_cls()
            except ValidationError:
                cfg = self.pusher_config_cls(hostname=self._config.hostname, app_name=self._config.app_name)
            return MetricsPusher(self.registry, config=cfg)
        return pusher

    @property
    def metric_names(self):
        return [metric.name for metric in self._metrics.values()]

    def new_metric(self, metric_name, additional_labels=None, **kwargs):
        self._metrics[metric_name] = Metric(metric_name, self.registry, additional_labels=additional_labels, metrics_config=self._config, **kwargs)

    def inc_metric(self, metric_name, number=1, label_dict=None):
        self._metrics[metric_name].inc(number=number, label_dict=label_dict)

    def set_metric(self, metric_name, number, label_dict=None):
        self._metrics[metric_name].set(number, label_dict=label_dict)

    def start(self):
        if not self._started:
            if self.pusher and self._config.enable_pushing:
                self.pusher.start()
            self._started = True
