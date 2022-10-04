from os import environ
from .metrics_pusher import MetricsPusher
from prometheus_client import CollectorRegistry


class MetricsConfig:
    def __init__(self, hostname=None, metrics_service_name=None, metrics_service_port=None, metrics_pod_port=None, pusher=None, registry=None):

        if not hostname:
            hostname = environ.get('FLUVII_HOSTNAME')
        if not metrics_service_name:
            metrics_service_name = environ.get('FLUVII_METRICS_SERVICE_NAME')
        if not metrics_service_port:
            metrics_service_port = environ.get('FLUVII_METRICS_SERVICE_PORT')
        if not metrics_pod_port:
            metrics_pod_port = environ.get('FLUVII_METRICS_POD_PORT')



        self.hostname = None
        self.app_name = None
        self.registry = None
        self.enable_metrics_pushing = None
        self.kubernetes_headless_service_name = None
        self.kubernetes_headless_service_port = None
        self.metrics_port = None




        if not registry:
            self.registry = CollectorRegistry()
        if not pusher:
            self.pusher = MetricsPusher(
                self.registry,
                self.hostname,
                self.kubernetes_headless_service_name,
                self.kubernetes_headless_service_port,
                self.metrics_port)
