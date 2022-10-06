from os import environ
from .metrics_pusher import MetricsPusher
from prometheus_client import CollectorRegistry


class MetricsConfig:
    def __init__(self, hostname=None, app_name=None, app_port=None,
                 push_headless_service_name=None, push_headless_service_port=None,
                 pusher=None, registry=None, enable_pushing=None):

        if not hostname:
            hostname = environ.get('FLUVII_HOSTNAME')
        if not app_name:
            app_name = environ.get('FLUVII_APP_NAME')
        if not app_port:
            app_port = environ.get('FLUVII_METRICS_APP_PORT')
        if not push_headless_service_name:
            push_headless_service_name = environ.get('FLUVII_METRICS_KUBERNETES_PUSH_HEADLESS_SERVICE_NAME')
        if not push_headless_service_port:
            push_headless_service_port = environ.get('FLUVII_METRICS_KUBERNETES_PUSH_HEADLESS_SERVICE_PORT')
        if not registry:
            registry = CollectorRegistry()
        if not pusher:
            pusher = MetricsPusher(
                registry,
                hostname,
                push_headless_service_name,
                push_headless_service_port,
                app_port)
        if not enable_pushing:
            enable_pushing = environ.get('FLUVII_METRICS_ENABLE_PUSHING', '')
        if enable_pushing.lower() == 'true':
            enable_pushing = True
        else:
            enable_pushing = False

        self.hostname = hostname
        self.app_name = app_name
        self.push_headless_service_name = push_headless_service_name
        self.push_headless_service_port = push_headless_service_port
        self.app_port = app_port
        self.enable_pushing = enable_pushing
        self.registry = registry
        self.pusher = pusher
