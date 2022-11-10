from os import environ


class MetricsPusherConfig:
    def __init__(self, hostname=None):
        if not hostname:
            hostname = environ.get('FLUVII_HOSTNAME')
        self.hostname = hostname
        self.headless_service_name = environ.get('FLUVII_METRICS_PUSHER_KUBERNETES_HEADLESS_SERVICE_NAME')
        self.headless_service_port = environ.get('FLUVII_METRICS_PUSHER_KUBERNETES_HEADLESS_SERVICE_PORT')
        self.metrics_port = environ.get('FLUVII_METRICS_PUSHER_KUBERNETES_POD_APP_PORT')
        self.push_rate_seconds = int(environ.get('FLUVII_METRICS_PUSHER_PUSH_RATE_SECONDS', '10'))

        self.enable_pushing = environ.get('FLUVII_METRICS_PUSHER_ENABLE_PUSHING', 'false')
        if self.enable_pushing.lower() == 'true':
            self.enable_pushing = True
        else:
            self.enable_pushing = False
