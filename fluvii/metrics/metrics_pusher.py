"""
Class for pushing metrics to Prometheus metrics cache (pushgateway)
"""
import logging
import socket
from prometheus_client import CollectorRegistry, push_to_gateway

LOGGER = logging.getLogger(__name__)


class MetricsPusher:
    """
    Pushes metrics to a prometheus pushgateway in a Kubernetes environment
    """

    def __init__(self, registry, hostname, service_name, service_port, app_port):
        """
        Initializes metrics pusher

        :param hostname: Unique name of running application
        :param service_name: host name of metrics service
        :param service_port: port of metrics service
        :param app_port: port for metrics cache on individual pod
        """

        self.registry = registry
        self.hostname = hostname
        self.service_name = service_name
        self.service_port = service_port
        self.app_port = app_port

        self.metrics_pod_ips = []

    def set_metrics_pod_ips(self):
        """
        Queries metrics service for gateway IP addresses

        A single Kubernetes service redirects to multiple IP addresses for
        redundant Prometheus pushgateways.
        :return: None
        """
        socket_info_list = socket.getaddrinfo(self.service_name, self.service_port)
        self.metrics_pod_ips = {f'{result[-1][0]}:{self.app_port}'
                                for result in socket_info_list}
        LOGGER.debug(f'Set gateway addresses: {self.metrics_pod_ips}')

    def push_metrics(self):
        for gateway in self.metrics_pod_ips:
            try:
                push_to_gateway(gateway, job=self.hostname, registry=self.registry, timeout=15)
            except Exception as error:
                LOGGER.error(f'Failed to push to pushgateway {gateway}\n{error}')
                self.set_metrics_pod_ips()
