"""
Class for pushing metrics to Prometheus metrics cache (pushgateway)
"""
import logging
import socket
from prometheus_client import push_to_gateway
import threading
import time

LOGGER = logging.getLogger(__name__)


class MetricsPusher:
    """
    Pushes metrics to a prometheus pushgateway in a Kubernetes environment
    """

    def __init__(self, registry, config, auto_start=True):
        self._config = config
        self.registry = registry
        self.push_thread = None
        self._started = False

        self.metrics_pod_ips = []

        if auto_start:
            self.start()

    def _set_metrics_pod_ips(self):
        """
        Queries metrics service for gateway IP addresses

        A single Kubernetes service redirects to multiple IP addresses for
        redundant Prometheus pushgateways.
        :return: None
        """
        try:
            socket_info_list = socket.getaddrinfo(self._config.kubernetes_headless_service_name, self._config.kubernetes_headless_service_port)
            self.metrics_pod_ips = {f'{result[-1][0]}:{self._config.kubernetes_pod_app_port}' for result in socket_info_list}
            LOGGER.debug(f'Set gateway addresses: {self.metrics_pod_ips}')
        except Exception:
            LOGGER.exception('Failed to set metric pod ips')

    def _push_metrics(self):
        for gateway in self.metrics_pod_ips:
            try:
                push_to_gateway(gateway, job=self._config.hostname, registry=self.registry, timeout=15)
            except Exception as error:
                LOGGER.error(f'Failed to push to pushgateway {gateway}\n{error}')
                self._set_metrics_pod_ips()

    def _push_metrics_loop(self):
        while True:
            self._set_metrics_pod_ips()
            time.sleep(self._config.push_rate_seconds)
            self._push_metrics()

    def _create_metrics_pushing_thread(self):
        self.push_thread = threading.Thread(target=self._push_metrics_loop, daemon=True)

    def _start_pushing_metrics(self):
        if not self.push_thread:
            self._create_metrics_pushing_thread()
        try:
            self.push_thread.start()
        except:
            pass

    def _stop_pushing_metrics(self):
        if self.push_thread:
            try:
                self.push_thread.stop()  # TODO: .stop() isn't a thing, but it's not really used so...leave it for now
            except:
                pass

    def start(self):
        if not self._started:
            self._start_pushing_metrics()

    def stop(self):
        self._stop_pushing_metrics()
