"""
A multithreading monitoring setup for use with non-streams Kafka applications

The thread target function calls the metrics pusher every couple of seconds,
and then waits.
"""
import threading
import time

from .metrics_manager import MetricsManager


def metrics_push_function(metrics_manager: MetricsManager, push_rate_seconds: int):
    """
    Sets gateways and pushes metrics

    Used as a multi-threading target
    :param metrics_manager: Pushes prometheus metrics to gateway
    :param push_rate_seconds: Amount of time to sleep between pushes
    """
    while True:
        time.sleep(push_rate_seconds)
        metrics_manager.push_metrics()


def get_metrics_pushing_thread(metrics_manager: MetricsManager, push_rate_seconds: int):
    """
    Creates and returns threading object for monitoring the app

    Thread is a daemon since it should exit as soon as the regular program exits
    :param metrics_manager: Pushes prometheus metrics to gateway
    :param push_rate_seconds: Amount of time to sleep between pushes
    """
    return threading.Thread(target=metrics_push_function, args=(metrics_manager, push_rate_seconds), daemon=True)


def start_pushing_metrics(metrics_manager: MetricsManager, push_rate_seconds: int):
    """
    Creates and starts a monitoring thread

    :param metrics_manager: Pushes prometheus metrics to gateway
    :param push_rate_seconds: Amount of time to sleep between pushes
    """
    monitoring_thread = get_metrics_pushing_thread(metrics_manager, push_rate_seconds)
    monitoring_thread.start()
