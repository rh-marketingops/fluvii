from fluvii.config_bases import FluviiConfigBase
from typing import Union


class MetricsPusherConfig(FluviiConfigBase):
    hostname: str
    kubernetes_headless_service_name: str
    kubernetes_headless_service_port: Union[str, int]
    kubernetes_pod_app_port: Union[str, int]
    push_rate_seconds: int = 10

    class Config(FluviiConfigBase.Config):
        env_prefix = "FLUVII_METRICS_PUSHER_"
