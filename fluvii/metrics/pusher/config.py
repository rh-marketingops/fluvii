from typing import Optional
from pydantic import BaseSettings


class MetricsPusherConfig(BaseSettings):
    hostname: str
    kubernetes_headless_service_name: str
    kubernetes_headless_service_port: Optional[str, int]
    kubernetes_pod_app_port: Optional[str, int]
    push_rate_seconds: int = 10

    class Config:
        env_prefix = "FLUVII_METRICS_PUSHER"
