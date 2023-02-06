from os import environ
from typing import Literal, Optional
from pydantic import BaseSettings, Field


# TODO: Not happy with how this is organized/works, but needed to release something for now.
class MetricsManagerConfig(BaseSettings):
    hostname: str
    app_name: str
    enable_metrics: bool = False
    enable_pushing: bool = False

    class Config:
        env_prefix = "FLUVII_METRICS_MANAGER"
