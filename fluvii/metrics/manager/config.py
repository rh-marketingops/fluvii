from os import environ
from typing import Literal, Optional
from pydantic import BaseSettings, Field


# TODO: Not happy with how this is organized/works, but needed to release something for now.
class MetricsManagerConfig(BaseSettings):
    hostname: str
    app_name: str
    enable_pushing: bool = True

    class Config:
        env_prefix = "FLUVII_METRICS_MANAGER"
