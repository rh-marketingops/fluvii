from pydantic import BaseSettings
from typing import Optional


class SchemaRegistryConfig(BaseSettings):
    """
    Common configs, along with some custom ones, that likely wont need to be changed from their defaults.
    """
    # client settings
    url: str
    username: Optional[str] = None
    password: Optional[str] = None

    class Config:
        env_prefix = "FLUVII_SCHEMA_REGISTRY_"
