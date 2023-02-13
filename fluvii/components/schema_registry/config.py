from pydantic import SecretStr
from fluvii.config_bases import FluviiConfigBase
from typing import Optional


class SchemaRegistryConfig(FluviiConfigBase):
    """
    Common configs, along with some custom ones, that likely wont need to be changed from their defaults.
    """
    # client settings
    url: str
    username: Optional[str] = None
    password: Optional[SecretStr] = None

    class Config(FluviiConfigBase.Config):
        env_prefix = "FLUVII_SCHEMA_REGISTRY_"
