from pydantic import BaseSettings


class SchemaRegistryConfig(BaseSettings):
    """
    Common configs, along with some custom ones, that likely wont need to be changed from their defaults.
    """
    # client settings
    url = str

    class Config:
        env_prefix = "FLUVII_SCHEMA_REGISTRY_"
