from pydantic import BaseSettings
from fluvii.config_base import KafkaConfigBase


class ProducerConfig(KafkaConfigBase, BaseSettings):
    """
    Common configs, along with some custom ones, that likely wont need to be changed from their defaults.
    """
    # client settings
    urls = str
    transaction_timeout_minutes: int = 1

    # fluvii settings
    schema_library_root: str = ''

    class Config:
        env_prefix = "FLUVII_PRODUCER_"

    def as_client_dict(self):
        return {
            "bootstrap.servers": self.urls,
            "transaction.timeout.ms": self.transaction_timeout_minutes * 60_000,
        }
