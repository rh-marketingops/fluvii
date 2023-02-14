from typing import Optional
from fluvii.config_bases import KafkaConfigBase, FluviiConfigBase
from fluvii.components.auth import AuthKafkaConfig, get_auth_kafka_config


class ProducerConfig(KafkaConfigBase, FluviiConfigBase):
    """
    Common configs, along with some custom ones, that likely wont need to be changed from their defaults.
    """
    # related configs
    auth_config: Optional[AuthKafkaConfig] = get_auth_kafka_config()

    # client settings
    urls: str
    transaction_timeout_minutes: int = 1

    # fluvii settings
    schema_library_root: Optional[str] = None

    class Config(FluviiConfigBase.Config):
        env_prefix = "FLUVII_PRODUCER_"

    def as_client_dict(self):
        _auth = self.auth_config.as_client_dict() if self.auth_config else {}
        return {
            "bootstrap.servers": self.urls,
            "transaction.timeout.ms": self.transaction_timeout_minutes * 60_000,
            **_auth,
        }
