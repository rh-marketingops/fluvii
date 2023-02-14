from typing import Optional
from fluvii.config_bases import KafkaConfigBase, FluviiConfigBase
from fluvii.components.auth import AuthKafkaConfig, get_auth_kafka_config


class AdminConfig(KafkaConfigBase, FluviiConfigBase):
    """
    Common configs, along with some custom ones, that likely wont need to be changed from their defaults.
    """
    # related configs
    auth_config: Optional[AuthKafkaConfig] = get_auth_kafka_config(oauth_url=None)  # AdminClient cannot do oauth

    # client settings
    urls: str

    class Config(FluviiConfigBase.Config):
        env_prefix = "FLUVII_ADMIN_"

    def as_client_dict(self):
        _auth = self.auth_config.as_client_dict() if self.auth_config else {}
        return {"bootstrap.servers": self.urls, **_auth}
