from fluvii.config_base import KafkaConfigBase
import requests
import time


class GlueRegistryClientConfig(KafkaConfigBase):
    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name, registry_name):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.registry_name = registry_name

    def as_client_dict(self):
        return {
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "region_name": self.region_name,
            "registry_name": self.registry_name,
        }


class SaslScramClientConfig(KafkaConfigBase):
    def __init__(self, username, password, mechanisms):
        self.username = username
        self.password = password
        self.mechanisms = mechanisms

    def as_client_dict(self):
        return {
            "security.protocol": 'SASL_SSL',
            "sasl.mechanisms": self.mechanisms,
            "sasl.username": self.username,
            "sasl.password": self.password,
        }
