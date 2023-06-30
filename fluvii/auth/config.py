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


class SaslPlainClientConfig(KafkaConfigBase):
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


class SaslOauthClientConfig(KafkaConfigBase):
    def __init__(self, username, password, url, scope):
        self.username = username
        self.password = password
        self.url = url
        self.scope = scope

    def _get_token(self, required_arg):
        """required_arg is...well, required. Was easier to set it up without using it (basically
        is just passed whatever you set sasl.oauthbearer.config to...(on the client, I'm assuming?))"""
        payload = {
            'grant_type': 'client_credentials',
            'scope': self.scope
        }
        resp = requests.post(self.url,
                             auth=(self.username, self.password),
                             data=payload)
        token = resp.json()
        return token['access_token'], time.time() + float(token['expires_in'])

    def as_client_dict(self):
        return {
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'OAUTHBEARER',
            'oauth_cb': self._get_token,
        }
