from fluvii.config_base import KafkaConfigBase
import requests
import time


class SaslPlainClientConfig(KafkaConfigBase):
    def __init__(self, username, password):
        self.security_protocol = 'sasl_ssl'
        self.mechanism = 'PLAIN'
        self.username = username
        self.password = password

    def as_client_dict(self):
        return {
            "security.protocol": self.security_protocol,
            "sasl.mechanism": self.mechanism,
            "sasl.username": self.username,
            "sasl.password": self.password,
        }


class SaslOauthClientConfig(KafkaConfigBase):
    def __init__(self, username, password, url, scope):
        self.security_protocol = 'sasl_ssl'
        self.mechanism = 'OUATHBEARER'
        self.username = username
        self.password = password
        self.url = url
        self.scope = scope

    def _get_token(self):
        payload = {
            'grant_type': 'client_credentials',
            'scope': ' '.join(self.scope)
        }
        resp = requests.post(self.url,
                             auth=(self.username, self.password),
                             data=payload)
        token = resp.json()
        return token['access_token'], time.time() + float(token['expires_in'])

    def as_client_dict(self):
        return {
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'OAUTHBEARER',
            'oauth_cb': self._get_token,
        }
