from typing import Literal, Optional
from pydantic import BaseSettings
from fluvii.config_base import KafkaConfigBase
import requests
import time


class AuthKafkaConfig(KafkaConfigBase, BaseSettings):
    username: str
    password: str
    oauth_url: Optional[str] = None
    oauth_scope: Optional[str] = None
    mechanisms: Literal['PLAIN', 'OAUTHBEARER'] = 'OAUTHBEARER' if oauth_url else 'PLAIN'
    protocol: Literal['SASL_SSL'] = 'SASL_SSL'

    class Config:
        env_prefix = "FLUVII_AUTH_KAFKA_"

    def _get_oauth_token(self, required_arg):
        """required_arg is...well, required. Was easier to set it up without using it (basically
        is just passed whatever you set sasl.oauthbearer.config to...(on the client, I'm assuming?))"""
        payload = {
            'grant_type': 'client_credentials',
            'scope': self.oauth_scope
        }
        resp = requests.post(
            self.oauth_url,
            auth=(self.username, self.password),
            data=payload)
        token = resp.json()
        return token['access_token'], time.time() + float(token['expires_in'])

    def as_client_dict(self):
        client_dict = {}
        if self.username:
            client_dict.update({
                'security.protocol': self.protocol,
                'sasl.mechanisms': self.mechanisms,
            })
            if self.oauth_url:
                client_dict.update({'oauth_cb': self._get_oauth_token})
            else:
                client_dict.update({'username': self.username, 'password': self.password})
        return client_dict


def get_auth_kafka_config():
    try:
        return AuthKafkaConfig()
    except:
        return None
