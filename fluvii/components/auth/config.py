from typing import Literal, Optional
from fluvii.config_bases import KafkaConfigBase, FluviiConfigBase
from pydantic import SecretStr, validator
import requests
import time


class AuthKafkaConfig(KafkaConfigBase, FluviiConfigBase):
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    oauth_url: Optional[str] = None
    oauth_scope: Optional[str] = None
    mechanisms: Literal['PLAIN', 'OAUTHBEARER'] = 'OAUTHBEARER' if oauth_url else 'PLAIN'
    protocol: Literal['SASL_SSL'] = 'SASL_SSL'

    class Config(FluviiConfigBase.Config):
        env_prefix = "FLUVII_AUTH_KAFKA_"

    @validator('mechanisms')
    def set_hostname(cls, value, values):
        if values["oauth_url"]:
            return 'OAUTHBEARER'
        return 'PLAIN'

    def _get_oauth_token(self, required_arg):
        """required_arg is...well, required. Was easier to set it up without using it (basically
        is just passed whatever you set sasl.oauthbearer.config to...(on the client, I'm assuming?))"""
        payload = {
            'grant_type': 'client_credentials',
            'scope': self.oauth_scope
        }
        resp = requests.post(
            self.oauth_url,
            auth=(self.username, self.password.get_secret_value()),
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
                client_dict.update({'sasl.username': self.username, 'sasl.password': self.password.get_secret_value()})
        return client_dict


def get_auth_kafka_config(**kwargs):
    try:
        return AuthKafkaConfig(**kwargs)
    except:
        return None
