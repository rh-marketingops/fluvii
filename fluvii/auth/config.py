from os import environ
from typing import Literal, Optional
from pydantic import BaseSettings, Field
from fluvii.config_base import KafkaConfigBase

import requests
import time


class SaslPlainClientConfig(KafkaConfigBase, BaseSettings):
    username: str
    password: str

    class Config:
        env_prefix = "FLUVII_AUTH_PLAIN_"

    def as_client_dict(self):
        return {
            "security.protocol": 'SASL_SSL',
            "sasl.mechanisms": 'PLAIN',
            "sasl.username": self.username,
            "sasl.password": self.password,
        }


class SaslOauthClientConfig(KafkaConfigBase, BaseSettings):
    def __init__(self, username, password, url, scope):
        username: str
        password: str
        url: str
        scope: str

    class Config:
        env_prefix = "FLUVII_AUTH_OUATH_"

    def _get_token(self, required_arg):
        """required_arg is...well, required. Was easier to set it up without using it (basically
        is just passed whatever you set sasl.oauthbearer.config to...(on the client, I'm assuming?))"""
        payload = {
            'grant_type': 'client_credentials',
            'scope': self.scope
        }
        resp = requests.post(
            self.url,
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
