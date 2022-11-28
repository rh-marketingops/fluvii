from confluent_kafka.schema_registry import SchemaRegistryClient
from urllib.parse import urlparse


# fixes a bug in confluent-kafka TODO: keep a look out for this in >1.8.2, should be fixed since I took it from a MR.
def patched_schema_loads(schema_str):
    from confluent_kafka.schema_registry.avro import Schema
    schema_str = schema_str.strip()
    if schema_str[0] != "{" and schema_str[0] != "[":
        schema_str = '{"type":' + schema_str + '}'
    return Schema(schema_str, schema_type='AVRO')


import confluent_kafka
confluent_kafka.schema_registry.avro._schema_loads = patched_schema_loads
import logging

LOGGER = logging.getLogger(__name__)


class SchemaRegistry:
    def __init__(self, url, auth_config=None, auto_init=True):
        self.registry = None
        self.url = url
        self._auth = auth_config
        if auto_init:
            self._init_registry()

    def __getattr__(self, attr):
        """Note: this includes methods as well!"""
        try:
            return self.__getattribute__(attr)
        except AttributeError:
            return self.registry.__getattribute__(attr)

    def _init_registry(self):
        url = urlparse(self.url)
        if self._auth:
            auth = f"{self._auth.username}:{self._auth.password}@"
        else:
            auth = ''
        scheme = url.scheme
        if not scheme:
            if auth:
                scheme = 'https://'
            else:
                scheme = 'http://'
            url = url._replace(scheme='')
        self.registry = SchemaRegistryClient({'url': f'{scheme}{auth}{url.geturl()}'})
        LOGGER.info('Registry client initialized successfully!')
