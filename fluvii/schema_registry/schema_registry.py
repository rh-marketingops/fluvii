import boto3
from aws_schema_registry import SchemaRegistryClient

import logging

LOGGER = logging.getLogger(__name__)


class SchemaRegistry:
    def __init__(self, auth_config=None, auto_init=True):
        self.registry = None
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
        # Pass your AWS credentials or profile information here
        session = boto3.Session(aws_access_key_id=self._auth.aws_access_key_id,
                                aws_secret_access_key=self._auth.aws_secret_access_key,
                                region_name=self._auth.region_name)
        glue_client = session.client('glue')
        self.registry = SchemaRegistryClient(glue_client,
                                             registry_name=self._auth.registry_name)
        LOGGER.info('Registry client initialized successfully!')
