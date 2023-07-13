import logging
import uuid

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from confluent_kafka.schema_registry import RegisteredSchema, Schema

LOGGER = logging.getLogger(__name__)


def get_uuid_to_int(schema_id):
    return uuid.UUID(schema_id).int


def get_int_to_uuid(schema_int):
    return uuid.UUID(int=schema_int)


class EntityNotFoundException(Exception):
    def __init__(self, entity_type, entity_name):
        self.entity_type = entity_type
        self.entity_name = entity_name
        message = f"{entity_type} '{entity_name}' not found"
        super().__init__(message)


class GlueSchemaRegistryClient:
    def __init__(self, auth_config=None, auto_init=True):
        self._auth = auth_config
        if auto_init:
            self._init_registry()

    def _init_registry(self):
        session = boto3.Session(region_name=self._auth.region_name,
                                aws_access_key_id=self._auth.aws_access_key_id,
                                aws_secret_access_key=self._auth.aws_secret_access_key)
        self.client = session.client('glue')
        LOGGER.info('Registry client initialized successfully!')

    def get_registry_id(self, registry_name):
        response = self.client.get_registry(RegistryName=registry_name)
        registry_id = response['RegistryId']
        return registry_id

    def register_schema(self, schema_name, schema, normalize_schemas=False):
        try:
            schemas = self.client.list_schema_versions(SchemaId={
                    'RegistryName': self._auth.registry_name, 
                    'SchemaName': schema_name
            })
            schema = schemas['Schemas'][0]
            return get_uuid_to_int(schema['SchemaVersionId'])
        except self.client.exceptions.EntityNotFoundException:
            try:
                schema = self.client.create_schema(
                    RegistryId={
                        'RegistryName': self._auth.registry_name,
                    },
                    SchemaName=schema_name,
                    DataFormat='AVRO',
                    SchemaDefinition=schema.schema_str,
                    Compatibility='BACKWARD'
                )
                return get_uuid_to_int(schema['SchemaVersionId'])
            except (BotoCoreError, ClientError) as e:
                LOGGER.debug(f"An error occurred while creating schema: {e}")
                return None
        
    def get_schema(self, schema_id):
        schema_id_decode = str(get_int_to_uuid(schema_id))
        schema = self.client.get_schema_version(SchemaVersionId=schema_id_decode)
        return Schema(schema_str=schema['SchemaDefinition'], schema_type='AVRO')

    def update_schema_definition(self, schema_name, schema_definition):
        try:
            response = self.client.register_schema_version(
                SchemaId={
                    'SchemaName': schema_name,
                    'RegistryName': self._auth.registry_name
                },
                SchemaDefinition=schema_definition
            )
            return response
        except (BotoCoreError, ClientError) as e:
            LOGGER.debug(f"An error occurred while updating schema: {e}")
            return e

    def update_schema(self, schema_name, schema_definition, schema_version_id):
        try:
            response = self.client.update_schema(
                SchemaId={
                    'SchemaName': schema_name,
                    'RegistryName': self._auth.registry_name
                },
                SchemaDefinition=schema_definition
            )
            return response['SchemaVersionId']
        except (BotoCoreError, ClientError) as e:
            LOGGER.debug(f"An error occurred while updating schema: {e}")
            return e

    def delete_schema(self, schema_name):
        try:
            self.client.delete_schema(
                SchemaId={
                    'SchemaName': schema_name,
                    'RegistryName': self._auth.registry_name
                }
            )
        except (BotoCoreError, ClientError) as e:
            LOGGER.debug(f"An error occurred while deleting schema: {e}")
