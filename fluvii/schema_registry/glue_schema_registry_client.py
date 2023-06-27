import logging

import boto3
from botocore.exceptions import BotoCoreError, ClientError

LOGGER = logging.getLogger(__name__)


class GlueSchemaRegistryClient:
    def __init__(self, auth_config=None, auto_init=True):
        """
        Initialize the Glue Schema Registry client.
        """
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

    def create_schema(self, schema_name, data_format, schema_definition, compatibility='BACKWARD'):
        """
        Create a new schema in the Glue Schema Registry.

        Args:
            schema_name (str): Name of the schema.
            data_format (str): Data format of the schema, e.g., 'AVRO', 'JSON'.
            compatibility (str): Compatibility mode for the schema. Default is 'BACKWARD'.

        Returns:
            str: The version ID of the created schema.

        Raises:
            BotoCoreError: If a Boto Core error occurs.
            ClientError: If an error occurs in the AWS Glue client.
        """

        try:
            response = self.client.create_schema(
                RegistryId={
                    'RegistryName': self._auth.registry_name,
                },
                SchemaName=schema_name,
                DataFormat=data_format,
                SchemaDefinition=schema_definition,
                Compatibility=compatibility
            )
            LOGGER.info(response)
            return response['SchemaVersionId']
        except (BotoCoreError, ClientError) as e:
            LOGGER.debug(f"An error occurred while creating schema: {e}")
            return None

    def get_schema_version(self, schema_name, schema_version_id):
        """
        Get a specific version of a schema from the Glue Schema Registry.

        Args:
            schema_name (str): Name of the schema.
            schema_version_id (str): ID of the schema version.

        Returns:
            dict: The schema version information.

        Raises:
            BotoCoreError: If a Boto Core error occurs.
            ClientError: If an error occurs in the AWS Glue client.
        """
        try:
            response = self.client.get_schema_version(
                SchemaId={
                    'SchemaName': schema_name,
                    'RegistryName': self._auth.registry_name
                },
                SchemaVersionId=schema_version_id
            )
            return response['SchemaVersion']
        except (BotoCoreError, ClientError) as e:
            LOGGER.debug(f"An error occurred while retrieving schema version: {e}")
            return None

    def update_schema(self, schema_name, schema_definition, schema_version_id):
        """
        Update an existing schema in the Glue Schema Registry.

        Args:
            schema_name (str): Name of the schema.
            schema_definition (dict): Definition of the updated schema.
            schema_version_id (str): ID of the schema version to update.

        Returns:
            str: The version ID of the updated schema.

        Raises:
            BotoCoreError: If a Boto Core error occurs.
            ClientError: If an error occurs in the AWS Glue client.
        """
        try:
            response = self.client.update_schema(
                SchemaId={
                    'SchemaName': schema_name,
                    'RegistryName': self._auth.registry_name
                },
                SchemaVersionId=schema_version_id,
                SchemaDefinition=schema_definition
            )
            return response['SchemaVersionId']
        except (BotoCoreError, ClientError) as e:
            LOGGER.debug(f"An error occurred while updating schema: {e}")
            return None

    def delete_schema(self, schema_name):
        """
        Delete a schema from the Glue Schema Registry.

        Args:
            schema_name (str): Name of the schema.

        Raises:
            BotoCoreError: If a Boto Core error occurs.
            ClientError: If an error occurs in the AWS Glue client.
        """
        try:
            self.client.delete_schema(
                SchemaId={
                    'SchemaName': schema_name,
                    'RegistryName': self._auth.registry_name
                }
            )
        except (BotoCoreError, ClientError) as e:
            LOGGER.debug(f"An error occurred while deleting schema: {e}")
