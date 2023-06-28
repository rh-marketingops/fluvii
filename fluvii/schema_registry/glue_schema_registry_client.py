import logging
import mmh3
import boto3
import uuid
from botocore.exceptions import BotoCoreError, ClientError
from confluent_kafka.schema_registry import RegisteredSchema
LOGGER = logging.getLogger(__name__)

def getUUIDtoInt(schemaId) :
    schema_id = uuid.UUID(schemaId)
    return schema_id.int

class EntityNotFoundException(Exception):
    def __init__(self, entity_type, entity_name):
        self.entity_type = entity_type
        self.entity_name = entity_name
        message = f"{entity_type} '{entity_name}' not found"
        super().__init__(message)

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

    def register_schema(self, schema_name, schema, normalize_schemas=False):
        """
        Registers a schema under ``subject_name``.

        Args:
            subject_name (str): subject to register a schema under

            schema (Schema): Schema instance to register

        Returns:
            int: Schema id

        Raises:
            SchemaRegistryError: if Schema violates this subject's
                Compatibility policy or is otherwise invalid.

        """  # noqa: E501
        try:
            schemas = self.client.list_schema_versions(SchemaId={
                    'RegistryName': self._auth.registry_name, 
                    'SchemaName': schema_name
            })
            schema = schemas['Schemas'][0]
            return getUUIDtoInt(schema['SchemaVersionId'])
        except self.client.exceptions.EntityNotFoundException:
            try:
                #Check if schema already exists first
                response = self.client.create_schema(
                    RegistryId={
                        'RegistryName': self._auth.registry_name,
                    },
                    SchemaName=schema_name,
                    DataFormat='AVRO',
                    SchemaDefinition=schema.schema_str,
                    Compatibility='BACKWARD'
                )
                #LOGGER.info(response)
                #return [11, 22, 33]
                schema_id = uuid.UUID(response['SchemaVersionId'])

                return getUUIDtoInt(response['SchemaVersionId']);
            except (BotoCoreError, ClientError) as e:
                print(e)
                LOGGER.debug(f"An error occurred while creating schema: {e}")
                return None
        


    def get_latest_version(self, schema_name, schema_version_id, registry_name):
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
                    'RegistryName': registry_name, 
                    'SchemaName': schema_name
                },
                #SchemaVersionId=schema_version_id, 
                SchemaVersionNumber={
                    'LatestVersion': True
                }
            )
            schema_id = uuid.UUID(response['SchemaVersionId'])
            return RegisteredSchema(schema.int, response['SchemaDefinition'], registry_name, response['VersionNumber']);

        except (BotoCoreError, ClientError) as e:
            LOGGER.debug(f"An error occurred while retrieving schema version: {e}")
            return None
        
    def update_schema_definition(self, schema_name, schema_definition): 
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
            response = self.client.register_schema_version(
                SchemaId={
                    'SchemaName': schema_name,
                    'RegistryName': self._auth.registry_name
                },
                #SchemaVersionId=schema_version_id,
                SchemaDefinition=schema_definition
            )
            return response
            return response['SchemaVersionId']
        except (BotoCoreError, ClientError) as e:
            LOGGER.debug(f"An error occurred while updating schema: {e}")
            return e
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
                SchemaDefinition=schema_definition
            )
            return response['SchemaVersionId']
        except (BotoCoreError, ClientError) as e:
            LOGGER.debug(f"An error occurred while updating schema: {e}")
            return e
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
