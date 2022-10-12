from fluvii.producer import ProducerConfig
from fluvii.consumer import ConsumerConfig
from fluvii.auth import SaslPlainClientConfig
from os import environ
from datetime import datetime


class FluviiConfig:
    """
    Manages configuration setup for FLUVII applications

    Generally, is getting things from environment variables.
    Will default to values for certain optional variables,
    and raise on exception for variables that are required.
    """
    def __init__(self, client_urls=None, schema_registry_url=None,
                 client_auth_config=None, schema_registry_auth_config=None, producer_config=None, consumer_config=None):
        # Required vars
        if not client_urls:
            client_urls = environ['FLUVII_KAFKA_BOOTSTRAP_SERVERS']
        if not schema_registry_url:
            schema_registry_url = environ['FLUVII_SCHEMA_REGISTRY_URL']
        self.client_urls = client_urls
        self.schema_registry_url = schema_registry_url

        # Set only if env var or object is specified
        if not client_auth_config:
            if environ.get("FLUVII_CLIENT_USERNAME"):
                client_auth_config = SaslPlainClientConfig(environ.get("FLUVII_CLIENT_USERNAME"), environ.get("FLUVII_CLIENT_PASSWORD"))
        if not schema_registry_auth_config:
            if environ.get("FLUVII_SCHEMA_REGISTRY_USERNAME"):
                schema_registry_auth_config = SaslPlainClientConfig(environ.get("FLUVII_SCHEMA_REGISTRY_USERNAME"), environ.get("FLUVII_SCHEMA_REGISTRY_PASSWORD"))
        self.client_auth_config = client_auth_config
        self.schema_registry_auth_config = schema_registry_auth_config

        # Everything else with defaults
        if not producer_config:
            producer_config = ProducerConfig()
        if not consumer_config:
            consumer_config = ConsumerConfig()
        self.consumer_config = consumer_config
        self.producer_config = producer_config
        self.app_name = environ.get("FLUVII_APP_NAME", 'fluvii_app')
        self.hostname = environ.get('FLUVII_HOSTNAME', f'{self.app_name}_{int(datetime.timestamp(datetime.now()))}')
        self.table_folder_path = environ.get('FLUVII_TABLE_FOLDER_PATH', '/tmp')
        self.table_changelog_topic = environ.get('FLUVII_TABLE_CHANGELOG_TOPIC', f'{self.app_name}__changelog')
        self.table_recovery_multiplier = int(environ.get('FLUVII_TABLE_RECOVERY_MULTIPLIER', '10'))
        self.loglevel = environ.get('FLUVII_LOGLEVEL', 'INFO')
        self.enable_metrics_pushing = True if environ.get('FLUVII_ENABLE_METRICS_PUSHING', 'false').lower() == 'true' else False
