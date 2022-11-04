from fluvii.producer import ProducerConfig
from fluvii.consumer import ConsumerConfig
from fluvii.auth import SaslPlainClientConfig, SaslOauthClientConfig
from fluvii.metrics import MetricsManagerConfig, MetricsPusherConfig
from os import environ
from datetime import datetime
import logging

LOGGER = logging.getLogger(__name__)


class FluviiConfig:
    """
    Manages configuration setup for FLUVII applications

    Generally, is getting things from environment variables.
    Will default to values for certain optional variables,
    and raise on exception for variables that are required.
    """
    def __init__(self,
                 client_urls=None, schema_registry_url=None, client_auth_config=None, schema_registry_auth_config=None,
                 producer_config=None, consumer_config=None, metrics_manager_config=None, metrics_pusher_config=None):
        self._time = int(datetime.timestamp(datetime.now()))

        # Set via properties
        self._hostname = None
        self._table_changelog_topic = None

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
                if environ.get("FLUVII_OAUTH_URL"):
                    LOGGER.info('Kafka clients will be initialized with OAUTH authentication')
                    client_auth_config = SaslOauthClientConfig(
                        environ["FLUVII_CLIENT_USERNAME"],
                        environ["FLUVII_CLIENT_PASSWORD"],
                        environ["FLUVII_OAUTH_URL"],
                        environ["FLUVII_OAUTH_SCOPE"])
                else:
                    LOGGER.info('Kafka clients will be initialized with plain authentication')
                    client_auth_config = SaslPlainClientConfig(
                        environ["FLUVII_CLIENT_USERNAME"],
                        environ["FLUVII_CLIENT_PASSWORD"])
        if not schema_registry_auth_config:
            if environ.get("FLUVII_SCHEMA_REGISTRY_USERNAME"):
                schema_registry_auth_config = SaslPlainClientConfig(
                    environ["FLUVII_SCHEMA_REGISTRY_USERNAME"],
                    environ["FLUVII_SCHEMA_REGISTRY_PASSWORD"])
        self.client_auth_config = client_auth_config
        self.schema_registry_auth_config = schema_registry_auth_config

        # Everything else with defaults
        if not producer_config:
            producer_config = ProducerConfig()
        if not consumer_config:
            consumer_config = ConsumerConfig()

        self.app_name = environ.get("FLUVII_APP_NAME", 'fluvii_app')
        self.table_folder_path = environ.get('FLUVII_TABLE_FOLDER_PATH', '/tmp')
        self.table_recovery_multiplier = int(environ.get('FLUVII_TABLE_RECOVERY_MULTIPLIER', '10'))
        self.loglevel = environ.get('FLUVII_LOGLEVEL', 'INFO')

        self._hostname = None
        self._table_changelog_topic = None

        # TODO: come up with a way to handle config settings that overlap multiple config objects, like w/metrics configs
        if not metrics_manager_config:
            metrics_manager_config = MetricsManagerConfig(hostname=self.hostname, app_name=self.app_name)
        if not metrics_pusher_config:
            metrics_pusher_config = MetricsPusherConfig(hostname=self.hostname)
        self.consumer_config = consumer_config
        self.producer_config = producer_config
        self.metrics_manager_config = metrics_manager_config
        self.metrics_pusher_config = metrics_pusher_config

    # These properties allow settings that depend on others to dynamically adjust their defaults
    @property
    def hostname(self):
        if self._hostname:
            return self._hostname
        return environ.get('FLUVII_HOSTNAME', f'{self.app_name}_{self._time}')

    @hostname.setter
    def hostname(self, value):
        self._hostname = value

    @property
    def table_changelog_topic(self):
        if self._table_changelog_topic:
            return self._table_changelog_topic
        return environ.get('FLUVII_TABLE_CHANGELOG_TOPIC', f'{self.app_name}__changelog')

    @table_changelog_topic.setter
    def table_changelog_topic(self, value):
        self._table_changelog_topic = value
