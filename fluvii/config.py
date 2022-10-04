from os import environ
from abc import ABC, abstractmethod
from datetime import datetime


class KafkaConfigBase(ABC):
    """
    Manages the connection configuration for a Kafka client

    The options that need to be defined for a kafka client differ depending
    on which security protocol is being used
    """

    @abstractmethod
    def as_client_dict(self):
        pass


class SaslPlainClientConfig(KafkaConfigBase):
    def __init__(self, username, password):
        self.security_protocol = 'sasl_ssl'
        self.mechanism = 'PLAIN'
        self.username = username
        self.password = password

    def as_client_dict(self):
        return {
            "security.protocol": self.security_protocol,
            "sasl.mechanism": self.mechanism,
            "sasl.username": self.username,
            "sasl.password": self.password,
        }


class ConsumerConfig(KafkaConfigBase):
    """
    Common configs, along with some custom ones, that likely wont need to be changed from their defaults.
    """
    def __init__(self):
        self.auto_offset_reset = environ.get('FLUVII_CONSUMER_AUTO_OFFSET_RESET', 'latest')
        self.auto_commit_secs = int(environ.get('FLUVII_CONSUMER_AUTO_COMMIT_INTERVAL_SECONDS', '20')) * 1000

        self.timestamp_offset_mins = int(environ.get('FLUVII_CONSUMER_PROCESS_DELAY_MINUTES', '0'))  # for "retry" logic
        self._timeout_mins = int(environ.get('FLUVII_CONSUMER_TIMEOUT_MINUTES', '4'))
        self.timeout_mins = self._timeout_mins + self.timestamp_offset_mins
        self.heartbeat_timeout_ms = max(60, (self.timeout_mins // 60) // 2) * 1000

        self.message_max_size_mb = int(environ.get('FLUVII_CONSUMER_MESSAGE_BATCH_MAX_MB', '2'))
        self.message_batch_max_size_mb = int(environ.get('FLUVII_CONSUMER_MESSAGE_TOTAL_MAX_MB', '5'))
        self.message_queue_max_size_mb = int(environ.get('FLUVII_CONSUMER_MESSAGE_QUEUE_MAX_MB', '20'))  # as kilobytes

        # used in consumer.poll(), not actually passed as a config
        self.poll_timeout_secs = int(environ.get('FLUVII_CONSUMER_POLL_TIMEOUT_SECONDS', '8'))

        self.batch_consume_max_count = int(environ.get('FLUVII_CONSUMER_BATCH_CONSUME_MAX_COUNT', '100'))
        self.batch_consume_max_time_secs = int(environ.get('NU_CONSUMER_DEFAULT_BATCH_CONSUME_MAX_TIME_SECONDS', '10'))
        self.batch_consume_store_messages = False

    def as_client_dict(self):
        ms_tolerance = 1000
        return {
            "auto.offset.reset": self.auto_offset_reset,
            "auto.commit.interval.ms": self.auto_commit_secs * 1000,

            "max.poll.interval.ms": self.timeout_mins * 60000,  # Max time between poll() calls before considered dead.
            "session.timeout.ms": self.heartbeat_timeout_ms,  # need at least 1 heartbeat within "session" time to be considered alive;
            "heartbeat.interval.ms": (self.heartbeat_timeout_ms // 5) - ms_tolerance,  # 5 failed heartbeats == bad consumer.

            "message.max.bytes": self.message_max_size_mb * (2 ** 20),
            "fetch.max.bytes": self.message_batch_max_size_mb * (2 ** 20),
            "queued.max.messages.kbytes": self.message_queue_max_size_mb * (2 ** 10),
        }


class ProducerConfig(KafkaConfigBase):
    """
    Common configs, along with some custom ones, that likely wont need to be changed from their defaults.
    """
    def __init__(self):
        self.transaction_timeout_mins = int(environ.get('FLUVII_TRANSACTION_TIMEOUT_MINUTES', '1'))

    def as_client_dict(self):
        return {
            "transaction.timeout.ms": self.transaction_timeout_mins * 60000,
        }


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
        self.table_recovery_multiplier = environ.get('FLUVII_TABLE_RECOVERY_MULTIPLIER')
        self.loglevel = environ.get('FLUVII_LOGLEVEL', 'INFO')
        self.enable_metrics_pushing = True if environ.get('FLUVII_ENABLE_METRICS_PUSHING', 'false').lower() == 'true' else False
