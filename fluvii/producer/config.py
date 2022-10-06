from fluvii.config_base import KafkaConfigBase
from os import environ


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
