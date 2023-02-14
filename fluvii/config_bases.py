from abc import ABC, abstractmethod
from pydantic import BaseSettings
from pprint import pformat
import logging
from os import environ

LOGGER = logging.getLogger(__name__)


class KafkaConfigBase(ABC):
    """
    Manages the connection configuration for a Kafka client

    The options that need to be defined for a kafka client differ depending
    on which security protocol is being used
    """
    @abstractmethod
    def as_client_dict(self):
        pass


class FluviiConfigBase(BaseSettings):
    class Config:
        env_file = environ.get('FLUVII_CONFIG_DOTENV', None)

    def __str__(self):
        return pformat(self.dict(), indent=4)
