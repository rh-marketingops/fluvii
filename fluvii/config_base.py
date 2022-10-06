from abc import ABC, abstractmethod


class KafkaConfigBase(ABC):
    """
    Manages the connection configuration for a Kafka client

    The options that need to be defined for a kafka client differ depending
    on which security protocol is being used
    """

    @abstractmethod
    def as_client_dict(self):
        pass
