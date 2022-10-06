from fluvii.config_base import KafkaConfigBase


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
