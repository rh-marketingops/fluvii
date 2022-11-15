from fluvii.producer import ProducerConfig


def test_default_producer_config():
    producer_config = ProducerConfig()
    assert producer_config.as_client_dict() == {'transaction.timeout.ms': 60000}
