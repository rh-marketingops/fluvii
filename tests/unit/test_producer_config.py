from fluvii.components.producer import ProducerConfig


def test_default_producer_config():
    producer_config = ProducerConfig(urls='urls')
    assert producer_config.as_client_dict() == {
        "bootstrap.servers": 'urls',
        'transaction.timeout.ms': 60000}
