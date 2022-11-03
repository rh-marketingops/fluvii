from typing import Any, Dict

import pytest

from fluvii.consumer import Consumer
from fluvii.exceptions import NoMessageError


class MockDeserializingConsumer:
    def __init__(self, consumer_config: Dict[Any, Any]):
        pass

    def subscribe(self, topics):
        pass

    def poll(self, timeout):
        pass


def test_consumer_can_init():
    Consumer([], "", [], auto_subscribe=False)


def test_more_complex_init_is_okay_too():
    Consumer([], "", [], auto_subscribe=True, consumer_cls=MockDeserializingConsumer)


class TestConsumer:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.consumer = Consumer([], "", [], auto_subscribe=True, consumer_cls=MockDeserializingConsumer)

    def test_raises_error_when_consume_returns_no_messages(self):
        with pytest.raises(NoMessageError):
            self.consumer.consume()
