import os
from unittest.mock import patch

from fluvii.components import ConsumerConfig


def test_everything_has_a_default():
    config = ConsumerConfig(urls='urls')
    assert config.as_client_dict() == {
        "bootstrap.servers": 'urls',
        "auto.commit.interval.ms": 20_000,
        "auto.offset.reset": "latest",
        "fetch.max.bytes": 5_242_880,
        "heartbeat.interval.ms": 23_000,
        "max.poll.interval.ms": 240_000,
        "message.max.bytes": 2_097_152,
        "queued.max.messages.kbytes": 20_480,
        "session.timeout.ms": 120_000,
    }


def test_environment_variables_can_override():
    with patch.dict(
        os.environ,
        {
            "FLUVII_CONSUMER_AUTO_COMMIT_INTERVAL_SECONDS": "50",
            "FLUVII_CONSUMER_BATCH_CONSUME_MAX_TIME_SECONDS": "30",
        },
    ):
        config = ConsumerConfig(urls='')
    cd = config.as_client_dict()
    assert cd["auto.commit.interval.ms"] == 50_000
    assert config.batch_consume_max_time_seconds == 30


def test_calculated_properties_are_overridable():
    config = ConsumerConfig(urls='')
    assert config.heartbeat_timeout_seconds == 120
    config.heartbeat_timeout_seconds = 40
    assert config.heartbeat_timeout_seconds == 40
