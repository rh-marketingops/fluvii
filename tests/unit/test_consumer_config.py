import os
from unittest.mock import patch

from fluvii.consumer import ConsumerConfig

def test_everything_has_a_default():
    config = ConsumerConfig()
    assert config.as_client_dict() == {
        'auto.commit.interval.ms': 20_000,
        'auto.offset.reset': 'latest',
        'fetch.max.bytes': 2_097_152,
        'heartbeat.interval.ms': 23_000,
        'max.poll.interval.ms': 240_000,
        'message.max.bytes': 5_242_880,
        'queued.max.messages.kbytes': 20_480,
        'session.timeout.ms': 120_000,
    }

def test_environment_variables_can_override():
    with patch.dict(os.environ, {
            "FLUVII_CONSUMER_AUTO_COMMIT_INTERVAL_SECONDS": "50",
            "FLUVII_CONSUMER_BATCH_CONSUME_MAX_TIME_SECONDS": "30",
    }):
        config = ConsumerConfig()
    cd = config.as_client_dict()
    assert cd["auto.commit.interval.ms"] == 50_000
    assert config.batch_consume_max_time_seconds == 30

def test_calculated_properties_are_overridable():
    config = ConsumerConfig()
    assert config.heartbeat_timeout_ms == 120_000
    config.heartbeat_timeout_ms = 40_000
    assert config.heartbeat_timeout_ms == 40_000
