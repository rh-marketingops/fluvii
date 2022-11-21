from os import environ
from typing import Literal, Optional

from pydantic import BaseSettings, Field

from fluvii.config_base import KafkaConfigBase


class ConsumerConfig(KafkaConfigBase, BaseSettings):
    """
    Common configs, along with some custom ones, that likely wont need to be changed from their defaults.
    """
    auto_commit_interval_seconds: int = 20
    auto_offset_reset: Literal["earliest", "latest"] = "latest"
    batch_consume_max_count: int = 100
    batch_consume_max_empty_polls: Optional[int] = 2
    batch_consume_max_time_seconds: Optional[int] = 10
    batch_consume_trigger_message_age_seconds: int = 5
    batch_consume_store_messages: bool = False
    heartbeat_timeout_ms: int = 4 * 60 // 2 * 1_000  # TODO document that this pairs with timeout_minutes
    message_singleton_max_mb: int = 2
    message_batch_max_mb: int = 5
    message_queue_max_mb: int = 20
    poll_timeout_seconds: int = 5
    timeout_minutes: int = 4  # TODO document that this pairs with heartbeat_timeout_ms

    class Config:
        env_prefix = "FLUVII_CONSUMER_"

    def as_client_dict(self):
        ms_tolerance = 1_000
        return {
            "auto.commit.interval.ms": self.auto_commit_interval_seconds * 1_000,
            "auto.offset.reset": self.auto_offset_reset,
            "fetch.max.bytes": self.message_batch_max_mb * (2 ** 20),
            "heartbeat.interval.ms": (self.heartbeat_timeout_ms // 5) - ms_tolerance,  # 5 failed heartbeats == bad consumer.
            "max.poll.interval.ms": self.timeout_minutes * 60_000,  # Max time between poll() calls before considered dead.
            "message.max.bytes": self.message_singleton_max_mb * (2 ** 20),
            "queued.max.messages.kbytes": self.message_queue_max_mb * (2 ** 10),
            "session.timeout.ms": self.heartbeat_timeout_ms,  # need at least 1 heartbeat within "session" time to be considered alive;
        }
