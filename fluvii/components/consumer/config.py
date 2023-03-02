from typing import Literal, Optional
from pydantic import validator
from fluvii.config_bases import KafkaConfigBase, FluviiConfigBase
from fluvii.components.auth import AuthKafkaConfig, get_auth_kafka_config


class ConsumerConfig(KafkaConfigBase, FluviiConfigBase):
    """
    Configs that are unlikely to change between different consumer instances.
    """
    # related configs
    auth_config: Optional[AuthKafkaConfig] = get_auth_kafka_config()

    # client settings
    urls: str
    auto_commit_interval_seconds: int = 20
    auto_offset_reset: Literal["earliest", "latest"] = "latest"
    timeout_minutes: int = 4
    heartbeat_timeout_seconds: Optional[int] = None
    message_singleton_max_mb: int = 2
    message_batch_max_mb: int = 5
    message_queue_max_mb: int = 20

    # fluvii settings
    poll_timeout_seconds: int = 5
    batch_consume_max_count: Optional[int] = 100
    batch_consume_max_empty_polls: Optional[int] = 2
    batch_consume_max_time_seconds: Optional[int] = 10
    batch_consume_trigger_message_age_seconds: int = 5
    batch_consume_store_messages: bool = False

    @validator('heartbeat_timeout_seconds')
    def set_heartbeat_timeout(cls, value, values):
        if not value:
            return (values["timeout_minutes"] * 60) // 2
        return value

    class Config(FluviiConfigBase.Config):
        env_prefix = "FLUVII_CONSUMER_"

    def as_client_dict(self):
        _auth = self.auth_config.as_client_dict() if self.auth_config else {}
        ms_tolerance = 1_000
        return {
            "bootstrap.servers": self.urls,
            **_auth,
            "auto.commit.interval.ms": self.auto_commit_interval_seconds * 1_000,
            "auto.offset.reset": self.auto_offset_reset,
            "fetch.max.bytes": self.message_batch_max_mb * (2 ** 20),
            "heartbeat.interval.ms": ((self.heartbeat_timeout_seconds // 5) * 1_000) - ms_tolerance,  # 5 failed heartbeats == bad consumer.
            "max.poll.interval.ms": self.timeout_minutes * 60_000,  # Max time between poll() calls before considered dead.
            "message.max.bytes": self.message_singleton_max_mb * (2 ** 20),
            "queued.max.messages.kbytes": self.message_queue_max_mb * (2 ** 10),
            "session.timeout.ms": self.heartbeat_timeout_seconds * 1_000,  # need at least 1 heartbeat within "session" time to be considered alive;
        }
