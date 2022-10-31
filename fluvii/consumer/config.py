from fluvii.config_base import KafkaConfigBase
from os import environ


class ConsumerConfig(KafkaConfigBase):
    """
    Common configs, along with some custom ones, that likely wont need to be changed from their defaults.
    """
    def __init__(self):
        self.auto_offset_reset = environ.get('FLUVII_CONSUMER_AUTO_OFFSET_RESET', 'latest')
        self.auto_commit_secs = int(environ.get('FLUVII_CONSUMER_AUTO_COMMIT_INTERVAL_SECONDS', '20'))

        self.timestamp_offset_mins = int(environ.get('FLUVII_CONSUMER_PROCESS_DELAY_MINUTES', '0'))  # for "retry" logic
        self._timeout_mins = int(environ.get('FLUVII_CONSUMER_TIMEOUT_MINUTES', '4'))
        self.timeout_mins = self._timeout_mins + self.timestamp_offset_mins
        self.heartbeat_timeout_ms = max(30, (self.timeout_mins * 60) // 2) * 1000

        self.message_max_size_mb = int(environ.get('FLUVII_CONSUMER_MESSAGE_BATCH_MAX_MB', '2'))
        self.message_batch_max_size_mb = int(environ.get('FLUVII_CONSUMER_MESSAGE_TOTAL_MAX_MB', '5'))
        self.message_queue_max_size_mb = int(environ.get('FLUVII_CONSUMER_MESSAGE_QUEUE_MAX_MB', '20'))  # as kilobytes

        # used in consumer.poll(), not actually passed as a config
        self.poll_timeout_secs = int(environ.get('FLUVII_CONSUMER_POLL_TIMEOUT_SECONDS', '5'))

        self.batch_consume_max_count = int(environ.get('FLUVII_CONSUMER_BATCH_CONSUME_MAX_COUNT', '100'))
        self.batch_consume_max_time_secs = int(environ.get('NU_CONSUMER_BATCH_CONSUME_MAX_TIME_SECONDS', '10'))
        self.batch_consume_max_empty_polls = int(environ.get('NU_CONSUMER_BATCH_CONSUME_MAX_EMPTY_POLLS', '2'))
        self.batch_consume_store_messages = False

    def as_client_dict(self):
        ms_tolerance = 1000
        return {
            "auto.offset.reset": self.auto_offset_reset,
            "auto.commit.interval.ms": self.auto_commit_secs * 1000,

            "max.poll.interval.ms": self.timeout_mins * 60000,  # Max time between poll() calls before considered dead.
            "session.timeout.ms": self.heartbeat_timeout_ms,  # need at least 1 heartbeat within "session" time to be considered alive;
            "heartbeat.interval.ms": (self.heartbeat_timeout_ms // 5) - ms_tolerance,  # 5 failed heartbeats == bad consumer.

            "message.max.bytes": self.message_max_size_mb * (2 ** 20),
            "fetch.max.bytes": self.message_batch_max_size_mb * (2 ** 20),
            "queued.max.messages.kbytes": self.message_queue_max_size_mb * (2 ** 10),
        }
