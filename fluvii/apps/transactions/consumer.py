from confluent_kafka import TopicPartition
from fluvii.exceptions import NoMessageError, FinishedTransactionBatch, TransactionNotRequired
from fluvii.components.consumer import Consumer
import datetime
import logging

LOGGER = logging.getLogger(__name__)


class TransactionalConsumer(Consumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._consume_max_time_secs = self._config.batch_consume_max_time_seconds
        self._consume_max_count = self._config.batch_consume_max_count
        self._consume_max_empty_polls = self._config.batch_consume_max_empty_polls
        self._store_batch_messages = self._config.batch_consume_store_messages
        self._max_msg_secs_behind = self._config.batch_consume_trigger_message_age_seconds
        self._batch_consume = False
        self._init_attrs()
        self._reset_keep_consuming_trackers()

    def _refresh_batch_consume_status(self):
        """
        This allows us to keep the consumer in a "batch" state until it reaches conditions where it likely no longer
        needs to remain so
        """
        if self._batch_consume:
            if self._consume_max_count and self._consume_message_count < self._consume_max_count:
                self._batch_consume = False

    def _reset_keep_consuming_trackers(self):
        self._refresh_batch_consume_status()
        self._batch_time_elapse_start = None
        self._batch_remaining_empty_polls = self._consume_max_empty_polls
        self._consume_message_count = 0

    def _init_attrs(self):
        self._batch_offset_starts = {}
        self._batch_offset_ends = {}
        self._messages = []
        self.message = None

    def _set_batch_start_time(self):
        self._batch_time_elapse_start = datetime.datetime.now().timestamp()

    def _max_consume_time_continue(self):
        continue_consume = True
        if self._consume_max_time_secs:
            if not self._batch_time_elapse_start:
                self._set_batch_start_time()
            seconds_elapsed = datetime.datetime.now().timestamp() - self._batch_time_elapse_start
            continue_consume = seconds_elapsed < self._consume_max_time_secs
        return continue_consume

    def _max_consume_count_continue(self, consume_multiplier=1):
        if self._consume_max_count:
            return self._consume_message_count < (self._consume_max_count * consume_multiplier)
        return True

    def _requires_batch_consuming(self):
        if not self._batch_consume:
            ts_now = int(datetime.datetime.timestamp(datetime.datetime.now(tz=self._tz)))
            msg_ts = int(self.message.timestamp()[1] * .001)
            LOGGER.debug(f'Timestamps: now - {ts_now}, msg - {msg_ts}')
            delta = ts_now - msg_ts
            LOGGER.debug(f'Message is {delta} seconds old')
            if delta > self._max_msg_secs_behind:
                LOGGER.info(f"Message is at least {self._max_msg_secs_behind} seconds old. Switching to batch mode!")
                self._batch_consume = True

    def _keep_consuming(self, consume_multiplier=1):
        if self.message:  # if at least 1 message has already been consumed
            return self._batch_consume and self._batch_remaining_empty_polls and self._max_consume_count_continue(
                consume_multiplier=consume_multiplier) and self._max_consume_time_continue()
        return self._batch_remaining_empty_polls and self._max_consume_time_continue()

    def _mark_offset_start(self):
        if self.message.topic() not in self._batch_offset_starts:
            self._batch_offset_starts[self.message.topic()] = {}
        if self.message.partition() not in self._batch_offset_starts[self.message.topic()]:
            self._batch_offset_starts[self.message.topic()][self.message.partition()] = self.message.offset()

    def _mark_offset_end(self):
        if self.message.topic() not in self._batch_offset_ends:
            self._batch_offset_ends[self.message.topic()] = {}
        self._batch_offset_ends[self.message.topic()][self.message.partition()] = self.message.offset()

    def _get_consumer_partition_assignment(self):
        assignments = self._consumer.assignment()
        assignments = {topic: [int(obj.partition) for obj in assignments if obj.topic == topic] for topic in
                       {obj.topic for obj in assignments}}
        return assignments

    def _commit(self, producer, offsets):
        if offsets:
            LOGGER.debug(f'Offset ends ready for commit: {self._batch_offset_ends}')
            if not producer.active_transaction:
                producer.begin_transaction()
            producer.send_offsets_to_transaction(offsets, self._consumer.consumer_group_metadata())
        else:
            LOGGER.info('No messages were consumed in this batch.')

        if producer.active_transaction:
            LOGGER.info('Comitting transaction!')
            producer.commit_transaction(30)
        else:
            raise TransactionNotRequired

    def _make_config(self):
        config = super()._make_config()
        config.update({
            "isolation.level": "read_committed",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        })
        return config

    def _handle_consumed_message(self):
        super()._handle_consumed_message()
        self._requires_batch_consuming()
        self._mark_offset_start()
        self._mark_offset_end()
        self._consume_message_count += 1
        if self._store_batch_messages:
            self._messages.append(self.message)

    @property
    def pending_commits(self):
        return bool(self._mark_offset_end)

    def messages(self):
        if self._store_batch_messages:
            return self._messages
        return super().messages()

    def rollback_consumption(self):
        LOGGER.info('Rolling back consumer state to earliest non-committed offset(s)...')
        assignments = self._get_consumer_partition_assignment()
        for topic, partitions in self._batch_offset_starts.items():
            for partition, offset in partitions.items():
                if partition in assignments.get(topic, []):
                    LOGGER.info(f"Reversing topic {topic} partition {partition} back to offset {offset}")
                    self._consumer.seek(TopicPartition(topic=topic, partition=partition, offset=offset))
        self._init_attrs()
        self._reset_keep_consuming_trackers()

    def commit(self, producer):
        offsets_to_commit = [TopicPartition(topic, partition, offset + 1) for topic, partitions in
                             self._batch_offset_ends.items() for partition, offset in partitions.items()]
        self._commit(producer, offsets_to_commit)
        self._init_attrs()

    def consume(self, timeout=None, consume_multiplier=1):
        """
        Consumes a message from the broker while handling errors.
        If the message is valid, then the message is returned.
        """
        while self._keep_consuming(consume_multiplier=consume_multiplier):
            try:
                return super().consume(timeout=timeout)
            except NoMessageError:
                self._batch_remaining_empty_polls -= 1  # still useful in cases with multiple empty polls and no other consumption limitations
                self._batch_consume = False
        LOGGER.info('Consumption attempts for this batch are finished.')
        self._reset_keep_consuming_trackers()
        raise FinishedTransactionBatch
