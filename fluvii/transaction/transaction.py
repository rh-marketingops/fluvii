from confluent_kafka import KafkaException
import logging
from copy import deepcopy
from fluvii.general_utils import parse_headers
from fluvii.exceptions import GracefulTransactionFailure, FatalTransactionFailure, FailedAbort, TransactionTimeout
from json import dumps, loads


LOGGER = logging.getLogger(__name__)


def handle_kafka_exception(kafka_error):
    LOGGER.error(kafka_error)
    LOGGER.error(f'KAFKA_ERROR_CODE: {kafka_error.args[0].code()}')
    retriable = kafka_error.args[0].retriable()
    abort = kafka_error.args[0].txn_requires_abort()
    LOGGER.info(f'KafkaException: is retriable? - {retriable}, should abort? - {abort}')
    if kafka_error.args[0].code() == '_TIMED_OUT':  # TODO: try to handle this more gracefully; it's supposedly "retriable"
        raise TransactionTimeout
    if retriable:
        raise GracefulTransactionFailure
    elif abort:
        raise FatalTransactionFailure
    else:
        pass


class Transaction:
    def __init__(self, producer, consumer, auto_consume=True, message=None, fluvii_app_instance=None, refresh_after_commit=False):
        self.producer = producer
        self.consumer = consumer
        self.message = message
        self.metrics_manager = self.consumer.metrics_manager
        self._allow_auto_consume = auto_consume
        self._refresh_after_commit = refresh_after_commit  # allows you to re-use the object, as needed
        self._init_attrs()

        # this optional attribute should never be referenced/used by class methods here to keep transactions independent of the app
        # only intended for runtime access of the app instance by the transaction
        self.app = fluvii_app_instance

    def _init_attrs(self):
        self._auto_consume(self.message, self._allow_auto_consume)

    def _auto_consume(self, has_input, should_consume):
        """ Consume message if initialized without one (and allowed to) """
        if not has_input and should_consume:
            self.consume()

    @property
    def has_outstanding_updates(self):
        return self.producer.active_transaction or self.consumer.pending_commits

    def messages(self):  # here for a consistent way to access message(s) currently being managed for when you subclass
        """ For a standardized way to access message(s) consumed pertaining to this transaction """
        return self.consumer.messages()

    def consume(self, **kwargs):
        self.message = self.consumer.consume(**kwargs)

    def key(self):
        return deepcopy(self.message.key())

    def value(self):
        return deepcopy(self.message.value())

    def headers(self):
        return deepcopy(parse_headers(self.message.headers()))

    def topic(self):
        return self.message.topic()

    def partition(self):
        return self.message.partition()

    def offset(self):
        return self.message.offset()

    def _abort_transaction(self):
        try:
            LOGGER.info('Aborting transaction.')
            self.producer.abort_transaction(10)
        except KafkaException as e:
            LOGGER.error(f"Failed to abort transaction: {e}")
            raise FailedAbort

    def abort_transaction(self):
        LOGGER.debug('Aborting any open transactions, if needed.')
        if self.consumer.pending_commits:
            self.consumer.rollback_consumption()
        if self.producer.active_transaction:
            self._abort_transaction()
        self._init_attrs()

    def produce(self, producer_kwargs):
        value = producer_kwargs.pop('value', producer_kwargs)
        self.producer.produce(value, message_passthrough=self.message, **producer_kwargs)

    def _commit(self):
        try:
            self.consumer.commit(self.producer)
        except KafkaException as kafka_error:
            handle_kafka_exception(kafka_error)

    def commit(self):
        """ Allows manual commits (safety measures in place so that you cant commit the same message twice)."""
        self._commit()
        if self._refresh_after_commit:
            self._init_attrs()


class TableTransaction(Transaction):
    def __init__(self, producer, consumer, fluvii_changelog_topic, fluvii_tables, auto_consume=True, message=None, fluvii_app_instance=None, refresh_after_commit=False):
        # set first since we override _init_attrs
        self.app_changelog_topic = fluvii_changelog_topic
        self.tables = fluvii_tables

        super().__init__(producer, consumer, message=message, auto_consume=auto_consume,
                         fluvii_app_instance=fluvii_app_instance, refresh_after_commit=refresh_after_commit)

    # -------------------------  Protected Method Overrides ------------------------
    def _init_attrs(self):
        super()._init_attrs()
        self._is_not_changelog_message = True
        self._pending_table_writes = {p: {} for p in self.tables}
        self._pending_table_offset_increase = {p: 0 for p in self.tables}

    def _commit(self):
        super()._commit()
        self._table_write()

    # ----------------------- Protected Method Extensions -----------------------
    def _table_offset(self, partition=None):
        if partition is None:
            partition = self.partition()
        value = self.tables[partition].offset
        value = int(value) if value else 0
        return value + self._pending_table_offset_increase.get(partition, 0)

    def _update_pending_table_writes(self, value):
        self._pending_table_writes[self.partition()][self.key()] = value
        if self._is_not_changelog_message:
            self._pending_table_offset_increase[self.partition()] += 1
        else:
            self._pending_table_offset_increase[self.partition()] += self.offset() - self._table_offset()

    def _update_changelog(self):
        if self._is_not_changelog_message and (pending_write := self._pending_table_writes.get(self.partition(), {}).get(self.key())):
            LOGGER.debug(f'Updating changelog topic for {self.key()}')
            self.produce(dict(value=dumps(pending_write), topic=self.app_changelog_topic, key=self.key(), partition=self.partition()))

    def _update_table_entry_from_changelog(self):
        self._is_not_changelog_message = False  # so we dont produce a message back to the changelog
        self.update_table_entry(loads(self.value()))

    def _table_write(self, recovery_multiplier=1):
        for p, msgs in self._pending_table_writes.items():
            if msgs:
                table = self.tables[p]
                LOGGER.debug(f'Finalizing table entry batch write of {len(msgs)} records for table {table.table_name}')
                LOGGER.debug(f'Table {table.table_name} offset before write: {table.offset}, expected after write: {self._table_offset(partition=p) + int(self._is_not_changelog_message)}')
                table.set_offset(self._table_offset(partition=p) + int(self._is_not_changelog_message))  # the +1 is because the transaction causes an extra offset at the end (per trans + partition)
                table.write_batch(msgs)
                table.commit_and_cleanup_if_ready(recovery_multiplier=recovery_multiplier)
                self._pending_table_writes[p] = {}
                self._pending_table_offset_increase[p] = 0

    # ----------------------- Method Extensions -----------------------
    def read_table_entry(self):
        pending_update = self._pending_table_writes.get(self.partition(), {}).get(self.key())
        if pending_update:
            return deepcopy(pending_update)
        return self.tables[self.partition()].read(self.key())

    def update_table_entry(self, value):
        self._update_pending_table_writes(value)
        self._update_changelog()

    def delete_table_entry(self):
        self._update_pending_table_writes('-DELETED-')
        self._update_changelog()
