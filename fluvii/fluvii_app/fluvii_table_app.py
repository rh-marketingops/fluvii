from fluvii.custom_exceptions import NoMessageError, PartitionsAssigned, FinishedTransactionBatch, GracefulTransactionFailure, FatalTransactionFailure, EndCurrentLoop
from fluvii.transaction import TableTransaction
from .fluvii_app import FluviiApp
from .rebalance_manager import TableRebalanceManager
from datetime import datetime
import logging


LOGGER = logging.getLogger(__name__)


class FluviiTableApp(FluviiApp):
    def __init__(self, app_function, consume_topic, fluvii_config=None, produce_topic_schema_dict=None, transaction_cls=TableTransaction,
                 app_function_arglist=None, metrics_manager=None, table_changelog_topic=None, table_recovery_multiplier=None):

        super().__init__(
            app_function, consume_topic, fluvii_config=fluvii_config, produce_topic_schema_dict=produce_topic_schema_dict,
            transaction_cls=transaction_cls, app_function_arglist=app_function_arglist, metrics_manager=metrics_manager)

        if not table_changelog_topic:
            table_changelog_topic = self._config.table_changelog_topic
        self.topic = consume_topic
        self.changelog_topic = table_changelog_topic
        self.tables = {}
        self._rebalance_manager = None

        # # Table recovery attributes
        if not table_recovery_multiplier:
            table_recovery_multiplier = self._config.table_recovery_multiplier
        self._recovery_multiplier = table_recovery_multiplier

    # -------------------------  Protected Method Overrides ------------------------
    def _set_consumer(self):
        super()._set_consumer(auto_subscribe=False)
        self._consumer.subscribe(self._consume_topics_list, on_assign=self._partition_assignment,
                                 on_revoke=self._partition_unassignment, on_lost=self._partition_unassignment)

    def _init_transaction_handler(self, **kwargs):
        super()._init_transaction_handler(fluvii_changelog_topic=self.changelog_topic, fluvii_tables=self.tables, **kwargs)

    def _finalize_app_batch(self):
        super()._finalize_app_batch()
        self.check_table_commits()

    def _no_message_callback(self):
        super()._no_message_callback()
        self.commit_tables()

    def _app_batch_run_loop(self, **kwargs):
        try:
            super()._app_batch_run_loop(**kwargs)
        except PartitionsAssigned:
            self._handle_rebalance()

    def _runtime_init(self):
        super()._runtime_init()
        self._producer.add_topic(self.changelog_topic, {"type": "string"})
        self._rebalance_manager = TableRebalanceManager(self._consumer, self.changelog_topic, self.tables, self._config)

    def _app_shutdown(self):
        super()._app_shutdown()

    # ----------------------- Protected Method Extensions -----------------------
    def _init_recovery_time_remaining_attrs(self):
        self._recovery_time_start = int(datetime.timestamp(datetime.now()))
        self._recovery_offsets_remaining = 0
        self._recovery_offsets_handled = 0

    def _partition_assignment(self, consumer, add_partition_objs):
        """
        Called every time a rebalance happens and handles table assignment and recovery flow.
        NOTE: confluent-kafka expects this method to have exactly these two arguments ONLY
        NOTE: _partition_assignment will ALWAYS be called (even when no new assignments are required) after _partition_unassignment.
            """
        if not self._shutdown:
            LOGGER.debug('Rebalance Triggered - Assigment')
            self.abort_transaction()
            self._rebalance_manager.assign_partitions(add_partition_objs)
            raise PartitionsAssigned

    def _partition_unassignment(self, consumer, drop_partition_objs):
        """
        NOTE: confluent-kafka expects this method to have exactly these two arguments ONLY
        NOTE: _partition_assignment will always be called (even when no new assignments are required) after _partition_unassignment.
        """
        LOGGER.info('Rebalance Triggered - Unassigment')
        if not self._shutdown:
            self.abort_transaction()
        self._rebalance_manager.unassign_partitions(drop_partition_objs)

    def _handle_recovery_message(self):
        self.transaction.consume(consume_multiplier=self._recovery_multiplier)
        self.transaction._update_table_entry_from_changelog()

    def _finalize_recovery_batch(self):
        LOGGER.info(f'Consumed {self._consumer._consume_message_count} changelog messages')
        self.transaction._table_write(recovery_multiplier=self._recovery_multiplier)  # relay transaction's cached writes to the table's write cache
        self.transaction._recovery_commit()
        self._rebalance_manager.update_recovery_status()

    def _table_recovery_consume_loop(self, checks):
        LOGGER.info(f'Consuming from changelog partitions: {[p.partition for p in self._rebalance_manager.recovery_partitions]}')
        LOGGER.info(f'Processing up to {self._config.consumer_config.batch_consume_max_count * self._recovery_multiplier} messages for up to {self._config.consumer_config.batch_consume_max_time_secs} seconds!')
        # NOTE: no transaction commits since its just consuming from changelog and writing to the table, we dont care about the consumer group offset
        try:
            while not self._shutdown:
                try:
                    self._handle_recovery_message()
                except FinishedTransactionBatch:
                    self._finalize_recovery_batch()
                    raise EndCurrentLoop
        except EndCurrentLoop:
            pass
        except NoMessageError:
            checks -= 1
            LOGGER.debug(f'No further changelog messages, checks remaining: {checks}')
            self.commit_tables()
            self._rebalance_manager.update_recovery_status()
        except GracefulTransactionFailure:
            self._finalize_app_batch()
            self._table_recovery_consume_loop(checks)
        except FatalTransactionFailure:
            self.abort_transaction()
            self._table_recovery_consume_loop(checks)
        return checks

    def _handle_rebalance(self):
        try:
            LOGGER.debug('Rebalance management initiated...')
            self._rebalance_manager.set_table_and_recovery_state()
            if self._rebalance_manager.recovery_partitions:
                LOGGER.info('BEGINNING TABLE RECOVERY PROCEDURE')
                checks = 2
                while checks and self._rebalance_manager.recovery_partitions:
                    self._init_transaction_handler()
                    checks = self._table_recovery_consume_loop(checks)
                self.commit_tables()
                LOGGER.info('TABLE RECOVERY COMPLETE!')
            else:
                LOGGER.info('NO TABLE RECOVERY REQUIRED!')
            self._rebalance_manager.finalize_rebalance()
        except PartitionsAssigned:
            self._handle_rebalance()

    # ----------------------- Method Extensions -----------------------
    def check_table_commits(self, recovery_multiplier=1):
        for table in self.tables.values():
            table.commit_and_cleanup_if_ready(recovery_multiplier=recovery_multiplier)

    def commit_tables(self):
        for table in self.tables.values():
            table.commit()
