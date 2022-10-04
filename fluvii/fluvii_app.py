from confluent_kafka.error import KafkaException
from confluent_kafka import TopicPartition
from .general_utils import log_and_raise_error
from .custom_exceptions import NoMessageError, SignalRaise, GracefulTransactionFailure, FatalTransactionFailure, PartitionsAssigned, FinishedTransactionBatch
from .consumer import TransactionalConsumer
from .producer import TransactionalProducer
from .config import FluviiConfig
from .schema_registry import SchemaRegistry
from .sqlite_utils import SqliteFluvii
from .transaction import Transaction, TableTransaction
import logging
from datetime import datetime
from collections import deque
from time import sleep


LOGGER = logging.getLogger(__name__)


class FluviiApp:
    """ The main class to use for most GTFO apps. See README for initialization/usage details. """
    def __init__(self, app_function, consume_topics_list, fluvii_config=None, produce_topic_schema_dict=None, transaction_type=Transaction,
                 app_function_arglist=None, metrics_manager=None):
        if not app_function_arglist:
            app_function_arglist = []
        if isinstance(consume_topics_list, str):
            consume_topics_list = consume_topics_list.split(',')
        if not produce_topic_schema_dict:  # Should only be the case if you dynamically add topics at runtime...otherwise should just use the plain Consumer
            produce_topic_schema_dict = {topic: None for topic in consume_topics_list}

        self._shutdown = False
        self._config = fluvii_config
        self._transaction_type = transaction_type
        self._app_function = app_function
        self._app_function_arglist = app_function_arglist
        self._produce_topic_schema_dict = produce_topic_schema_dict
        self._consume_topics_list = consume_topics_list
        self._consumer = None
        self._producer = None
        self._schema_registry = None

        self.metrics_manager = metrics_manager
        self.transaction = None

        self._set_config()
        self._set_metrics_manager()
        self._set_schema_registry()
        self._set_producer()
        self._set_consumer()

    def _set_config(self):
        if not self._config:
            self._config = FluviiConfig()

    def _set_metrics_manager(self):
        if not self.metrics_manager:
            pass  # TODO: add metrics manager back

    def _set_schema_registry(self):
        LOGGER.debug('Setting up Schema Registry...')
        self._schema_registry = SchemaRegistry(
            self._config.schema_registry_url,
            auth_config=self._config.schema_registry_auth_config
        ).registry

    def _set_producer(self, force_init=False):
        if (not self._producer) or force_init:
            LOGGER.debug('Setting up Kafka Transactional Producer')
            self._producer = TransactionalProducer(
                urls=self._config.client_urls,
                client_auth_config=self._config.client_auth_config,
                topic_schema_dict=self._produce_topic_schema_dict,
                transactional_id=self._config.hostname,
                metrics_manager=self.metrics_manager,
                schema_registry=self._schema_registry,
                settings_config=self._config.producer_config,
            )
            LOGGER.debug('Producer setup complete.')

    def _set_consumer(self, auto_subscribe=True):
        if not self._consumer:
            self._consumer = TransactionalConsumer(
                urls=self._config.client_urls,
                client_auth_config=self._config.client_auth_config,
                group_id=self._config.app_name,
                consume_topics_list=self._consume_topics_list,
                auto_subscribe=auto_subscribe,
                metrics_manager=self.metrics_manager,
                schema_registry=self._schema_registry,
                settings_config=self._config.consumer_config,
            )

    def _reset_producer(self):
        self._set_producer(force_init=True)
        self.transaction.producer = self._producer

    def _abort_transaction(self):
        if self.transaction.abort_transaction():
            self._reset_producer()

    def _init_transaction_handler(self, **kwargs):
        self.transaction = self._transaction_type(self._producer, self._consumer, fluvii_app_instance=self, auto_consume=False, **kwargs)
        return self.transaction

    def _handle_message(self, **kwargs):
        self.consume(**kwargs)
        self._app_function(self.transaction, *self._app_function_arglist)

    def _no_message_callback(self):
        self._producer.poll(0)
        LOGGER.info('No messages!')

    def _finalize_transaction_batch(self):
        LOGGER.debug('Finalizing transaction batch...')
        self.commit()

    def _app_batch_run_loop(self, **kwargs):
        self._init_transaction_handler()
        LOGGER.info(f'Consuming {self._config.consumer_config.batch_consume_max_count} messages'
                    f' over {self._config.consumer_config.batch_consume_max_time_secs} seconds...')
        try:
            while not self._shutdown:
                self._handle_message(**kwargs)
        except FinishedTransactionBatch:
            self._finalize_transaction_batch()
        except NoMessageError:
            self._no_message_callback()
        except GracefulTransactionFailure:
            LOGGER.info("Graceful transaction failure; retrying message with a new transaction...")
            self._abort_transaction()
        except FatalTransactionFailure:
            LOGGER.info("Fatal transaction failure; recreating the producer and retrying message...")
            self._abort_transaction()

    def _app_shutdown(self):
        LOGGER.info('App is shutting down...')
        self._shutdown = True
        self.transaction.abort_transaction()
        self.kafka_cleanup()

    def kafka_cleanup(self):
        """ Public method in the rare cases where you need to do some cleanup on the consumer object manually. """
        LOGGER.info('Performing graceful teardown of producer and/or consumer...')
        if self._consumer:
            LOGGER.debug("Shutting down consumer; no further commits can be queued or finalized.")
            self._consumer.close()
        if self._producer:
            LOGGER.debug("Sending/confirming the leftover messages in producer message queue")
            self._producer.close()

    def consume(self, **kwargs):
        self.transaction.consume(**kwargs)

    def commit(self):
        self.transaction.commit()

    def run(self, **kwargs):
        """
        # as_loop is really only for rare apps that don't follow the typical consume-looping behavior
        (ex: async apps) and don't seem to raise out of the True loop as expected.
        """
        LOGGER.info('RUN initialized!')
        try:
            while not self._shutdown:
                self._app_batch_run_loop(**kwargs)
        except SignalRaise:
            LOGGER.info('Shutdown requested via SignalRaise!')
        except Exception as e:
            raise
            # LOGGER.error(e)
            # if self.metrics_manager:
            #     log_and_raise_error(self.metrics_manager, e)
        finally:
            self._app_shutdown()


class FluviiBatchManagerApp(FluviiApp):
    def _app_batch_run(self, **kwargs):
        if not self.transaction._committed:
            try:
                self.consume(**kwargs)
            except FinishedTransactionBatch as e:
                LOGGER.debug(e)
            except NoMessageError:
                self._producer.poll(0)
                if not (any(self.transaction._pending_table_offset_increase.values())):
                    raise
        self._app_function(self.transaction, *self._app_function_arglist)
        self.commit()


class FluviiTableApp(FluviiApp):
    def __init__(self, app_function, consume_topic, fluvii_config=None, produce_topic_schema_dict=None, transaction_type=TableTransaction,
                 app_function_arglist=None, metrics_manager=None, table_changelog_topic=None, table_recovery_multiplier=None):

        super().__init__(
            app_function, consume_topic, fluvii_config=fluvii_config, produce_topic_schema_dict=produce_topic_schema_dict,
            transaction_type=transaction_type, app_function_arglist=app_function_arglist, metrics_manager=metrics_manager)

        if not table_changelog_topic:
            table_changelog_topic = self._config.table_changelog_topic
        self.changelog_topic = table_changelog_topic
        self._producer.add_topic(self.changelog_topic, {"type": "string"})
        self.tables = {}

        # Table recovery attributes
        if not table_recovery_multiplier:
            table_recovery_multiplier = self._config.table_recovery_multiplier
        self._active_primary_partitions = {}
        self._paused_primary_partitions = {}
        self._active_table_changelog_partitions = {}
        self._pending_table_db_assignments = deque()
        self._pending_table_recoveries = {}
        self._recovery_multiplier = table_recovery_multiplier
        self._init_recovery_time_remaining_attrs()

    # -------------------------  Protected Method Overrides ------------------------
    def _set_consumer(self):
        super()._set_consumer(auto_subscribe=False)
        self._consumer.subscribe(self._consume_topics_list, on_assign=self._partition_assignment,
                                 on_revoke=self._partition_unassignment, on_lost=self._partition_unassignment)

    def _init_transaction_handler(self, **kwargs):
        super()._init_transaction_handler(fluvii_changelog_topic=self.changelog_topic, fluvii_tables=self.tables, **kwargs)

    def _finalize_transaction_batch(self):
        super()._finalize_transaction_batch()
        self.check_table_commits()

    def _no_message_callback(self):
        super()._no_message_callback()
        self.commit_tables()

    def _app_batch_run_loop(self, **kwargs):
        LOGGER.info(f'Consuming from partitions: {list(self._active_primary_partitions.keys())}')
        try:
            super()._app_batch_run_loop(**kwargs)
        except PartitionsAssigned:
            self._table_and_recovery_manager()

    def _app_shutdown(self):
        super()._app_shutdown()
        self._table_close()

    # ----------------------- Protected Method Extensions -----------------------
    def _init_recovery_time_remaining_attrs(self):
        self._recovery_time_start = int(datetime.timestamp(datetime.now()))
        self._recovery_offsets_remaining = 0
        self._recovery_offsets_handled = 0

    def _partition_assignment(self, consumer, add_partition_objs):
        """
        Called every time a rebalance happens and handles table assignment and recovery flow.
        NOTE: rebalances pass relevant partitions per rebalance call which can happen multiple times, especially when
        multiple apps join at once; we have objects to track all updated partitions received during the entire rebalance.
        NOTE: confluent-kafka expects this method to have exactly these two arguments ONLY
        NOTE: _partition_assignment will ALWAYS be called (even when no new assignments are required) after _partition_unassignment.
            """
        if not self._shutdown:
            LOGGER.debug('Rebalance Triggered - Assigment')
            self.transaction.abort_transaction()
            partitions = {p_obj.partition: p_obj for p_obj in add_partition_objs}
            if add_partition_objs:
                LOGGER.info(f'Rebalance - Assigning additional partitions: {list(partitions.keys())}')
                self._paused_primary_partitions.update(partitions)
                self._pending_table_db_assignments.extend(list(partitions.keys()))
                self._pending_table_recoveries.update({p: None for p in partitions.keys()})
                self._consumer.incremental_assign(add_partition_objs)
                self._consumer.pause(add_partition_objs)
                raise PartitionsAssigned  # want to interrupt what it was doing
            else:
                LOGGER.debug(f'No new partitions assigned, skipping rebalance handling')

    def _partition_unassignment(self, consumer, drop_partition_objs):
        """
        NOTE: confluent-kafka expects this method to have exactly these two arguments ONLY
        NOTE: _partition_assignment will always be called (even when no new assignments are required) after _partition_unassignment.
        """
        LOGGER.info('Rebalance Triggered - Unassigment')
        self.transaction.abort_transaction()
        partitions = [p_obj.partition for p_obj in drop_partition_objs]
        self._table_close([p for p in self.tables.keys() if p in partitions])
        unassign_active_changelog = {p: p_obj for p, p_obj in self._active_table_changelog_partitions.items() if p in partitions}
        full_unassign = {self.changelog_topic: list(unassign_active_changelog.keys()), self._consume_topics_list[0]: partitions}
        if self._pending_table_recoveries:
            full_unassign.update({self.changelog_topic: list(unassign_active_changelog.keys())}),

        LOGGER.info(f'Unassigning partitions:\n{full_unassign}')
        self._consumer.incremental_unassign(list(unassign_active_changelog.values()) + drop_partition_objs)

        for var in ['_active_primary_partitions', '_paused_primary_partitions', '_active_table_changelog_partitions', '_pending_table_recoveries']:
            self.__setattr__(var, {k: v for k, v in self.__getattribute__(var).items() if k not in partitions})
            LOGGER.debug(f'{var} after unassignment: {list(self.__getattribute__(var).keys())}')
        self._pending_table_db_assignments = deque([i for i in self._pending_table_recoveries if i not in partitions])
        LOGGER.debug(f'_pending_table_db_assignments after unassignment: {self._pending_table_db_assignments}')
        LOGGER.info('Unassignment Complete!')

    def _pause_active_primary_partitions(self):
        self._consumer.pause(list(self._active_primary_partitions.values()))
        self._paused_primary_partitions.update(self._active_primary_partitions)
        self._active_primary_partitions = {}

    def _resume_active_primary_partitions(self):
        self._consumer.resume(list(self._paused_primary_partitions.values()))
        self._active_primary_partitions.update(self._paused_primary_partitions)
        self._paused_primary_partitions = {}

    def _cleanup_and_resume_app_loop(self):
        if self._active_table_changelog_partitions:
            LOGGER.info(f'unassigning changelog partitions: {list(self._active_table_changelog_partitions.values())}')
            self._consumer.incremental_unassign(list(self._active_table_changelog_partitions.values()))
            self._active_table_changelog_partitions = {}
        LOGGER.debug(f'Resuming consumption for paused topic partitions:\n{list(self._paused_primary_partitions.keys())}')
        self._resume_active_primary_partitions()
        LOGGER.info(f'Continuing normal consumption loop for partitions {list(self._active_primary_partitions.keys())}')

    def _get_changelog_watermarks(self, p_pobj_dict):
        """
        Note: this is a separate function since it requires the consumer to communicate with the broker
        """
        LOGGER.debug(f'Getting watermarks for changelog partitions {list(p_pobj_dict.keys())}')
        return {p: {'watermarks': self._consumer.get_watermark_offsets(p_obj, timeout=8), 'partition_obj': p_obj} for p, p_obj in p_pobj_dict.items()}

    def _table_recovery_set_offset_seek(self):
        """ Refresh the offsets of recovery partitions to account for updated recovery states during rebalancing """
        for p, offsets in self._pending_table_recoveries.items():
            new_offset = self._pending_table_recoveries[p]['table_offset_recovery']
            low_mark = self._pending_table_recoveries[p]['watermarks'][0]
            if low_mark > new_offset:  # handles offsets that have been removed/compacted. Should never happen, but ya know
                LOGGER.info(f'p{p} table has an offset ({new_offset}) less than the changelog lowwater ({low_mark}), likely due to retention settings. Setting {low_mark} as offset start point.')
                new_offset = low_mark
                self.tables[p].set_offset(new_offset - 1)  # -1 since the table marks the latest offset it has, not which offset is next
            high_mark = self._pending_table_recoveries[p]['watermarks'][1]
            LOGGER.debug(f'p{p} table has an offset delta of {high_mark - new_offset}')
            self._pending_table_recoveries[p]['partition_obj'].offset = new_offset
        self._recovery_offsets_remaining = sum([self._pending_table_recoveries[p]['watermarks'][1] - self._pending_table_recoveries[p]['partition_obj'].offset for p in self._pending_table_recoveries])

    def _refresh_pending_table_recoveries(self):
        """
        confirms new recoveries and removes old ones if not applicable anymore
        """
        partition_watermarks = self._get_changelog_watermarks({p: TopicPartition(topic=self.changelog_topic, partition=int(p)) for p, d in self._pending_table_recoveries.items()})
        LOGGER.debug(f'Changelog watermarks: {partition_watermarks}')
        for partition, data in partition_watermarks.items():
            watermarks = data['watermarks']
            if watermarks[0] != watermarks[1]:
                table_offset = self.tables[partition].offset
                LOGGER.info(f'(lowwater, highwater) [table_offset] for changelog p{partition}: {watermarks}, [{table_offset}]')
                if table_offset < watermarks[1]:
                    data['table_offset_recovery'] = table_offset
        self._pending_table_recoveries = {k: v for k, v in partition_watermarks.items() if v.get('table_offset_recovery') is not None}
        LOGGER.debug(f'Remaining recoveries after offset refresh check: {self._pending_table_recoveries}')
        self._table_recovery_set_offset_seek()

    def _table_db_init(self, partition):
        self.tables[partition] = SqliteFluvii(f'p{partition}', fluvii_config=self._config)

    def _table_db_assignments(self):
        while self._pending_table_db_assignments:
            partition = self._pending_table_db_assignments.popleft()
            if partition not in self.tables:
                sleep(.1)  # try to slow down pvc stuff a little
                self._table_db_init(partition)

    def _set_table_offset_to_latest(self, partition):
        LOGGER.debug(f'Setting table offset p{partition} to latest')
        self.tables[partition].offset = self._pending_table_recoveries[partition]['watermarks'][1]
        self.tables[partition].commit()
        del self._pending_table_recoveries[partition]
        LOGGER.info(f'table p{partition} fully recovered!')

    def _throwaway_poll(self):
        """ Have to poll (changelog topic) after first assignment to it to allow seeking """
        LOGGER.debug("Performing throwaway poll to allow assignments to properly initialize...")
        try:
            self.consume(timeout=8)
        except NoMessageError:
            pass

    def _table_close(self, partitions=None):
        interrupt = None
        full_shutdown = False
        if not partitions or self._shutdown:
            partitions = list(self.tables.keys())
            full_shutdown = True
        LOGGER.debug(f'Table - closing connections for partitions {partitions}')
        for p in partitions:
            try:
                self.tables[p].close()
                sleep(.1)
                LOGGER.debug(f'p{p} table connection closed.')
                del self.tables[p]
            except KeyError:
                if not full_shutdown:
                    LOGGER.debug(
                        f'Table p{p} did not seem to be mounted and thus could not unmount,'
                        f' likely caused by multiple rebalances in quick succession.'
                        f' This is unliklely to cause issues as the client is in the middle of adjusting itself, '
                        f' but should be noted.')
            except Exception as e:
                LOGGER.debug(f'Interrupt received during table db closing, {e}')
                interrupt = e
        LOGGER.info(f'Table - closed connections for partitions {partitions}')
        if interrupt:  # ensure all table cleanup happens
            LOGGER.info('Continuing with exception interrupt raised during table closing')
            raise interrupt

    def _assign_recovery_partitions(self):
        # TODO: maybe add additional check/inclusion of assignment based on c.assignment to avoid desync? (hasnt happened yet though)
        LOGGER.debug(f'Preparing changelog table recovery partition assignments...')
        to_assign = {p: self._pending_table_recoveries[p]['partition_obj'] for p in self._pending_table_recoveries.keys() if p not in self._active_table_changelog_partitions}
        LOGGER.info(f'Assigning changelog partitions {to_assign}')
        self._active_table_changelog_partitions.update(to_assign)
        self._consumer.incremental_assign(list(to_assign.values()))  # strong assign due to needing to seek
        LOGGER.debug(f'pending table recoveries before recovery attempt: {list(self._pending_table_recoveries.keys())}')
        LOGGER.debug(f'assigned recoveries before recovery attempt (should match pending now): {self._active_table_changelog_partitions}')

    def _table_recovery_start(self):
        try:
            for partition_info in self._pending_table_recoveries.values():
                self._consumer.seek(partition_info['partition_obj'])
            self._table_recovery_loop()
        except KafkaException as kafka_error:
            if 'Failed to seek to offset' in kafka_error.args[0].str():
                LOGGER.debug('Running a consumer poll to allow seeking to work on the changelog partitions...')
                self._throwaway_poll()
                self._table_recovery_start()
            else:
                raise

    def _check_pending_recovery_status(self):
        recovery_elapsed_time_secs = int(datetime.timestamp(datetime.now())) - self._recovery_time_start
        LOGGER.debug(f'Current pending recovery partition statuses before refreshing: {self._pending_table_recoveries}')
        recovery_status = {
            p: {
                'table_offset': self.transaction._table_offset(partition=p),
                'highwater': self._pending_table_recoveries[p]['watermarks'][1]
            } for p in self._pending_table_recoveries.keys()
        }
        current_recovery_offsets_remaining = {p: recovery_status[p]['highwater'] - recovery_status[p]['table_offset'] for p in recovery_status}
        total_remaining_offsets = sum(current_recovery_offsets_remaining.values())
        offsets_processed = self._recovery_offsets_remaining - total_remaining_offsets
        self._recovery_offsets_handled += offsets_processed
        self._recovery_offsets_remaining = total_remaining_offsets
        recovery_rate_offsets_secs = self._recovery_offsets_handled / (recovery_elapsed_time_secs + 1)  # avoid 0 div error
        remaining_time_mins = round((self._recovery_offsets_remaining / recovery_rate_offsets_secs) / 60, 2)
        LOGGER.debug(f'RECOVERY - offsets processed this iteration: {offsets_processed}')
        LOGGER.debug(f'RECOVERY - current process rate: {int(recovery_rate_offsets_secs)} offsets/sec')
        LOGGER.info(f'RECOVERY - {self._recovery_offsets_handled} TOTAL OFFSETS PROCESSED OVER {round(recovery_elapsed_time_secs / 60, 2)} minutes')
        LOGGER.info(f'RECOVERY - ESTIMATED TIME REMAINING: {remaining_time_mins} minutes')
        LOGGER.info(f'RECOVERY - OFFSETS REMAINING: {current_recovery_offsets_remaining}')
        for p in recovery_status:
            if recovery_status[p]['highwater'] - (recovery_status[p]['table_offset'] + 2) <= 0:  # +2 comes from: +1 due to table tracking offset it has up to, and another +1 due to last offset expecting to be a transaction marker
                LOGGER.info(f'Offset delta for p{p} is within an expected tolerable range of the changelog highwater; it is safe to assume transactional markers are the remaining delta.')
                self._set_table_offset_to_latest(p)

    def _table_recovery_loop(self, checks=2):  # autobatch
        while self._pending_table_recoveries and checks:
            LOGGER.info(f'Consuming from changelog partitions: {list(self._active_table_changelog_partitions.keys())}')
            LOGGER.info(f'Processing up to {self._config.consumer_config.batch_consume_max_count * self._recovery_multiplier} messages for up to {self._config.consumer_config.batch_consume_max_time_secs} seconds!')
            # NOTE: no transaction commits since its just consuming from changelog and writing to the table, we dont care about the consumer group offset
            try:
                self._init_transaction_handler()
                while checks:
                    self.transaction.consume(consume_multiplier=self._recovery_multiplier)
                    self.transaction._update_table_entry_from_changelog()
            except FinishedTransactionBatch:
                LOGGER.info(f'Consumed {self._consumer._consume_message_count} changelog messages')
                self.transaction._table_write(recovery_multiplier=self._recovery_multiplier)  # relay transaction's cached writes to the table's write cache
                self._check_pending_recovery_status()
            except NoMessageError:  # NOTE: this does not necc mean no messages were pulled at all in the batch, just that there's now none left!
                checks -= 1
                LOGGER.debug(f'No further changelog messages, checks remaining: {checks}')
        self.commit_tables()
        self._check_pending_recovery_status()
        if not checks:
            for p in list(self._pending_table_recoveries.keys()):  # list stops dict iter size change exception
                self._set_table_offset_to_latest(p)

    def _table_and_recovery_manager(self):
        LOGGER.debug('Table and recovery manager initialized')
        self._init_recovery_time_remaining_attrs()
        try:
            self._table_db_assignments()
            self._refresh_pending_table_recoveries()
            if self._pending_table_recoveries:
                self._pause_active_primary_partitions()
                while self._pending_table_recoveries:
                    self._assign_recovery_partitions()
                    LOGGER.info('BEGINNING TABLE RECOVERY PROCEDURE')
                    self._table_recovery_start()
                    if self._pending_table_recoveries:
                        self._refresh_pending_table_recoveries()
                LOGGER.info("TABLE RECOVERY COMPLETE!")
            else:
                LOGGER.info('No table recovery required!')
            self._cleanup_and_resume_app_loop()
        except PartitionsAssigned:
            LOGGER.info('Rebalance triggered while recovering tables. Will resume recovery after, if needed.')
            self._table_and_recovery_manager()

    # ----------------------- Method Extensions -----------------------
    def check_table_commits(self, recovery_multiplier=1):
        for table in self.tables.values():
            table.commit_and_cleanup_if_ready(recovery_multiplier=recovery_multiplier)

    def commit_tables(self):
        for table in self.tables.values():
            table.commit()
