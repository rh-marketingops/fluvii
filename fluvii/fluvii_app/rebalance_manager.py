from confluent_kafka import TopicPartition, KafkaException
from fluvii.sqlite import SqliteFluvii
from fluvii.exceptions import TransactionNotRequired
from datetime import datetime
import logging
from time import sleep

LOGGER = logging.getLogger(__name__)


# TODO: override the "Partition" class types to match TopicPartition so we dont need to do ._partition calls all the time

class Partition:
    def __init__(self, topic=None, partition=None, offset=0, p_obj=None):
        if p_obj:
            self._partition = p_obj
        else:
            self._partition = TopicPartition(topic=topic, partition=partition, offset=offset)
        self.assigned = False
        self.active = False

    def __getattr__(self, attr):
        """Note: this includes methods as well!"""
        try:
            return self.__getattribute__(attr)
        except AttributeError:
            return self._partition.__getattribute__(attr)

    def mark_assigned(self):
        self.assigned = True
        self.active = True


class TablePartition(Partition):
    def __init__(self, topic=None, partition=None, offset=0, p_obj=None):
        super().__init__(topic=topic, partition=partition, offset=offset, p_obj=p_obj)
        self.table_assigned = False
        self.is_recovering = False
        self.table_offset = None
        self.lowwater = None
        self.highwater = None

    def __getattr__(self, attr):
        """Note: this includes methods as well!"""
        try:
            return self.__getattribute__(attr)
        except AttributeError:
            return self._partition.__getattribute__(attr)

    @property
    def needs_recovery(self):
        # note: >2 is because: +1 due to table tracking "current" offset, and +1 due to last offset being a transaction marker
        has_consumable_offets = self.lowwater != self.highwater
        table_is_behind = self.recovery_offset_delta > 2
        LOGGER.debug(f'table {self.partition}: has consumable offsets? {has_consumable_offets}; table is behind? {table_is_behind}')
        return has_consumable_offets and table_is_behind

    @property
    def recovery_offset_delta(self):
        return self.highwater - self.table_offset

    def refresh_recovery_status(self):
        if self.is_recovering and not self.needs_recovery:
            self.is_recovering = False


class TableRebalanceManager:
    """
    Helps manage rebalancing with tables. In summary, will:
    1. assign both the "primary" topic and the changelog topic (which is tied to a table) upon a rebalance.
    2. immediately pause ALL partition consumption.
    3. check table offsets against related changelog partition highwaters; seek and resume any with deltas.
    4. If any changelogs resumed, consume from all unpaused partitions until tables are fully recovered.
    5. Pause all changelog partitions indefinitely, resume all primary partitions.

    """
    def __init__(self, consumer, changelog_topic, tables, config):
        self.consumer = consumer
        self.changelog = changelog_topic
        self._primary_partitions = []
        self._changelog_partitions = []
        self.tables = tables
        self._config = config
        self._init_recovery_time_remaining_attrs()
        LOGGER.debug('table rebalance manager initialized')

    @property
    def _all_partitions(self):
        return self._primary_partitions + self._changelog_partitions

    @property
    def _active_partitions(self):
        return [p for p in self._all_partitions if p.active]

    @property
    def recovery_partitions(self):
        return [p for p in self._changelog_partitions if p.is_recovering]

    def _resume_partitions(self, partitions):
        self.consumer.resume([p._partition for p in partitions])
        for p in partitions:
            p.active = True
            
    def _pause_all_changelog_partitions(self):
        LOGGER.debug('Pausing all active partitions')
        self.consumer.pause([p._partition for p in self._changelog_partitions if p.active])
        for p in self._changelog_partitions:
            p.active = False

    def _pause_all_active_partitions(self):
        LOGGER.debug('Pausing all active partitions')
        self.consumer.pause([p._partition for p in self._active_partitions])
        for p in self._active_partitions:
            p.active = False
    
    def _init_table_dbs(self):
        for p in self._changelog_partitions:
            if not p.table_assigned:
                self.tables[p.partition] = SqliteFluvii(f'p{p.partition}', self._config)
                p.table_assigned = True

    def _init_recovery_time_remaining_attrs(self):
        self._recovery_time_start = int(datetime.timestamp(datetime.now()))
        self._recovery_offsets_remaining = 0
        self._recovery_offsets_handled = 0

    def _get_changelog_watermarks(self):
        for p in self._changelog_partitions:
            p.lowwater, p.highwater = self.consumer.get_watermark_offsets(p._partition, timeout=10)

    def _get_table_offsets(self):
        for p in self._changelog_partitions:
            p.table_offset = self.tables[p.partition].offset
            if p.table_offset < p.lowwater:
                LOGGER.debug('Adjusting table offset due to it being lower than the changelog lowwater')
                p.table_offset = p.lowwater - 2  # -2 since the table marks the latest offset it has (which can never be the "last" offset since it's a marker), not which offset is next (aka how kafka tracks it)
        LOGGER.info(f'NOTE: tables are considered "current" if (highwater - table_offset) <= 2')
        LOGGER.info(f'(table, table offset, highwater) list : {[(p.partition, p.table_offset, p.highwater) for p in self._changelog_partitions]}')

    def _set_partition_recovery_statuses(self):
        for p in self._changelog_partitions:
            if p.needs_recovery:
                p.is_recovering = True

    def _changelog_seek_and_activate(self):
        LOGGER.debug('Seeking all changelog partitions that need to be recovered from...')
        for p in self.recovery_partitions:
            try:
                self.consumer.seek(TopicPartition(topic=p.topic, partition=p.partition, offset=p.table_offset))
            except KafkaException as kafka_error:
                if 'Failed to seek to offset' in kafka_error.args[0].str():
                    LOGGER.debug('Running a consumer poll to allow seeking to work on the changelog partitions...')
                    try:
                        self.consumer.consume(timeout=8)
                    except TransactionNotRequired:
                        pass
                    self._changelog_seek_and_activate()
                else:
                    raise
        self._resume_partitions(self.recovery_partitions)

    def _update_recovery_partition_table_offsets(self):
        for p in self.recovery_partitions:
            p.table_offset = self.tables[p.partition].offset
            
    def _set_all_tables_to_latest_offset(self):
        for p in self._changelog_partitions:
            if p.table_offset < p.highwater:
                LOGGER.debug(f'Setting table p{p.partition} to {p.highwater-2} (highwater-2) to mark as fully recovered')
                self.tables[p.partition].set_offset(p.highwater - 2)
                self.tables[p.partition].commit()
    
    def _close_tables(self, partitions=None):
        interrupt = None
        if not partitions:
            partitions = list(self.tables.keys())
        LOGGER.debug(f'Table - closing connections for partitions {partitions}')
        for p in partitions:
            try:
                self.tables[p].close()
                sleep(.1)
                LOGGER.debug(f'p{p} table connection closed.')
                del self.tables[p]
            except KeyError:
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
    
    def assign_partitions(self, partition_obj_list):
        LOGGER.info(f'ASSIGNMENT called for: {partition_obj_list}')
        new_primary = [Partition(p_obj=p) for p in partition_obj_list]
        new_changelog = [TablePartition(topic=self.changelog, partition=p.partition) for p in partition_obj_list]
        all_new_partitions = new_primary + new_changelog
        LOGGER.debug(f'Assigning partitions to consumer...')
        self.consumer.incremental_assign([p._partition for p in all_new_partitions])
        LOGGER.debug(f'partitions assigned!')
        self._primary_partitions += new_primary
        self._changelog_partitions += new_changelog
        LOGGER.debug('marking all partitions as assigned...')
        for p in all_new_partitions:
            p.mark_assigned()
        self._pause_all_active_partitions()
        self._init_table_dbs()
        LOGGER.debug(f'assignment steps finished!')

    def unassign_partitions(self, partition_obj_list):
        LOGGER.info(f'UNASSIGNMENT called for: {partition_obj_list}')
        partitions = [p.partition for p in partition_obj_list]
        self.consumer.incremental_unassign([p._partition for p in self._all_partitions if p.partition in partitions])
        self._primary_partitions = [obj for obj in self._primary_partitions if obj.partition not in partitions]
        self._changelog_partitions = [obj for obj in self._changelog_partitions if obj.partition not in partitions]
        self._close_tables(partitions=partitions)

    def set_table_and_recovery_state(self):
        self._init_recovery_time_remaining_attrs()
        self._get_changelog_watermarks()
        self._get_table_offsets()
        self._set_partition_recovery_statuses()
        if self.recovery_partitions:
            self._changelog_seek_and_activate()
            self._recovery_offsets_remaining = sum([p.recovery_offset_delta for p in self.recovery_partitions])

    def update_recovery_status(self):
        self._update_recovery_partition_table_offsets()
        for partition in self.recovery_partitions:
            partition.refresh_recovery_status()
        recovery_elapsed_time_secs = max(int(datetime.timestamp(datetime.now())) - self._recovery_time_start, 1)
        offsets_remaining = sum([p.recovery_offset_delta for p in self.recovery_partitions])
        offsets_processed = self._recovery_offsets_remaining - offsets_remaining
        self._recovery_offsets_handled += offsets_processed
        self._recovery_offsets_remaining = offsets_remaining
        recovery_rate_offsets_secs = (max(self._recovery_offsets_handled, 1)) / recovery_elapsed_time_secs  # avoid 0 div error
        remaining_time_mins = round((self._recovery_offsets_remaining / recovery_rate_offsets_secs) / 60, 2)
        LOGGER.debug(f'RECOVERY - offsets processed this iteration: {offsets_processed}')
        LOGGER.debug(f'RECOVERY - current process rate: {int(recovery_rate_offsets_secs)} offsets/sec')
        LOGGER.info(f'RECOVERY - {self._recovery_offsets_handled} TOTAL OFFSETS PROCESSED OVER {round(recovery_elapsed_time_secs / 60, 2)} minutes')
        LOGGER.info(f'RECOVERY - ESTIMATED TIME REMAINING: {remaining_time_mins} minutes')
        LOGGER.info(f'RECOVERY - OFFSETS REMAINING: {self._recovery_offsets_remaining}')

    def finalize_rebalance(self):
        self._set_all_tables_to_latest_offset()
        self._pause_all_changelog_partitions()
        self._resume_partitions(self._primary_partitions)
