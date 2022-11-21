from fluvii.general_utils import log_and_raise_error
from fluvii.exceptions import SignalRaise, GracefulTransactionFailure, FatalTransactionFailure, FinishedTransactionBatch, FailedAbort, TransactionCommitted, TransactionNotRequired
from fluvii.consumer import TransactionalConsumer
from fluvii.producer import TransactionalProducer
from fluvii.transaction import Transaction
from fluvii.schema_registry import SchemaRegistry
from fluvii.metrics import MetricsManager
from .config import FluviiConfig
import logging


LOGGER = logging.getLogger(__name__)


class FluviiApp:
    def __init__(self, app_function, consume_topics_list, fluvii_config=None, produce_topic_schema_dict=None, app_function_arglist=None, metrics_manager=None,
                 transaction_cls=Transaction, consumer_cls=TransactionalConsumer, producer_cls=TransactionalProducer):
        if not app_function_arglist:
            app_function_arglist = []
        if isinstance(consume_topics_list, str):
            consume_topics_list = consume_topics_list.split(',')
        if not produce_topic_schema_dict:  # Should only be the case if you dynamically add topics at runtime...otherwise should just use the plain Consumer
            produce_topic_schema_dict = {}

        self._shutdown = False
        self._config = fluvii_config
        self._transaction_cls = transaction_cls
        self._producer_cls = producer_cls
        self._consumer_cls = consumer_cls
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

    def _set_config(self):
        if not self._config:
            self._config = FluviiConfig()

    def _init_clients(self):
        LOGGER.info('Initializing Kafka clients...')
        self._set_schema_registry()
        self._set_producer()
        self._set_consumer()

    def _init_metrics_manager(self):
        LOGGER.info("Initializing the MetricsManager...")
        if not self.metrics_manager:
            self.metrics_manager = MetricsManager(
                metrics_config=self._config.metrics_manager_config, pusher_config=self._config.metrics_pusher_config)

    def _set_schema_registry(self):
        LOGGER.debug('Setting up Schema Registry...')
        self._schema_registry = SchemaRegistry(
            self._config.schema_registry_url,
            auth_config=self._config.schema_registry_auth_config
        ).registry

    def _set_producer(self, force_init=False):
        if (not self._producer) or force_init:
            LOGGER.debug('Setting up Kafka Transactional Producer')
            self._producer = self._producer_cls(
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
            self._consumer = self._consumer_cls(
                urls=self._config.client_urls,
                client_auth_config=self._config.client_auth_config,
                group_id=self._config.app_name,
                consume_topics_list=self._consume_topics_list,
                auto_subscribe=auto_subscribe,
                metrics_manager=self.metrics_manager,
                schema_registry=self._schema_registry,
                settings_config=self._config.consumer_config,
            )

    def abort_transaction(self):
        try:
            self.transaction.abort_transaction()
        except FailedAbort:
            self._set_producer(force_init=True)

    def _init_transaction_handler(self, **kwargs):
        LOGGER.debug('initing a transaction handler...')
        self.transaction = self._transaction_cls(self._producer, self._consumer, fluvii_app_instance=self, auto_consume=False, **kwargs)
        return self.transaction

    def _handle_message(self, **kwargs):
        self.consume(**kwargs)
        self._app_function(self.transaction, *self._app_function_arglist)

    def _no_message_callback(self):
        self._producer.poll(0)
        LOGGER.debug('No messages!')

    def _finalize_app_batch(self):
        LOGGER.debug('Finalizing transaction batch, if necessary...')
        self.commit()

    def _app_batch_run_loop(self, **kwargs):
        LOGGER.info(f'Consuming {self._config.consumer_config.batch_consume_max_count} messages'
                    f' over {self._config.consumer_config.batch_consume_max_time_seconds} seconds...')
        try:
            while not self._shutdown:
                try:
                    self._handle_message(**kwargs)
                except FinishedTransactionBatch:
                    self._finalize_app_batch()
                    raise TransactionCommitted
        except TransactionCommitted:
            pass
        except TransactionNotRequired:
            self._no_message_callback()
        except GracefulTransactionFailure:
            LOGGER.info("Graceful transaction failure; retrying commit...")
            self._app_batch_run_loop(**kwargs)
        except FatalTransactionFailure:
            LOGGER.info("Fatal transaction failure; aborting the transaction and resetting consumer state...")
            self.abort_transaction()

    def _app_shutdown(self):
        LOGGER.info('App is shutting down...')
        self._shutdown = True
        try:
            self.abort_transaction()
        except:
            pass
        self.kafka_cleanup()

    def _runtime_init(self):
        self._init_metrics_manager()
        self._init_clients()

    def _run(self, **kwargs):
        try:
            while not self._shutdown:
                self._init_transaction_handler()
                self._app_batch_run_loop(**kwargs)
        except SignalRaise:
            LOGGER.info('Shutdown requested via SignalRaise!')
        except Exception as e:
            LOGGER.error(f'{e.__class__}, {e}')
            if self.metrics_manager:
                log_and_raise_error(self.metrics_manager, e)
        finally:
            self._app_shutdown()

    def kafka_cleanup(self):
        """ Public method in the rare cases where you need to do some cleanup on the consumer object manually. """
        LOGGER.info('Performing graceful teardown of producer and/or consumer...')
        if self._consumer:
            LOGGER.debug("Shutting down consumer; no further commits can be queued or finalized.")
            # TODO: make sure consumer unsubscribes on close.
            self._consumer.close()

    def consume(self, **kwargs):
        LOGGER.debug('Calling consume...')
        self.transaction.consume(**kwargs)

    def commit(self):
        LOGGER.debug('Calling commit...')
        self.transaction.commit()

    def run(self, **kwargs):
        LOGGER.info('RUN initialized!')
        self._runtime_init()
        self._run(**kwargs)
        # TODO: consider just returning self._run here to allow easy way to return things?
