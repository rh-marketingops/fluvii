from fluvii.general_utils import log_and_raise_error
from fluvii.exceptions import SignalRaise, GracefulTransactionFailure, FatalTransactionFailure, FinishedTransactionBatch, FailedAbort, TransactionCommitted, TransactionNotRequired
from fluvii.transaction import Transaction
import logging

LOGGER = logging.getLogger(__name__)


class FluviiApp:
    def __init__(self, app_function, consume_topics_list, fluvii_config, producer, consumer, schema_registry,
                 transaction_cls=Transaction, produce_topic_schema_dict=None, app_function_arglist=None, metrics_manager=None):
        if not app_function_arglist:
            app_function_arglist = []
        if isinstance(consume_topics_list, str):
            consume_topics_list = consume_topics_list.split(',')
        if not produce_topic_schema_dict:  # Should only be the case if you dynamically add topics at runtime...otherwise should just use the plain Consumer
            produce_topic_schema_dict = {}

        self._shutdown = False
        self._config = fluvii_config
        self._app_function = app_function
        self._app_function_arglist = app_function_arglist
        self._produce_topic_schema_dict = produce_topic_schema_dict
        self._consume_topics_list = consume_topics_list

        self._transaction_cls = transaction_cls
        self.transaction = None

        self._consumer = consumer
        self._producer = producer
        self._schema_registry = schema_registry
        self.metrics_manager = metrics_manager

    def abort_transaction(self):
        try:
            self.transaction.abort_transaction()
        except FailedAbort:
            # TODO: reset producer method
            pass

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
        for component in [self.metrics_manager, self._schema_registry, self._producer, self._consumer]:
            if component:
                component.start()

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
