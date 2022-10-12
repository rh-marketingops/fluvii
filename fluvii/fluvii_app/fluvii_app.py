from fluvii.general_utils import log_and_raise_error
from fluvii.custom_exceptions import NoMessageError, SignalRaise, GracefulTransactionFailure, FatalTransactionFailure, PartitionsAssigned, FinishedTransactionBatch
from fluvii.consumer import TransactionalConsumer
from fluvii.producer import TransactionalProducer
from fluvii.transaction import Transaction
from fluvii.schema_registry import SchemaRegistry
from .config import FluviiConfig
import logging


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
        LOGGER.debug('initing a transaction handler...')
        self.transaction = self._transaction_type(self._producer, self._consumer, fluvii_app_instance=self, auto_consume=False, **kwargs)
        return self.transaction

    def _handle_message(self, **kwargs):
        self.consume(**kwargs)
        self._app_function(self.transaction, *self._app_function_arglist)

    def _no_message_callback(self):
        self._producer.poll(0)
        LOGGER.debug('No messages!')

    def _finalize_transaction_batch(self):
        LOGGER.debug('Finalizing transaction batch, if necessary...')
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
        LOGGER.debug('Calling consume...')
        self.transaction.consume(**kwargs)

    def commit(self):
        LOGGER.debug('Calling commit...')
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
            LOGGER.error(f'{e.__class__}, {e}')
            if self.metrics_manager:
                log_and_raise_error(self.metrics_manager, e)
        finally:
            self._app_shutdown()


# class FluviiBatchManagerApp(FluviiApp):
#     def _app_batch_run(self, **kwargs):
#         if not self.transaction._committed:
#             try:
#                 self.consume(**kwargs)
#             except FinishedTransactionBatch as e:
#                 LOGGER.debug(e)
#             except NoMessageError:
#                 self._producer.poll(0)
#                 if not (any(self.transaction._pending_table_offset_increase.values())):
#                     raise
#         self._app_function(self.transaction, *self._app_function_arglist)
#         self.commit()
