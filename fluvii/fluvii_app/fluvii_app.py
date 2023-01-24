from fluvii.general_utils import log_and_raise_error
from fluvii.exceptions import SignalRaise, GracefulTransactionFailure, FatalTransactionFailure, FinishedTransactionBatch, FailedAbort, TransactionCommitted, TransactionNotRequired
from fluvii.consumer import TransactionalConsumer, ConsumerConfig
from fluvii.producer import TransactionalProducer, ProducerConfig
from fluvii.transaction import Transaction
from fluvii.schema_registry import SchemaRegistry
from fluvii.metrics import MetricsManager, MetricsManagerConfig
from .config import FluviiConfig
import logging
from fluvii.auth import SaslPlainClientConfig, SaslOauthClientConfig

LOGGER = logging.getLogger(__name__)


class FluviiApp:
    def __init__(self, app_function, consume_topics_list, fluvii_config=FluviiConfig(), produce_topic_schema_dict=None, app_function_arglist=None,
                 component_dict=None, component_configs=None):
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

        self._component_dict = self._set_component_dict(component_dict)
        self._config_dict = self._set_config_dict(component_configs)
        self._transaction_cls = self._component_dict['transaction']
        self.transaction = None

        # components that get build if missing
        self._consumer = None
        self._producer = None
        self._schema_registry = None
        self.metrics_manager = None
        self._set_components()

    def _set_component_dict(self, component_dict):
        if not component_dict:
            component_dict = {}
        default_component_dict = {
            'transaction': Transaction,
            'producer': TransactionalProducer,
            'consumer': TransactionalConsumer,
            'metrics_manager': MetricsManager,
            'schema_registry': SchemaRegistry,
        }
        default_component_dict.update(component_dict)
        return default_component_dict

    def _convert_configs_to_dict(self, configs):
        if not configs:
            return {}
        conversion = {
            ConsumerConfig: 'consumer',
            ProducerConfig: 'producer',
            MetricsManager: 'metrics_manager'
        }
        configs_out = {}
        if isinstance(configs, dict):
            assert len(set(configs.keys()) - set(conversion.values())) == 0
            configs_out = configs
        elif isinstance(configs, list):
            for c in configs:
                if isinstance(c, dict):
                    configs_out.update({k: v for k, v in c.items()})
                else:
                    configs_out[conversion[c.__class__]] = c
        assert len(set(configs_out.keys()) - set(conversion.values())) == 0
        return configs_out

    def _set_config_dict(self, configs):
        config_dict = self._convert_configs_to_dict(configs)
        fluvii_url_optional_override = {k: v for k, v in {'urls': self._config.kafka_urls} if v}
        fluvii_name_overrides = {'app_name': self._config.name, 'hostname': self._config.hostname}

        default_config_dict = {
            'producer': ProducerConfig,
            'consumer': ConsumerConfig,
            'metrics_manager': MetricsManagerConfig,
        }

        for config_name in ['producer', 'consumer']:
            if config_name in config_dict:
                if isinstance(config_dict[config_name], dict):
                    config_dict[config_name].update(fluvii_url_optional_override)
                    config_dict[config_name] = default_config_dict[config_name](**config_dict[config_name])
                else:
                    config_dict[config_name] = config_dict[config_name].copy(update=fluvii_url_optional_override)
            else:
                config_dict[config_name] = default_config_dict[config_name](**fluvii_url_optional_override)

        for config_name in ['metrics_manager']:
            if config_name in config_dict:
                if isinstance(config_dict[config_name], dict):
                    config_dict[config_name].update(fluvii_name_overrides)
                    config_dict[config_name] = default_config_dict[config_name](**config_dict[config_name])
                else:
                    config_dict[config_name] = config_dict[config_name].copy(update=fluvii_name_overrides)
            else:
                config_dict[config_name] = default_config_dict[config_name](**fluvii_name_overrides)

        for auth in [SaslOauthClientConfig, SaslPlainClientConfig]:
            for client_type in ['kafka', 'registry']:
                try:
                    config_dict.update({f'auth_{client_type}': auth(**self._config.get_auth(client_type))})
                    break
                except:
                    pass

        return config_dict

    def _set_components(self):
        self._set_metrics_manager()
        self._set_schema_registry()
        self._set_producer()
        self._set_consumer()

    def _set_metrics_manager(self):
        if not self.metrics_manager:
            LOGGER.info("Generating the MetricsManager component...")
            self.metrics_manager = self._component_dict['metrics'](config=self._config_dict['metrics'], auto_init=False)
            LOGGER.debug('MetricsManager generation complete.')

    def _set_schema_registry(self):
        if not self._schema_registry:
            LOGGER.info("Generating the SchemaRegistry component...")
            self._schema_registry = self._component_dict['registry'](auth_config=self._config_dict.get('auth_registry'), auto_init=False)
            LOGGER.debug('SchemaRegistry generation complete.')

    def _set_producer(self, force_init=False):
        # TODO: make a "restart" function on the producer
        if (not self._producer) or force_init:
            LOGGER.info("Generating the Producer component...")
            self._producer = self._component_dict['producer'](
                settings_config=self._config_dict['producer'],
                transactional_id=self._config.hostname,
                auth_config=self._config_dict.get('auth_kafka'),
                topic_schema_dict=self._produce_topic_schema_dict,
                metrics_manager=self.metrics_manager,
                schema_registry=self._schema_registry,
                auto_start=False,
            )
            LOGGER.debug('Producer generation complete.')

    def _set_consumer(self, auto_subscribe=True):
        if not self._consumer:
            LOGGER.info("Generating the Consumer component")
            self._consumer = self._component_dict['consumer'](
                settings_config=self._config_dict['consumer'],
                auth_config=self._config_dict.get('auth_kafka'),
                group_id=self._config.name,
                consume_topics_list=self._consume_topics_list,
                auto_subscribe=auto_subscribe,
                metrics_manager=self.metrics_manager,
                schema_registry=self._schema_registry,
                auto_start=False)
            LOGGER.debug('Consumer generation complete.')

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
        LOGGER.info(f'Consuming {self._config_dict["consumer"].batch_consume_max_count} messages'
                    f' over {self._config_dict["consumer"].batch_consume_max_time_seconds} seconds...')
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
