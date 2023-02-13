import logging
from fluvii.components.producer import Producer

LOGGER = logging.getLogger(__name__)


class TransactionalProducer(Producer):
    def __init__(self, transactional_id, *args, **kwargs):
        self._transactional_id = transactional_id
        self.active_transaction = False
        super().__init__(*args, **kwargs)

    def _make_client_config(self):
        config = super()._make_client_config()
        config.update({"transactional.id": self._transactional_id})
        return config

    def _init_producer(self):
        super()._init_producer()
        self._producer.init_transactions()

    def begin_transaction(self, *args, **kwargs):
        LOGGER.debug('Initializing a transaction...')
        self._producer.begin_transaction(*args, **kwargs)
        self.active_transaction = True

    def produce(self, value, key=None, topic=None, headers=None, partition=None, message_passthrough=None):
        if not self.active_transaction:
            self.begin_transaction()
        super().produce(value, key=key, topic=topic, headers=headers, partition=partition,
                        message_passthrough=message_passthrough)

    def abort_transaction(self, *args, **kwargs):
        LOGGER.debug('Aborting the transaction...')
        self._producer.abort_transaction(*args, **kwargs)
        self.active_transaction = False

    def commit_transaction(self, *args, **kwargs):
        self._producer.commit_transaction(*args, **kwargs)
        self._producer.poll(0)
        self.active_transaction = False

    def close(self):
        pass
