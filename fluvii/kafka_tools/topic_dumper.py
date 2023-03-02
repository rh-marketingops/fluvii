from fluvii.apps.fluvii_multi_msg_app import FluviiMultiMessageAppFactory, FluviiMultiMessageApp
from confluent_kafka import TopicPartition
import logging
from time import sleep


LOGGER = logging.getLogger(__name__)


class TopicDumperApp(FluviiMultiMessageApp):
    # TODO: convert this to use a consumer only
    def __init__(self, consume_topics_dict, *args, app_function=None, **kwargs):
        """
        consume_topics_dict example: {"topic_a": {0: 100, 2: 400, 3: "earliest"}, "topic_b": {1: 220}}
        """
        self._consume_topics_dict = consume_topics_dict
        super().__init__(app_function, list(consume_topics_dict.keys()), *args, **kwargs)

    def _init_metrics_manager(self):
        pass

    def _get_partition_assignment(self):
        LOGGER.debug('Getting partition assignments...')
        self._consumer.consume(timeout=5)  # enables seeking
        partitions = self._consumer.assignment()  # Note: this actually can change in-place as partitions get assigned in the background!
        assign_count_prev = 0
        checks = 2
        while checks:
            assign_count_current = len(partitions)
            if (assign_count_current > assign_count_prev) or assign_count_current == 0:
                assign_count_prev = assign_count_current
            else:
                checks -= 1
            sleep(1)
        LOGGER.debug(f'All partition assignments retrieved: {partitions}')
        return partitions

    def _seek_consumer_to_offsets(self):
        LOGGER.info('Setting up consumer to pull from the beginning of the topics...')
        self._get_partition_assignment()  # mostly just to ensure we can seek
        for topic, partitions in self._consume_topics_dict.items():
            for p, offset in partitions.items():
                watermarks = self._consumer.get_watermark_offsets(TopicPartition(topic=topic, partition=p))
                if offset == 'earliest':
                    offset = watermarks[0]
                elif offset == 'latest':
                    offset = watermarks[1]
                elif int(offset) < watermarks[0]:
                    offset = watermarks[0]
                LOGGER.debug(f'Seeking {topic} p{p} to offset {offset}')
                self._consumer.seek(TopicPartition(topic=topic, partition=p, offset=offset))
        # drop the residual consumed message (.poll required for seeking) if that msg's partition had a specified offset
        if self._consumer.message and self._consumer.message.partition() in self._consume_topics_dict[self._consumer.message.topic()]:
            self._consumer._messages = []

    def _finalize_app_batch(self):
        if self._app_function:
            LOGGER.info('Transforming all messages to desired format...')
            self._consumer._messages = self._app_function(self.transaction, *self._app_function_arglist)
        raise Exception('Got all messages!')

    def _app_shutdown(self):
        LOGGER.info('App is shutting down...')
        self._shutdown = True
        self.kafka_cleanup()

    def _runtime_init(self):
        super()._runtime_init()
        self._seek_consumer_to_offsets()

    def run(self, **kwargs):
        try:
            super().run(**kwargs)
        finally:
            return self.transaction.messages()


class TopicDumperAppFactory(FluviiMultiMessageAppFactory):
    fluvii_app_cls = TopicDumperApp

    def _make_consumer(self):
        self._consumer_config.batch_consume_max_count = None
        self._consumer_config.batch_consume_max_time_seconds = None
        self._consumer_config.auto_offset_reset = 'earliest'
        return super()._make_consumer()

    def __init__(self, consume_topics_dict, *args, app_function=None, **kwargs):
        consume_topics_dict = {topic: {int(p): o for p, o in part_offset.items()} for topic, part_offset in consume_topics_dict.items()}
        self._consume_topics_dict = consume_topics_dict

        super().__init__(app_function, list(consume_topics_dict.keys()), *args, **kwargs)

    def _return_class(self):
        return self.fluvii_app_cls(
            self._consume_topics_dict, self._fluvii_config, self._make_producer(), self._make_consumer(), self._schema_registry, app_function=self._app_function,
            app_function_arglist=self._app_function_arglist, metrics_manager=self._metrics_manager, produce_topic_schema_dict=self._produce_topic_schema_dict
        )
