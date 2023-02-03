# Is your environment already configured? Cool, use these!

from fluvii.fluvii_app import FluviiApp, FluviiTableApp, FluviiAppFactory, _FluviiMultiMessageApp
from fluvii.consumer import ConsumerConfig
import logging

LOGGER = logging.getLogger(__name__)


def fluvii_producer():
    # TODO
    pass


def fluvii_app(app_function, consume_topics_list, produce_topic_schema_dict=None, app_function_arglist=None):
    return FluviiAppFactory(
        app_function, consume_topics_list,
        produce_topic_schema_dict=produce_topic_schema_dict, app_function_arglist=app_function_arglist,
        fluvii_app_cls=FluviiApp
    )


def fluvii_table_app(app_function, consume_topics_list, produce_topic_schema_dict=None, app_function_arglist=None):
    return FluviiAppFactory(
        app_function, consume_topics_list,
        produce_topic_schema_dict=produce_topic_schema_dict, app_function_arglist=app_function_arglist,
        fluvii_app_cls=FluviiTableApp,
    )


def fluvii_multi_message_app(app_function, consume_topics_list, produce_topic_schema_dict=None, app_function_arglist=None):
    consumer_config = ConsumerConfig(batch_consume_store_messages=True)
    LOGGER.warning('By default, there are custom consumer settings for this type of fluvii app '
                   'that will override any corresponding environment settings to ensure proper functionality')
    return FluviiAppFactory(
        app_function, consume_topics_list,
        produce_topic_schema_dict=produce_topic_schema_dict, app_function_arglist=app_function_arglist,
        consumer_config=consumer_config, fluvii_app_cls=_FluviiMultiMessageApp,
    )
