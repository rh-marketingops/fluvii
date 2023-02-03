from .fluvii_app import FluviiApp, FluviiAppFactory
import logging

LOGGER = logging.getLogger(__name__)


class FluviiMultiMessageApp(FluviiApp):
    """
    This is for when you need to handle multiple messages at once, like for doing a bulk api batch.
    Your app_function should be written with the expectation that transaction.messages() is a list of messages to manipulate
    """
    # def _set_config_dict(self, config_dict):
    #     config_dict = super()._set_config_dict(config_dict)
    #     config_dict['consumer'].batch_consume_store_messages = True
    #     return config_dict

    def _handle_message(self, **kwargs):
        # Don't do the app function here anymore!
        self.consume(**kwargs)

    def _finalize_app_batch(self):
        # Do it at the end of consuming instead!
        if self.transaction.messages():
            self._app_function(self.transaction, *self._app_function_arglist)
        super()._finalize_app_batch()


class FluviiMultiMessageAppFactory(FluviiAppFactory):
    fluvii_app_cls = FluviiMultiMessageApp

    def _set_consumer(self, *args, **kwargs):
        LOGGER.warning('By default, there are custom consumer settings for this type of fluvii app '
                       'that will override any corresponding environment settings to ensure expected functionality')
        self._consumer_config.batch_consume_store_messages = True
        super()._set_consumer(*args, **kwargs)
