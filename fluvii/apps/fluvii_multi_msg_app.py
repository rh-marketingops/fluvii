from .fluvii_app import FluviiApp, FluviiAppFactory
import logging

LOGGER = logging.getLogger(__name__)


class FluviiMultiMessageApp(FluviiApp):
    """
    This is for when you need to handle multiple messages at once, like for doing a bulk api batch.
    Your app_function should be written with the expectation that transaction.messages() is a list of messages to manipulate
    """

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

    def _make_consumer(self):
        LOGGER.warning('By default, there are custom consumer settings for this type of fluvii app '
                       'that will override any corresponding configuration settings to ensure expected functionality')
        self._consumer_config.batch_consume_store_messages = True
        return super()._make_consumer()
