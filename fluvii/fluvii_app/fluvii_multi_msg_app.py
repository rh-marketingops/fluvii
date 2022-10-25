from . import FluviiApp


class FluviiMultiMessageApp(FluviiApp):

    def _set_config(self):
        super()._set_config()
        self._config.consumer_config.batch_consume_store_messages = True

    def _handle_message(self, **kwargs):
        # Don't do the app function here anymore!
        self.consume(**kwargs)

    def _finalize_app_batch(self):
        # Do it at the end of consuming instead!
        self._app_function(self.transaction, *self._app_function_arglist)
        super()._finalize_app_batch()
