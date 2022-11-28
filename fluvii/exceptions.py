import signal


class SignalRaise(Exception):
    """Signal exception for confluent apps"""
    def __init__(self, signum, *args):
        if not args:
            default_message = f"external {signum} request received...gracefully killing app."
            args = (default_message,)
        super().__init__(*args)


class WrappedSignals:
    """
    Signal catcher for confluent apps
    Initialized in confluent_configs.py
    """
    def __init__(self):
        self.signals = {
            signal.SIGINT: 'SIGINT',  # aka KeyboardInterrupt
            signal.SIGTERM: 'SIGTERM'  # What Kubernetes sends to pods
        }
        self.init_signals()

    def gentle_murder(self, signum, stack):
        raise SignalRaise(self.signals[signum])

    def init_signals(self):
        for sig in self.signals:
            signal.signal(sig, self.gentle_murder)


class NoMessageError(Exception):
    """
    No message was returned when consuming from the broker
    """
    def __init__(self, *args):
        if not args:
            default_message = "No messages to consume"
            args = (default_message,)
        super().__init__(*args)


class FinishedTransactionBatch(Exception):
    """
    No message was returned when consuming from the broker
    """
    def __init__(self, *args):
        if not args:
            default_message = "Reached maximum message batch size or time limit for this transaction block"
            args = (default_message,)
        super().__init__(*args)


class ConsumeMessageError(Exception):
    """
    Non-trivial errors when consuming from the broker
    """
    pass


class MessageValueException(Exception):
    """
    Represents a significant error in the value of the message
    """
    pass


class ProduceHeadersException(Exception):
    """
    Represents a significant error in the value of the message
    """
    pass


class FailedAbort(Exception):
    pass


class TransactionCommitted(Exception):
    pass


class TransactionNotRequired(Exception):
    pass


class ProducerTimeoutFailure(Exception):
    """ Exception signifying a producer failing to communicate with the broker(s). """
    def __init__(self, *args):
        if not args:
            default_message = "Producer could not reach the broker(s) when attempting to send a message."
            args = (default_message,)
        super().__init__(*args)


class GracefulTransactionFailure(Exception):
    def __init__(self):
        pass


class FatalTransactionFailure(Exception):
    def __init__(self):
        pass


class TransactionTimeout(Exception):
    def __init__(self, *args):
        if not args:
            default_message = "The transaction timed out; until this is more gracefully handled by fluvii, this will terminate the application to ensure appropriate client handling"
            args = (default_message,)
        super().__init__(*args)


class PartitionsAssigned(Exception):
    def __init__(self):
        pass
