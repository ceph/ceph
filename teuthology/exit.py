import logging
import os
import signal


log = logging.getLogger(__name__)


class Exiter(object):
    """
    A helper to manage any signal handlers we need to call upon receiving a
    given signal
    """
    def __init__(self):
        self.handlers = list()

    def add_handler(self, signals, func):
        """
        Adds a handler function to be called when any of the given signals are
        received.

        The handler function should have a signature like::

            my_handler(signal, frame)
        """
        if isinstance(signals, int):
            signals = [signals]

        for signal_ in signals:
            signal.signal(signal_, self.default_handler)

        handler = Handler(self, func, signals)
        log.debug(
            "Installing handler: %s",
            repr(handler),
        )
        self.handlers.append(handler)
        return handler

    def default_handler(self, signal_, frame):
        log.debug(
            "Got signal %s; running %s handler%s...",
            signal_,
            len(self.handlers),
            '' if len(self.handlers) == 1 else 's',
        )
        for handler in self.handlers:
            handler.func(signal_, frame)
        log.debug("Finished running handlers")
        # Restore the default handler
        signal.signal(signal_, 0)
        # Re-send the signal to our main process
        os.kill(os.getpid(), signal_)


class Handler(object):
    def __init__(self, exiter, func, signals):
        self.exiter = exiter
        self.func = func
        self.signals = signals

    def remove(self):
        try:
            log.debug("Removing handler: %s", self)
            self.exiter.handlers.remove(self)
        except ValueError:
            pass

    def __repr__(self):
        return "{c}(exiter={e}, func={f}, signals={s})".format(
            c=self.__class__.__name__,
            e=self.exiter,
            f=self.func,
            s=self.signals,
        )


exiter = Exiter()
