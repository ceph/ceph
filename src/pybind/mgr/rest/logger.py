
import logging


def logger():
    # The logger name corresponds to the module name (courtesy of
    # MgrModule.__init__
    return logging.getLogger("rest")
