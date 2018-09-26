import logging
import pprint

log = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=4)

def _pprint_me(thing, prefix):
    return prefix + "\n" + pp.pformat(thing)

def task(ctx, config):
    """
    Dump task context and config in teuthology log/output

    The intended use case is didactic - to provide an easy way for newbies, who
    are working on teuthology tasks for the first time, to find out what
    is inside the ctx and config variables that are passed to each task.
    """
    log.info(_pprint_me(ctx, "Task context:"))
    log.info(_pprint_me(config, "Task config:"))
