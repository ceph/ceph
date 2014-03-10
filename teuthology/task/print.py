"""
Print task
"""

import logging

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Print out config argument in teuthology log/output
    """
    log.info('{config}'.format(config=config))
