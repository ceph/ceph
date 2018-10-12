"""
Print task

A task that logs whatever is given to it as an argument. Can be used
like any other task (under sequential, etc...).j

For example, the following would cause the strings "String" and "Another
string" to appear in the teuthology.log before and after the chef task
runs, respectively.

tasks:
- print: "String"
- chef: null
- print: "Another String"
"""

import logging

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Print out config argument in teuthology log/output
    """
    log.info('{config}'.format(config=config))
