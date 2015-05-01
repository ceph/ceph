import logging
import time

log = logging.getLogger(__name__)

def sleep(ctx, config):
    to_sleep = config.get("to_sleep", 5)
    log.info("Sleeping for {to_sleep}".format(to_sleep=to_sleep))
    time.sleep(to_sleep)
