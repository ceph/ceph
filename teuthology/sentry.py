import logging
from raven import Client
from .config import config

log = logging.getLogger(__name__)

client = None


def get_client():
    global client
    if client:
        log.debug("Found client, reusing")
        return client

    log.debug("Getting sentry client")
    dsn = config.sentry_dsn
    if dsn:
        client = Client(dsn=dsn)
        return client
