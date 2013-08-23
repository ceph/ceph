import logging
from raven import Client
from .config import config

log = logging.getLogger(__name__)

client = None


def get_client():
    global client
    if client:
        return client

    dsn = config.sentry_dsn
    if dsn:
        client = Client(dsn=dsn)
        return client
