"""
immutable object cache thrash task
"""
from io import StringIO

import contextlib
import logging
import os
import yaml
import time

from teuthology import misc as teuthology
from teuthology import contextutil
from tasks import rbd
from teuthology.orchestra import run
from teuthology.config import config as teuth_config

DEFAULT_KILL_DAEMON_TIME = 2
DEFAULT_DEAD_TIME = 30
DEFAULT_LIVE_TIME = 120

log = logging.getLogger(__name__)

@contextlib.contextmanager
def thrashes_immutable_object_cache_daemon(ctx, config):
    """
    thrashes immutable object cache daemon.
    It can test reconnection feature of RO cache when RO daemon crash
    TODO : replace sleep with better method.
    """
    log.info("thrashes immutable object cache daemon")

    # just thrash one rbd client.
    client, client_config = list(config.items())[0]
    (remote,) = ctx.cluster.only(client).remotes.keys()
    client_config = client_config if client_config is not None else dict()
    kill_daemon_time = client_config.get('kill_daemon_time', DEFAULT_KILL_DAEMON_TIME)
    dead_time = client_config.get('dead_time', DEFAULT_DEAD_TIME)
    live_time = client_config.get('live_time', DEFAULT_LIVE_TIME)

    for i in range(kill_daemon_time):
        log.info("ceph-immutable-object-cache crash....")
        remote.run(
            args=[
                'sudo', 'killall', '-s', '9', 'ceph-immutable-object-cache', run.Raw('||'), 'true',
                 ]
            )
        # librbd shoud normally run when ceph-immutable-object-cache
        remote.run(
            args=[
                'sleep', '{dead_time}'.format(dead_time=dead_time),
                 ]
            )
        # librbd should reconnect daemon
        log.info("startup ceph-immutable-object-cache")
        remote.run(
            args=[
                'ceph-immutable-object-cache', '-b',
                 ]
            )
        remote.run(
            args=[
                'sleep', '{live_time}'.format(live_time=live_time),
                 ]
            )
    try:
        yield
    finally:
        log.info("cleanup")

@contextlib.contextmanager
def task(ctx, config):
    """
    This is task for testing immutable_object_cache thrash.
    """
    assert isinstance(config, dict), \
            "task immutable_object_cache_thrash only supports a dictionary for configuration"

    managers = []
    config = teuthology.replace_all_with_clients(ctx.cluster, config)
    managers.append(
        lambda: thrashes_immutable_object_cache_daemon(ctx=ctx, config=config)
        )

    with contextutil.nested(*managers):
        yield
