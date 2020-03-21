"""
imuutable object cache task
"""
from cStringIO import StringIO

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

from util.workunit import get_refspec_after_overrides

# based on qemu task function.
from qemu import create_images
from qemu import create_clones
from qemu import create_dirs
from qemu import generate_iso
from qemu import download_image
from qemu import run_qemu

DEFAULT_KILL_DAEMON_TIME = 2
DEFAULT_DEAD_TIME = 30
DEFAULT_LIVE_TIME = 120
DEFAULT_QEMU_BOOT_TIME = 120

log = logging.getLogger(__name__)

@contextlib.contextmanager
def immutable_object_cache(ctx, config):
    """
    setup and cleanup immutable object cache
    """
    for client, client_config in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        # make sure that there is one immutable object cache daemon on the same node. 
        remote.run(
            args=[
                'sudo', 'killall', '-s', '9', 'ceph-immutable-object-cache', run.Raw('||'), 'true',
                ]
            )
        remote.run(
            args=[
                'sudo', 'ceph-immutable-object-cache', '-b',
                ]
            )
    try:
        yield
    finally:
        for client, client_config in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(
                args=[
                    'sudo', 'killall', '-s', '9', 'ceph-immutable-object-cache', run.Raw('||'), 'true',
                    ]
                )
            cahce_path = client_config.get('immutable object cache path', '/tmp/ceph-immutable-object-cache')
            remote.run(
                args=[
                    'sudo', 'rm', '-rf', '/tmp/ceph-immutable-object-cache', run.Raw('||'), 'true',
                    ]
                )

@contextlib.contextmanager
def thrashes_immutable_object_cache_daemon(ctx, config):
    """
    thrashes immutable object cache daemon.
    It can test reconnection feature of RO cache when RO daemon crash
    TODO : replace sleep with better method.
    """
    log.info("thrashes immutable object cache daemon")

    # just thrash one rbd client.
    client, client_config = config.items()[0]
    (remote,) = ctx.cluster.only(client).remotes.keys()
    kill_daemon_time = client_config.get('kill_daemon_time', DEFAULT_KILL_DAEMON_TIME)
    dead_time = client_config.get('dead_time', DEFAULT_DEAD_TIME)
    live_time = client_config.get('live_time', DEFAULT_LIVE_TIME)
    wait_for_qemu_boot = client_config.get('qemu_boot_time', DEFAULT_QEMU_BOOT_TIME)

    # wait for all qemu startup
    log.info("ceph-immutable-object-cache speedup read request.....")
    remote.run(
        args=[
            'sleep', '{wait_for_qemu_boot}'.format(wait_for_qemu_boot=wait_for_qemu_boot),
             ]
        )
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
        log.info("cleanup thrashes immutable object cache")


@contextlib.contextmanager
def task(ctx, config):
    """
    based on implement of qemu task. 
    This is task for testing immutable_object_cache.
    """
    assert isinstance(config, dict), \
           "task qemu only supports a dictionary for configuration"

    config = teuthology.replace_all_with_clients(ctx.cluster, config)

    managers = []
    create_images(ctx=ctx, config=config, managers=managers)
    managers.extend([
        lambda: create_dirs(ctx=ctx, config=config),
        lambda: generate_iso(ctx=ctx, config=config),
        lambda: download_image(ctx=ctx, config=config),
        ])
    create_clones(ctx=ctx, config=config, managers=managers)
    managers.append(
        lambda: immutable_object_cache(ctx=ctx, config=config),
        )
    managers.append(
        lambda: run_qemu(ctx=ctx, config=config),
        )
    managers.append(
        lambda: thrashes_immutable_object_cache_daemon(ctx=ctx, config=config)
	)

    with contextutil.nested(*managers):
        yield
