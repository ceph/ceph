"""
immutable object cache task
"""
import contextlib
import logging

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def immutable_object_cache(ctx, config):
    """
    setup and cleanup immutable object cache
    """
    log.info("start immutable object cache daemon")
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
                'ceph-immutable-object-cache', '-b',
                ]
            )
    try:
        yield
    finally:
        log.info("cleanup immutable object cache")
        for client, client_config in config.items():
            client_config = client_config if client_config is not None else dict()
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(
                args=[
                    'sudo', 'killall', '-s', '9', 'ceph-immutable-object-cache', run.Raw('||'), 'true',
                    ]
                )
            cache_path = client_config.get('immutable object cache path', '/tmp/ceph-immutable-object-cache')
            remote.run(
                args=[
                    'sudo', 'rm', '-rf', cache_path, run.Raw('||'), 'true',
                    ]
                )

@contextlib.contextmanager
def task(ctx, config):
    """
    This is task for start immutable_object_cache.
    """
    assert isinstance(config, dict), \
           "task immutable_object_cache only supports a dictionary for configuration"

    managers = []
    config = teuthology.replace_all_with_clients(ctx.cluster, config)
    managers.append(
        lambda: immutable_object_cache(ctx=ctx, config=config)
        )

    with contextutil.nested(*managers):
        yield
