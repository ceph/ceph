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
        cluster_name, daemon_type, client_id = teuthology.split_role(client)
        client_with_id = daemon_type + '.' + client_id
        client_with_cluster = cluster_name + '.' + client_with_id
        # make sure that there is one immutable object cache daemon on the same node.
        remote.run(
            args=[
                'sudo', 'killall', '-s', '9', 'ceph-immutable-object-cache', run.Raw('||'), 'true',
                ]
            )
        remote.run(
            args=[
                'sudo', 'ceph-immutable-object-cache', '-b',
                '--cluster', cluster_name,
                '--log-file', '/var/log/ceph/immutable-object-cache.{client_with_cluster}.log'.format(client_with_cluster=client_with_cluster)
                ]
            )
    try:
        yield
    finally:
        log.info("check and cleanup immutable object cache")
        for client, client_config in config.items():
            client_config = client_config if client_config is not None else dict()
            (remote,) = ctx.cluster.only(client).remotes.keys()
            cache_path = client_config.get('immutable object cache path', '/tmp/ceph-immutable-object-cache')
            ls_command = '"$(ls {} )"'.format(cache_path)
            remote.run(
                args=[
                    'test', '-n', run.Raw(ls_command),
                    ]
                )
            remote.run(
                args=[
                    'sudo', 'killall', '-s', '9', 'ceph-immutable-object-cache', run.Raw('||'), 'true',
                    ]
                )
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
