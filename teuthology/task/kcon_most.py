import contextlib
import logging

from teuthology import misc as teuthology

log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    """
    Enable most ceph console logging

    Example that enables logging on all clients::

        tasks:
        - ceph:
        - kclient:
        - kcon_most
        - interactive:

    Example that enables logging only on the client using kclient::

        tasks:
        - ceph:
        - kclient: [client.0]
        - kcon_most [client.0]
        - interactive:
    """
    log.info('Enable additional kernel logging...')
    assert config is None or isinstance(config, list), \
        "task kcon_most got invalid config"

    if config is None:
        config = ['client.{id}'.format(id=id_)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    clients = list(teuthology.get_clients(ctx=ctx, roles=config))

    for id_, remote in clients:
        # TODO: Don't have to run this more than once per node (remote)
        log.info('Enable logging on client.{id} at {remote} ...'.format(
                id=id_, remote=remote))
        remote.run(
            args=[
                'sudo',
                'kcon_most',
                'on'
                ],
            )

    try:
        yield
    finally:
        log.info('Disable extra kernel logging on clients...')
        for id_, remote in clients:
            log.debug('Disable extra kernel logging on client.{id}...'.format(id=id_))
            remote.run(
                args=[
                    'sudo',
                    'kcon_most',
                    'off'
                    ],
                )
