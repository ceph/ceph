import contextlib
import logging
import os

from teuthology import misc as teuthology
from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Mount/unmount a cifs client.

    The config is optional and defaults to mounting on all clients. If
    a config is given, it is expected to be a list of clients to do
    this operation on.

    Example that starts smbd and mounts cifs on all nodes::

        tasks:
        - ceph:
        - samba:
        - cifs-mount:
        - interactive:

    Example that splits smbd and cifs:

        tasks:
        - ceph:
        - samba: [samba.0]
        - cifs-mount: [client.0]
        - ceph-fuse: [client.1]
        - interactive:

    Example that specifies the share name:

        tasks:
        - ceph:
        - ceph-fuse:
        - samba:
            samba.0:
                cephfuse: "{testdir}/mnt.0"
        - cifs-mount:
            client.0:
                share: cephfuse
    """
    log.info('Mounting cifs clients...')

    if config is None:
        config = dict(('client.{id}'.format(id=id_), None)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    clients = list(teuthology.get_clients(ctx=ctx, roles=config.keys()))

    from teuthology.task.samba import get_sambas
    samba_roles = ['samba.{id_}'.format(id_=id_) for id_ in teuthology.all_roles_of_type(ctx.cluster, 'samba')]
    sambas = list(get_sambas(ctx=ctx, roles=samba_roles))
    (ip, port) = sambas[0][1].ssh.get_transport().getpeername()
    log.info('samba ip: {ip}'.format(ip=ip))

    for id_, remote in clients:
        mnt = os.path.join(teuthology.get_testdir(ctx), 'mnt.{id}'.format(id=id_))
        log.info('Mounting cifs client.{id} at {remote} {mnt}...'.format(
                id=id_, remote=remote,mnt=mnt))

        remote.run(
            args=[
                'mkdir',
                '--',
                mnt,
                ],
            )

        rolestr = 'client.{id_}'.format(id_=id_)
        unc = "ceph"
        log.info("config: {c}".format(c=config))
        if config[rolestr] is not None and 'share' in config[rolestr]:
            unc = config[rolestr]['share']

        remote.run(
            args=[
                'sudo',
                'mount',
                '-t',
                'cifs',
                '//{sambaip}/{unc}'.format(sambaip=ip, unc=unc),
                '-o',
                'username=ubuntu,password=ubuntu',
                mnt,
                ],
            )

        remote.run(
            args=[
                'sudo',
                'chown',
                'ubuntu:ubuntu',
                '{m}/'.format(m=mnt),
                ],
            )

    try:
        yield
    finally:
        log.info('Unmounting cifs clients...')
        for id_, remote in clients:
            remote.run(
                args=[
                    'sudo',
                    'umount',
                    mnt,
                    ],
                )
        for id_, remote in clients:
            while True:
                try:
                    remote.run(
                        args=[
                            'rmdir', '--', mnt,
                            run.Raw('2>&1'),
                            run.Raw('|'),
                            'grep', 'Device or resource busy',
                            ],
                        )
                    import time
                    time.sleep(1)
                except:
                    break
