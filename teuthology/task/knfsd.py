"""
Export/Unexport a ``nfs server`` client.
"""
import contextlib
import logging
import os

from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def get_nfsd_args(remote, cmd):
    args=[
        'sudo',
        'service',
        'nfs',
        cmd,
        ]
    if remote.os.package_type == 'deb':
        args[2] = 'nfs-kernel-server'
    return args

@contextlib.contextmanager
def task(ctx, config):
    """
    Export/Unexport a ``nfs server`` client.

    The config is optional and defaults to exporting on all clients. If
    a config is given, it is expected to be a list or dict of clients to do
    this operation on. You must have specified ``ceph-fuse`` or
    ``kclient`` on all clients specified for knfsd.

    Example that exports all clients::

        tasks:
        - ceph:
        - kclient:
        - knfsd:
        - interactive:

    Example that uses both ``kclient` and ``ceph-fuse``::

        tasks:
        - ceph:
        - ceph-fuse: [client.0]
        - kclient: [client.1]
        - knfsd: [client.0, client.1]
        - interactive:

    Example that specifies export options::

        tasks:
        - ceph:
        - kclient: [client.0, client.1]
        - knfsd:
            client.0:
              options: [rw,root_squash]
            client.1:
        - interactive:

    Note that when options aren't specified, rw,no_root_squash is the default.
    When you specify options, the defaults are as specified by exports(5).

    So if empty options are specified, i.e. options: [] these are the defaults:
        ro,sync,wdelay,hide,nocrossmnt,secure,root_squash,no_all_squash,
        no_subtree_check,secure_locks,acl,anonuid=65534,anongid=65534

    :param ctx: Context
    :param config: Configuration
    """
    log.info('Exporting nfs server...')

    if config is None:
        config = dict(('client.{id}'.format(id=id_), None)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    clients = list(teuthology.get_clients(ctx=ctx, roles=config.keys()))

    for id_, remote in clients:
        mnt = os.path.join(teuthology.get_testdir(ctx), 'mnt.{id}'.format(id=id_))
        client_config = config.get("client.%s" % id_)
        if client_config is None:
            client_config = {}
        log.debug("Client client.%s config is %s" % (id_, client_config))

        if client_config.get('options') is not None:
            opts = ','.join(client_config.get('options'))
        else:
            opts = 'rw,no_root_squash'

        # Undocumented option to export to any client in case
        # testing in interactive mode from other unspecified clients.
        wildcard = False
        if client_config.get('wildcard') is not None:
            wildcard = True
        
        log.info('Exporting knfsd client.{id} at {remote} *:{mnt} ({opt})...'.format(
                id=id_, remote=remote, mnt=mnt, opt=opts))

        """
        Should the user want to run with root_squash enabled, there is no
        way to write anything to the initial ceph root dir which is set to
        rwxr-xr-x root root.

        This could possibly break test cases that make assumptions about
        the initial state of the root dir.
        """
        remote.run(
            args=[
                'sudo',
                'chmod',
                "777",
                '{MNT}'.format(MNT=mnt),
                ],
            )
        """
        Start NFS kernel server
        """
        remote.run( args=get_nfsd_args(remote, 'restart') )
        args=[
            'sudo',
            "exportfs",
            '-o',
            'fsid=123{id},{opt}'.format(id=id_,opt=opts),
            ]

        if wildcard:
            args += ['*:{MNT}'.format(MNT=mnt)]
        else:
            """
            DEFAULT
            Prevent bogus clients from old runs from access our 
            export.  Specify all specify node addresses for this run.
            """
            ips = [host for (host, _) in (remote.ssh.get_transport().getpeername() for (remote, roles) in ctx.cluster.remotes.items())]
            for ip in ips:
                args += [ '{ip}:{MNT}'.format(ip=ip, MNT=mnt) ]

        log.info('remote run {args}'.format(args=args))
        remote.run( args=args )

    try:
        yield
    finally:
        log.info('Unexporting nfs server...')
        for id_, remote in clients:
            log.debug('Unexporting client client.{id}...'.format(id=id_))
            mnt = os.path.join(teuthology.get_testdir(ctx), 'mnt.{id}'.format(id=id_))
            try:
                log.debug('Checking active files on mount {mnt}'.format(mnt=mnt))
                remote.run(
                    args=[
                        'sudo',
                        'lsof', '-V', '+D',
                        '{mnt}'.format(mnt=mnt),
                        ],
                    check_status=False
                    )
            finally:
                log.debug('Stopping NFS server on client.{id}...'.format(id=id_))
                remote.run( args=get_nfsd_args(remote, 'stop') )
                log.debug('Syncing client client.{id}'.format(id=id_))
                remote.run(
                    args=[
                        'sync'
                        ]
                    )
