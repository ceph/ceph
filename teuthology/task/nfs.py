"""
Nfs client tester
"""
import contextlib
import logging
import os

from teuthology import misc as teuthology

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Mount nfs client (requires nfs server export like knfsd or ganesh)

    Example that mounts a single nfs client::

        tasks:
        - ceph:
        - kclient: [client.0]
        - knfsd: [client.0]
        - nfs:
            client.1:
                server: client.0
        - interactive:

    Example that mounts multiple nfs clients with options::

        tasks:
        - ceph:
        - kclient: [client.0, client.1]
        - knfsd: [client.0, client.1]
        - nfs:
            client.2:
                server: client.0
                options: [rw,hard,intr,nfsvers=3]
            client.3:
                server: client.1
                options: [ro]
        - workunit:
            clients:
                client.2:
                    - suites/dbench.sh
                client.3:
                    - suites/blogbench.sh

    It is not recommended that the nfs client and nfs server reside on the same node.  So in the example above client.0-3 should be on 4 distinct
    nodes.  The client nfs testing would be using only client.2 and client.3.
    """
    log.info('Mounting nfs clients...')
    assert isinstance(config, dict)

    clients = list(teuthology.get_clients(ctx=ctx, roles=config.keys()))

    testdir = teuthology.get_testdir(ctx)
    for id_, remote in clients:
        mnt = os.path.join(testdir, 'mnt.{id}'.format(id=id_))
        client_config = config.get("client.%s" % id_)
        if client_config is None:
            client_config = {}
        log.debug("Client client.%s config is %s" % (id_, client_config))

        assert client_config.get('server') is not None
        server = client_config.get('server');

        svr_id = server[len('client.'):]
        svr_mnt = os.path.join(testdir, 'mnt.{id}'.format(id=svr_id))

        svr_remote  = None
        all_config = ['client.{id}'.format(id=tmpid)
                  for tmpid in teuthology.all_roles_of_type(ctx.cluster, 'client')]
        all_clients = list(teuthology.get_clients(ctx=ctx, roles=all_config))
        for tmpid, tmpremote in all_clients:
            if tmpid == svr_id:
                svr_remote = tmpremote
                break

        assert svr_remote is not None 
        svr_remote = svr_remote.name.split('@', 2)[1]

        if client_config.get('options') is not None:
            opts = ','.join(client_config.get('options'))
        else:
            opts = 'rw'

        log.info('Mounting client.{id} from client.{sid}'.format(id=id_, sid=svr_id))
        log.debug('mount -o {opts} {remote}:{svr_mnt} {mnt}'.format(
                remote=svr_remote, svr_mnt=svr_mnt, opts=opts, mnt=mnt))

        remote.run(
            args=[
                'mkdir',
                '--',
                mnt,
                ],
            )

        remote.run(
            args=[
                'sudo',
                "mount",
                "-o",
                opts,
                '{remote}:{mnt}'.format(remote=svr_remote, mnt=svr_mnt),
                mnt
                ],
            )

    try:
        yield
    finally:
        log.info('Unmounting nfs clients...')
        for id_, remote in clients:
            log.debug('Unmounting nfs client client.{id}...'.format(id=id_))
            mnt = os.path.join(testdir, 'mnt.{id}'.format(id=id_))
            try:
                log.debug('First, syncing client client.{id}'.format(id=id_))
                remote.run(
                    args=[
                        'sync'
                        ]
                    )
                remote.run(
                    args=[
                        'sudo',
                        'lsof', '-V', '+D',
                        '{mnt}'.format(mnt=mnt),
                        ],
                    check_status=False
                    )
            finally:
                remote.run(
                    args=[
                        'sudo',
                        'umount',
                        mnt,
                        ],
                    )
                remote.run(
                    args=[
                        'rmdir',
                        '--',
                        mnt,
                        ],
                    )
