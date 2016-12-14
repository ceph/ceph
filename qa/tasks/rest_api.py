"""
Rest Api
"""
import logging
import contextlib
import time

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run
from teuthology.orchestra.daemon import DaemonGroup

log = logging.getLogger(__name__)


@contextlib.contextmanager
def run_rest_api_daemon(ctx, api_clients):
    """
    Wrapper starts the rest api daemons
    """
    if not hasattr(ctx, 'daemons'):
        ctx.daemons = DaemonGroup()
    remotes = ctx.cluster.only(teuthology.is_type('client')).remotes
    for rems, roles in remotes.iteritems():
        for whole_id_ in roles:
            if whole_id_ in api_clients:
                id_ = whole_id_[len('clients'):]
                run_cmd = [
                    'sudo',
                    'daemon-helper',
                    'kill',
                    'ceph-rest-api',
                    '-n',
                    'client.rest{id}'.format(id=id_), ]
                cl_rest_id = 'client.rest{id}'.format(id=id_)
                ctx.daemons.add_daemon(rems, 'restapi',
                    cl_rest_id,
                    args=run_cmd,
                    logger=log.getChild(cl_rest_id),
                    stdin=run.PIPE,
                    wait=False,
                    )
                for i in range(1, 12):
                    log.info('testing for ceph-rest-api try {0}'.format(i))
                    run_cmd = [
                        'wget',
                        '-O',
                        '/dev/null',
                        '-q',
                        'http://localhost:5000/api/v0.1/status'
                    ]
                    proc = rems.run(
                        args=run_cmd,
                        check_status=False
                    )
                    if proc.exitstatus == 0:
                        break
                    time.sleep(5)
                if proc.exitstatus != 0:
                    raise RuntimeError('Cannot contact ceph-rest-api')
    try:
        yield

    finally:
        """
        TO DO: destroy daemons started -- modify iter_daemons_of_role
        """
        teuthology.stop_daemons_of_type(ctx, 'restapi')

@contextlib.contextmanager
def task(ctx, config):
    """
    Start up rest-api.

    To start on on all clients::

        tasks:
        - ceph:
        - rest-api:

    To only run on certain clients::

        tasks:
        - ceph:
        - rest-api: [client.0, client.3]

    or

        tasks:
        - ceph:
        - rest-api:
            client.0:
            client.3:

    The general flow of things here is:
        1. Find clients on which rest-api is supposed to run (api_clients)
        2. Generate keyring values
        3. Start up ceph-rest-api daemons
    On cleanup:
        4. Stop the daemons
        5. Delete keyring value files.
    """
    api_clients = []
    remotes = ctx.cluster.only(teuthology.is_type('client')).remotes
    log.info(remotes)
    if config == None:
        api_clients = ['client.{id}'.format(id=id_)
            for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    else:
        api_clients = config
    log.info(api_clients)
    testdir = teuthology.get_testdir(ctx)
    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)
    for rems, roles in remotes.iteritems():
        for whole_id_ in roles:
            if whole_id_ in api_clients:
                id_ = whole_id_[len('client.'):]
                keyring = '/etc/ceph/ceph.client.rest{id}.keyring'.format(
                        id=id_)
                rems.run(
                    args=[
                        'sudo',
                        'adjust-ulimits',
                        'ceph-coverage',
                        coverage_dir,
                        'ceph-authtool',
                        '--create-keyring',
                        '--gen-key',
                        '--name=client.rest{id}'.format(id=id_),
                        '--set-uid=0',
                        '--cap', 'mon', 'allow *',
                        '--cap', 'osd', 'allow *',
                        '--cap', 'mds', 'allow',
                        keyring,
                        run.Raw('&&'),
                        'sudo',
                        'chmod',
                        '0644',
                        keyring,
                        ],
                    )
                rems.run(
                    args=[
                        'sudo',
                        'sh',
                        '-c',
                        run.Raw("'"),
                        "echo",
                        '[client.rest{id}]'.format(id=id_),
                        run.Raw('>>'),
                        "/etc/ceph/ceph.conf",
                        run.Raw("'")
                        ]
                    )
                rems.run(
                    args=[
                        'sudo',
                        'sh',
                        '-c',
                        run.Raw("'"),
                        'echo',
                        'restapi',
                        'keyring',
                        '=',
                        '/etc/ceph/ceph.client.rest{id}.keyring'.format(id=id_),
                        run.Raw('>>'),
                        '/etc/ceph/ceph.conf',
                        run.Raw("'"),
                        ]
                    )
                rems.run(
                    args=[
                        'sudo',
                        'ceph',
                        'auth',
                        'import',
                        '-i',
                        '/etc/ceph/ceph.client.rest{id}.keyring'.format(id=id_),
                    ]
                )
    with contextutil.nested(
            lambda: run_rest_api_daemon(ctx=ctx, api_clients=api_clients),):
        yield

