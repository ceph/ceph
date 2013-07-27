import logging
import contextlib

from teuthology import misc as teuthology
from teuthology import contextutil
from ..orchestra import run
from teuthology.task.ceph import CephState

log = logging.getLogger(__name__)

@contextlib.contextmanager
def run_rest_api_daemon(ctx, api_clients):
    if not hasattr(ctx, 'daemons'):
        ctx.daemons = CephState()
    remotes = ctx.cluster.only(teuthology.is_type('client')).remotes
    testdir = teuthology.get_testdir(ctx)
    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)
    for rems, roles in remotes.iteritems():
        for whole_id_ in roles:
            if whole_id_ in api_clients:
                id_ = whole_id_[len('clients'):]
                run_cmd = [
                    'sudo',
                    '{tdir}/daemon-helper'.format(tdir=testdir),
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
        for _, role_v in remotes.iteritems():
            for node in role_v:
                api_clients.append(node)
    else:
        for role_v in config:
            api_clients.append(role_v)
    log.info(api_clients)
    testdir = teuthology.get_testdir(ctx)
    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)
    for rems, roles in remotes.iteritems():
        for whole_id_ in roles:
            if whole_id_ in api_clients:
                id_ = whole_id_[len('clients'):]
                keyring = '/etc/ceph/ceph.client.rest{id}.keyring'.format(
                        id=id_)
                rems.run(
                    args=[
                        'sudo',
                        '{tdir}/adjust-ulimits'.format(tdir=testdir),
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

