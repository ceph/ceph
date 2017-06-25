"""
Test Swift API
"""
from cStringIO import StringIO
from configobj import ConfigObj
import base64
import contextlib
import logging
import os

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.config import config as teuth_config
from teuthology.orchestra import run
from teuthology.orchestra.connection import split_user

log = logging.getLogger(__name__)


@contextlib.contextmanager
def download(ctx, config):
    """
    Download the Swift API.
    """
    testdir = teuthology.get_testdir(ctx)
    assert isinstance(config, list)
    log.info('Downloading swift...')
    for client in config:
        ctx.cluster.only(client).run(
            args=[
                'git',
                'clone',
                '--branch',
                'ceph-jewel',
                teuth_config.ceph_git_base_url + 'swift.git',
                '{tdir}/swift'.format(tdir=testdir),
                ],
            )
    try:
        yield
    finally:
        log.info('Removing swift...')
        testdir = teuthology.get_testdir(ctx)
        for client in config:
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/swift'.format(tdir=testdir),
                    ],
                )

def _config_user(testswift_conf, account, user, suffix):
    """
    Configure a swift user

    :param account: Swift account
    :param user: User name
    :param suffix: user name and email suffixes.
    """
    testswift_conf['func_test'].setdefault('account{s}'.format(s=suffix), account)
    testswift_conf['func_test'].setdefault('username{s}'.format(s=suffix), user)
    testswift_conf['func_test'].setdefault('email{s}'.format(s=suffix), '{account}+test@test.test'.format(account=account))
    testswift_conf['func_test'].setdefault('display_name{s}'.format(s=suffix), 'Mr. {account} {user}'.format(account=account, user=user))
    testswift_conf['func_test'].setdefault('password{s}'.format(s=suffix), base64.b64encode(os.urandom(40)))

@contextlib.contextmanager
def create_users(ctx, config):
    """
    Create rgw users to interact with the swift interface.
    """
    assert isinstance(config, dict)
    log.info('Creating rgw users...')
    testdir = teuthology.get_testdir(ctx)
    users = {'': 'foo', '2': 'bar'}
    for client in config['clients']:
        cluster_name, daemon_type, client_id = teuthology.split_role(client)
        testswift_conf = config['testswift_conf'][client]
        for suffix, user in users.iteritems():
            _config_user(testswift_conf, '{user}.{client}'.format(user=user, client=client), user, suffix)
            ctx.cluster.only(client).run(
                args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'radosgw-admin',
                    '-n', client,
                    '--cluster', cluster_name,
                    'user', 'create',
                    '--subuser', '{account}:{user}'.format(account=testswift_conf['func_test']['account{s}'.format(s=suffix)],user=user),
                    '--display-name', testswift_conf['func_test']['display_name{s}'.format(s=suffix)],
                    '--secret', testswift_conf['func_test']['password{s}'.format(s=suffix)],
                    '--email', testswift_conf['func_test']['email{s}'.format(s=suffix)],
                    '--key-type', 'swift',
                    '--access', 'full',
                ],
            )
    try:
        yield
    finally:
        for client in config['clients']:
            for user in users.itervalues():
                uid = '{user}.{client}'.format(user=user, client=client)
                cluster_name, daemon_type, client_id = teuthology.split_role(client)
                ctx.cluster.only(client).run(
                    args=[
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'radosgw-admin',
                        '-n', client,
                        '--cluster', cluster_name,
                        'user', 'rm',
                        '--uid', uid,
                        '--purge-data',
                        ],
                    )

@contextlib.contextmanager
def configure(ctx, config):
    """
    Configure rgw and Swift
    """
    assert isinstance(config, dict)
    log.info('Configuring testswift...')
    testdir = teuthology.get_testdir(ctx)
    for client, properties in config['clients'].iteritems():
        log.info('client={c}'.format(c=client))
        log.info('config={c}'.format(c=config))
        testswift_conf = config['testswift_conf'][client]
        if properties is not None and 'rgw_server' in properties:
            host = None
            for target, roles in zip(ctx.config['targets'].iterkeys(), ctx.config['roles']):
                log.info('roles: ' + str(roles))
                log.info('target: ' + str(target))
                if properties['rgw_server'] in roles:
                    _, host = split_user(target)
            assert host is not None, "Invalid client specified as the rgw_server"
            testswift_conf['func_test']['auth_host'] = host
        else:
            testswift_conf['func_test']['auth_host'] = 'localhost'

        log.info(client)
        (remote,) = ctx.cluster.only(client).remotes.keys()
        remote.run(
            args=[
                'cd',
                '{tdir}/swift'.format(tdir=testdir),
                run.Raw('&&'),
                './bootstrap',
                ],
            )
        conf_fp = StringIO()
        testswift_conf.write(conf_fp)
        teuthology.write_file(
            remote=remote,
            path='{tdir}/archive/testswift.{client}.conf'.format(tdir=testdir, client=client),
            data=conf_fp.getvalue(),
            )
    yield


@contextlib.contextmanager
def run_tests(ctx, config):
    """
    Run an individual Swift test.
    """
    assert isinstance(config, dict)
    testdir = teuthology.get_testdir(ctx)
    for client, client_config in config.iteritems():
        args = [
                'SWIFT_TEST_CONFIG_FILE={tdir}/archive/testswift.{client}.conf'.format(tdir=testdir, client=client),
                '{tdir}/swift/virtualenv/bin/nosetests'.format(tdir=testdir),
                '-w',
                '{tdir}/swift/test/functional'.format(tdir=testdir),
                '-v',
                '-a', '!fails_on_rgw',
                ]
        if client_config is not None and 'extra_args' in client_config:
            args.extend(client_config['extra_args'])

        ctx.cluster.only(client).run(
            args=args,
            )
    yield

@contextlib.contextmanager
def task(ctx, config):
    """
    Run the testswift suite against rgw.

    To run all tests on all clients::

        tasks:
        - ceph:
        - rgw:
        - testswift:

    To restrict testing to particular clients::

        tasks:
        - ceph:
        - rgw: [client.0]
        - testswift: [client.0]

    To run against a server on client.1::

        tasks:
        - ceph:
        - rgw: [client.1]
        - testswift:
            client.0:
              rgw_server: client.1

    To pass extra arguments to nose (e.g. to run a certain test)::

        tasks:
        - ceph:
        - rgw: [client.0]
        - testswift:
            client.0:
              extra_args: ['test.functional.tests:TestFileUTF8', '-m', 'testCopy']
            client.1:
              extra_args: ['--exclude', 'TestFile']
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task testswift only supports a list or dictionary for configuration"
    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)
    clients = config.keys()

    log.info('clients={c}'.format(c=clients))

    testswift_conf = {}
    for client in clients:
        testswift_conf[client] = ConfigObj(
                indent_type='',
                infile={
                    'func_test':
                        {
                        'auth_port'      : 7280,
                        'auth_ssl' : 'no',
                        'auth_prefix' : '/auth/',
                        },
                    }
                )

    with contextutil.nested(
        lambda: download(ctx=ctx, config=clients),
        lambda: create_users(ctx=ctx, config=dict(
                clients=clients,
                testswift_conf=testswift_conf,
                )),
        lambda: configure(ctx=ctx, config=dict(
                clients=config,
                testswift_conf=testswift_conf,
                )),
        lambda: run_tests(ctx=ctx, config=config),
        ):
        pass
    yield
