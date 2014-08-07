"""
Run rgw roundtrip message tests
"""
from cStringIO import StringIO
import base64
import contextlib
import logging
import os
import random
import string
import yaml

from teuthology import misc as teuthology
from teuthology import contextutil
from ..config import config as teuth_config
from ..orchestra import run
from ..orchestra.connection import split_user

log = logging.getLogger(__name__)


@contextlib.contextmanager
def download(ctx, config):
    """
    Download the s3 tests from the git builder.
    Remove downloaded s3 file upon exit.
    
    The context passed in should be identical to the context
    passed in to the main task.
    """
    assert isinstance(config, list)
    log.info('Downloading s3-tests...')
    testdir = teuthology.get_testdir(ctx)
    for client in config:
        ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                teuth_config.ceph_git_base_url + 's3-tests.git',
                '{tdir}/s3-tests'.format(tdir=testdir),
                ],
            )
    try:
        yield
    finally:
        log.info('Removing s3-tests...')
        for client in config:
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/s3-tests'.format(tdir=testdir),
                    ],
                )

def _config_user(s3tests_conf, section, user):
    """
    Configure users for this section by stashing away keys, ids, and
    email addresses.
    """
    s3tests_conf[section].setdefault('user_id', user)
    s3tests_conf[section].setdefault('email', '{user}+test@test.test'.format(user=user))
    s3tests_conf[section].setdefault('display_name', 'Mr. {user}'.format(user=user))
    s3tests_conf[section].setdefault('access_key', ''.join(random.choice(string.uppercase) for i in xrange(20)))
    s3tests_conf[section].setdefault('secret_key', base64.b64encode(os.urandom(40)))

@contextlib.contextmanager
def create_users(ctx, config):
    """
    Create a default s3 user.
    """
    assert isinstance(config, dict)
    log.info('Creating rgw users...')
    testdir = teuthology.get_testdir(ctx)
    users = {'s3': 'foo'}
    for client in config['clients']:
        s3tests_conf = config['s3tests_conf'][client]
        s3tests_conf.setdefault('roundtrip', {})
        s3tests_conf['roundtrip'].setdefault('bucket', 'rttest-' + client + '-{random}-')
        s3tests_conf['roundtrip'].setdefault('readers', 10)
        s3tests_conf['roundtrip'].setdefault('writers', 3)
        s3tests_conf['roundtrip'].setdefault('duration', 300)
        s3tests_conf['roundtrip'].setdefault('files', {})
        rtconf = s3tests_conf['roundtrip']
        rtconf['files'].setdefault('num', 10)
        rtconf['files'].setdefault('size', 2000)
        rtconf['files'].setdefault('stddev', 500)
        for section, user in [('s3', 'foo')]:
            _config_user(s3tests_conf, section, '{user}.{client}'.format(user=user, client=client))
            ctx.cluster.only(client).run(
                args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'radosgw-admin',
                    '-n', client,
                    'user', 'create',
                    '--uid', s3tests_conf[section]['user_id'],
                    '--display-name', s3tests_conf[section]['display_name'],
                    '--access-key', s3tests_conf[section]['access_key'],
                    '--secret', s3tests_conf[section]['secret_key'],
                    '--email', s3tests_conf[section]['email'],
                ],
            )
    try:
        yield
    finally:
        for client in config['clients']:
            for user in users.itervalues():
                uid = '{user}.{client}'.format(user=user, client=client)
                ctx.cluster.only(client).run(
                    args=[
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'radosgw-admin',
                        '-n', client,
                        'user', 'rm',
                        '--uid', uid,
                        '--purge-data',
                        ],
                    )

@contextlib.contextmanager
def configure(ctx, config):
    """
    Configure the s3-tests.  This includes the running of the
    bootstrap code and the updating of local conf files.
    """
    assert isinstance(config, dict)
    log.info('Configuring s3-roundtrip-tests...')
    testdir = teuthology.get_testdir(ctx)
    for client, properties in config['clients'].iteritems():
        s3tests_conf = config['s3tests_conf'][client]
        if properties is not None and 'rgw_server' in properties:
            host = None
            for target, roles in zip(ctx.config['targets'].iterkeys(), ctx.config['roles']):
                log.info('roles: ' + str(roles))
                log.info('target: ' + str(target))
                if properties['rgw_server'] in roles:
                    _, host = split_user(target)
            assert host is not None, "Invalid client specified as the rgw_server"
            s3tests_conf['s3']['host'] = host
        else:
            s3tests_conf['s3']['host'] = 'localhost'

        def_conf = s3tests_conf['DEFAULT']
        s3tests_conf['s3'].setdefault('port', def_conf['port'])
        s3tests_conf['s3'].setdefault('is_secure', def_conf['is_secure'])

        (remote,) = ctx.cluster.only(client).remotes.keys()
        remote.run(
            args=[
                'cd',
                '{tdir}/s3-tests'.format(tdir=testdir),
                run.Raw('&&'),
                './bootstrap',
                ],
            )
        conf_fp = StringIO()
        conf = dict(
                        s3=s3tests_conf['s3'],
                        roundtrip=s3tests_conf['roundtrip'],
                    )
        yaml.safe_dump(conf, conf_fp, default_flow_style=False)
        teuthology.write_file(
            remote=remote,
            path='{tdir}/archive/s3roundtrip.{client}.config.yaml'.format(tdir=testdir, client=client),
            data=conf_fp.getvalue(),
            )
    yield


@contextlib.contextmanager
def run_tests(ctx, config):
    """
    Run the s3 roundtrip after everything is set up.

    :param ctx: Context passed to task
    :param config: specific configuration information
    """
    assert isinstance(config, dict)
    testdir = teuthology.get_testdir(ctx)
    for client, client_config in config.iteritems():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        conf = teuthology.get_file(remote, '{tdir}/archive/s3roundtrip.{client}.config.yaml'.format(tdir=testdir, client=client))
        args = [
                '{tdir}/s3-tests/virtualenv/bin/s3tests-test-roundtrip'.format(tdir=testdir),
                ]
        if client_config is not None and 'extra_args' in client_config:
            args.extend(client_config['extra_args'])

        ctx.cluster.only(client).run(
            args=args,
            stdin=conf,
            )
    yield


@contextlib.contextmanager
def task(ctx, config):
    """
    Run the s3tests-test-roundtrip suite against rgw.

    To run all tests on all clients::

        tasks:
        - ceph:
        - rgw:
        - s3roundtrip:

    To restrict testing to particular clients::

        tasks:
        - ceph:
        - rgw: [client.0]
        - s3roundtrip: [client.0]

    To run against a server on client.1::

        tasks:
        - ceph:
        - rgw: [client.1]
        - s3roundtrip:
            client.0:
              rgw_server: client.1

    To pass extra test arguments

        tasks:
        - ceph:
        - rgw: [client.0]
        - s3roundtrip:
            client.0:
              roundtrip:
                bucket: mybucket
                readers: 10
                writers: 3
                duration: 600
                files:
                  num: 10
                  size: 2000
                  stddev: 500
            client.1:
              ...

    To override s3 configuration

        tasks:
        - ceph:
        - rgw: [client.0]
        - s3roundtrip:
            client.0:
              s3:
                user_id: myuserid
                display_name: myname
                email: my@email
                access_key: myaccesskey
                secret_key: mysecretkey

    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task s3tests only supports a list or dictionary for configuration"
    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)
    clients = config.keys()

    s3tests_conf = {}
    for client in clients:
        if config[client] is None:
            config[client] = {}
        config[client].setdefault('s3', {})
        config[client].setdefault('roundtrip', {})

        s3tests_conf[client] = ({
                'DEFAULT':
                    {
                    'port'      : 7280,
                    'is_secure' : False,
                    },
                'roundtrip' : config[client]['roundtrip'],
                's3'  : config[client]['s3'],
                })

    with contextutil.nested(
        lambda: download(ctx=ctx, config=clients),
        lambda: create_users(ctx=ctx, config=dict(
                clients=clients,
                s3tests_conf=s3tests_conf,
                )),
        lambda: configure(ctx=ctx, config=dict(
                clients=config,
                s3tests_conf=s3tests_conf,
                )),
        lambda: run_tests(ctx=ctx, config=config),
        ):
        pass
    yield
