"""
Run a set of s3 tests on rgw.
"""
from cStringIO import StringIO
from configobj import ConfigObj
import base64
import contextlib
import logging
import os
import random
import string

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.config import config as teuth_config
from teuthology.orchestra import run
from teuthology.orchestra.connection import split_user

log = logging.getLogger(__name__)

@contextlib.contextmanager
def download(ctx, config):
    """
    TODO: run the tests directly when we move to py3, this copy/git thing is
    only around as teuthology is not py3 ready yet

    """
    assert isinstance(config, dict)
    log.info('Downloading ceph...')
    testdir = teuthology.get_testdir(ctx)
    local_dir, _ = os.path.split(os.path.realpath(__file__))
    local_dir += "/rgw_scheduler_tests/"
    remote_dir = "{tdir}/rgw-scheduler-tests".format(tdir=testdir)
    try:
        # TODO:
        # hopefully this is less expensive than cloning the entire repo
        # if this trick works, modify workunits to do the same instead of a
        # full clone
        for client in config:
            (remote,) = ctx.cluster.only(client).remotes.keys()
            log.info('abhi, k:=%s' % remote.name)
            teuthology.sh("scp -r {local_dir} {host}:{remote_dir}".format(
                local_dir=local_dir,
                host=remote.name,
                remote_dir=remote_dir
            ))
        yield
    finally:
        log.info('Removing s3-tests...')
        testdir = teuthology.get_testdir(ctx)
        for client in config:
            ctx.cluster.only(client).run(
                args=[
                    'sudo',
                    'rm',
                    '-rf',
                    '{tdir}/rgw-scheduler-tests'.format(tdir=testdir),
                    ],
                )


def _config_user(sched_tests_conf, section, user):
    """
    Configure users for this section by stashing away keys, ids, and
    email addresses.
    """
    sched_tests_conf[section].setdefault('access_key', ''.join(random.choice(string.uppercase) for i in xrange(20)))
    sched_tests_conf[section].setdefault('secret_key', base64.b64encode(os.urandom(40)))
    sched_tests_conf[section].setdefault('user_id', user)
    sched_tests_conf[section].setdefault('email', '{user}+test@test.test'.format(user=user))
    sched_tests_conf[section].setdefault('display_name', 'Mr. {user}'.format(user=user))


@contextlib.contextmanager
def create_users(ctx, config):
    """
    Create a main and an alternate s3 user.
    """
    assert isinstance(config, dict)
    log.info('Creating rgw users...')
    testdir = teuthology.get_testdir(ctx)
    users = {'DEFAULT': 'foo'}
    for client in config['clients']:
        #log.info("abhi: client is ", client)
        s3tests_conf = config['rgw_scheduler_tests_conf'][client]
        s3tests_conf.setdefault('DEFAULT', {})
        for section, user in users.iteritems():
            _config_user(s3tests_conf, section, '{user}.{client}'.format(user=user, client=client))
            log.debug('Creating user {user} on {host}'.format(user=user, host=client))
            cluster_name, daemon_type, client_id = teuthology.split_role(client)
            client_with_id = daemon_type + '.' + client_id
            ctx.cluster.only(client).run(
                args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'radosgw-admin',
                    '-n', client_with_id,
                    'user', 'create',
                    '--uid', s3tests_conf[section]['user_id'],
                    '--display-name', s3tests_conf[section]['display_name'],
                    '--access-key', s3tests_conf[section]['access_key'],
                    '--secret', s3tests_conf[section]['secret_key'],
                    '--email', s3tests_conf[section]['email'],
                    '--cluster', cluster_name,
                ],
            )
    try:
        yield
    finally:
        for client in config['clients']:
            for user in users.itervalues():
                uid = '{user}.{client}'.format(user=user, client=client)
                cluster_name, daemon_type, client_id = teuthology.split_role(client)
                client_with_id = daemon_type + '.' + client_id
                ctx.cluster.only(client).run(
                    args=[
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'radosgw-admin',
                        '-n', client_with_id,
                        'user', 'rm',
                        '--uid', uid,
                        '--purge-data',
                        '--cluster', cluster_name,
                        ],
                    )


@contextlib.contextmanager
def configure(ctx, config):
    """
    Configure the s3-tests.  This includes the running of the
    bootstrap code and the updating of local conf files.
    """
    assert isinstance(config, dict)
    log.info('Configuring scheduler-tests...')
    testdir = teuthology.get_testdir(ctx)
    for client, properties in config['clients'].iteritems():
        scheduler_conf = config['rgw_scheduler_tests_conf'][client]
        # if properties is not None and 'rgw_server' in properties:
        #     log.debug("abhi", ctx.config['roles'])
        #     host = None
        #     for target, roles in zip(ctx.config['targets'].iterkeys(), ctx.config['roles']):
        #         log.info('roles: ' + str(roles))
        #         log.info('target: ' + str(target))
        #         if properties['rgw_server'] in roles:
        #             _, host = split_user(target)
        #     assert host is not None, "Invalid client specified as the rgw_server"
        #     scheduler_conf['DEFAULT']['baseurl'] = host
        # else:
        #     scheduler_conf['DEFAULT']['baseurl'] = 'localhost'

        if properties is not None and 'default' in properties:
            for key, val in properties['default'].items():
                scheduler_conf['DEFAULT'][key] = val

        if properties is not None and 'requests' in properties:
            for section, kvs in properties['requests'].items():
                scheduler_conf[section] = dict()
                for k,v in kvs.items():
                    scheduler_conf[section][k] = v

        (remote,) = ctx.cluster.only(client).remotes.keys()
        remote.run(
            args=[
                'cd',
                '{tdir}/rgw-scheduler-tests'.format(tdir=testdir),
                run.Raw('&&'),
                './bootstrap.sh',
                ],
            )
        conf_fp = StringIO()
        scheduler_conf.write(conf_fp)
        teuthology.write_file(
            remote=remote,
            path='{tdir}/archive/rgw-scheduler-tests.{client}.conf'.format(tdir=testdir, client=client),
            data=conf_fp.getvalue(),
            )

    try:
        yield
    finally:
        pass

@contextlib.contextmanager
def run_tests(ctx, config):
    """
    Run the s3tests after everything is set up.

    :param ctx: Context passed to task
    :param config: specific configuration information
    """
    assert isinstance(config, dict)
    testdir = teuthology.get_testdir(ctx)
    # civetweb > 1.8 && beast parsers are strict on rfc2616
    for client, client_config in config.iteritems():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        args = [
            'cd',
            '{tdir}/rgw-scheduler-tests'.format(tdir=testdir),
            run.Raw('&&'),
            run.Raw('TEST_CONF={tdir}/archive/rgw-scheduler-tests.{client}.conf'.format(tdir=testdir, client=client)),
            'sudo',
            '-E',
            '{tdir}/rgw-scheduler-tests/scheduler-venv/bin/pytest'.format(tdir=testdir),
            '-v',
            'tests'
        ]
        if client_config is not None and 'extra_args' in client_config:
            args.extend(client_config['extra_args'])

        remote.run(
            args=args,
            label="scheduler tests against rgw"
            )
    yield


@contextlib.contextmanager
def task(ctx, config):
    """
    Run the s3-tests suite against rgw.

    To run all tests on all clients::

        tasks:
        - ceph:
        - rgw:
        - s3tests:

    To restrict testing to particular clients::

        tasks:
        - ceph:
        - rgw: [client.0]
        - s3tests: [client.0]

    To run against a server on client.1 and increase the boto timeout to 10m::

        tasks:
        - ceph:
        - rgw: [client.1]
        - s3tests:
            client.0:
              rgw_server: client.1
              idle_timeout: 600

    To pass extra arguments to nose (e.g. to run a certain test)::

        tasks:
        - ceph:
        - rgw: [client.0]
        - s3tests:
            client.0:
              extra_args: ['test_s3:test_object_acl_grand_public_read']
            client.1:
              extra_args: ['--exclude', 'test_100_continue']
    """
    assert hasattr(ctx, 'rgw'), 's3tests must run after the rgw task'
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

    overrides = ctx.config.get('overrides', {})
    # merge each client section, not the top level.
    for client in config.iterkeys():
        if not config[client]:
            config[client] = {}
        teuthology.deep_merge(config[client], overrides.get('rgw_scheduler_tests', {}))

    log.debug('s3tests config is %s', config)

    rgw_scheduler_tests_conf = {}
    for client in clients:
        cluster_name, daemon_type, client_id = teuthology.split_role(client)
        client_with_id = daemon_type + '.' + client_id
        client_with_cluster = cluster_name + '.' + client_with_id

        endpoint = ctx.rgw.role_endpoints.get(client)
        assert endpoint, 's3tests: no rgw endpoint for {}'.format(client)
        #log.debug('abhi got endpoint', endpoint)
        rgw_scheduler_tests_conf[client] = ConfigObj(
            indent_type='',
            infile={
                'DEFAULT': {
                    'req_count': 100,
                    'base_url' : endpoint.url(),
                    'auth_type' : 's3',
                    'admin_sock_path': '/var/run/ceph/rgw.{client_with_cluster}.asok'.format(client_with_cluster=client_with_cluster)
                }
                }
            )

    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: create_users(ctx=ctx, config=dict(
                clients=clients,
                rgw_scheduler_tests_conf=rgw_scheduler_tests_conf,
                )),
        lambda: configure(ctx=ctx, config=dict(
                clients=config,
                rgw_scheduler_tests_conf=rgw_scheduler_tests_conf,
                )),
        lambda: run_tests(ctx=ctx, config=config),
        ):
        pass
    yield
