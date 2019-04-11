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

    We are shipping the current contents of in tree rgw_scheduler_tests folder
    to the remotes, one of the main reasons for doing this and not actually
    calling the rest apis from the teuthology machine itself is that, in the
    current form we are dependant on the admin socket to get the perf counter
    information on knowing if any of the limit/throttle counters were hit, an
    ability to retreive perf counters remotely will avoid the need to ship this
    to every remote and just run the tests directly from the teuthology machine
    itself. (also the fact that tests are py3 only atm due to asyncio
    requirements means that atleast that also has to be solved)

    """
    assert isinstance(config, dict)
    log.info('Downloading ceph...')
    testdir = teuthology.get_testdir(ctx)
    local_dir, _ = os.path.split(os.path.realpath(__file__))
    local_dir += "/rgw_scheduler_tests/"
    remote_dir = "{tdir}/rgw-scheduler-tests".format(tdir=testdir)
    try:
        # FIXME:
        # this is less expensive than cloning the entire repo
        # Also eventually modify workunits to do the same instead of a
        # full git clone
        for client in config:
            (remote,) = ctx.cluster.only(client).remotes.keys()
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
                    remote_dir
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
        scheduler_tests_conf = config['rgw_scheduler_tests_conf'][client]
        scheduler_tests_conf.setdefault('DEFAULT', {})
        for section, user in users.iteritems():
            _config_user(scheduler_tests_conf, section, '{user}.{client}'.format(user=user, client=client))
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
                    '--uid', scheduler_tests_conf[section]['user_id'],
                    '--display-name', scheduler_tests_conf[section]['display_name'],
                    '--access-key', scheduler_tests_conf[section]['access_key'],
                    '--secret', scheduler_tests_conf[section]['secret_key'],
                    '--email', scheduler_tests_conf[section]['email'],
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
    yield

@contextlib.contextmanager
def run_tests(ctx, config):
    """
    Run the s3tests after everything is set up.

    :param ctx: Context passed to task
    :param config: specific configuration information
    """
    assert isinstance(config, dict)
    testdir = teuthology.get_testdir(ctx)
    for client, client_config in config.iteritems():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        args = [
            'cd',
            '{tdir}/rgw-scheduler-tests'.format(tdir=testdir),
            run.Raw('&&'),
            run.Raw('TEST_CONF={tdir}/archive/rgw-scheduler-tests.{client}.conf'.format(tdir=testdir, client=client)),
            'sudo',   # needed for admin socket reading
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
    Run the rgw-scheduler-tests suite against rgw.

    We don't yet default construct the different type of requests, so the yaml
    has to be created similar to eg conf.

    eg. config:
      - rgw_scheduler_tests:
      client.0:
        default:
          req_count: 25
          create_buckets: bucket1, bucket2
        requests:
            create_obj1:
              req_type: PUT
              req_path: bucket1/foo
              per_req_path: true
              obj_size: 1
            get_bucket1:
              req_type: GET
              req_path: bucket2

    """
    assert hasattr(ctx, 'rgw'), 'rgw scheduler tests must run after the rgw task'
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task only supports a list or dictionary for configuration"
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

    log.debug('rgw scheduler test config is %s', config)

    rgw_scheduler_tests_conf = {}
    for client in clients:
        cluster_name, daemon_type, client_id = teuthology.split_role(client)
        client_with_id = daemon_type + '.' + client_id
        client_with_cluster = cluster_name + '.' + client_with_id

        endpoint = ctx.rgw.role_endpoints.get(client)
        assert endpoint, 's3tests: no rgw endpoint for {}'.format(client)
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
