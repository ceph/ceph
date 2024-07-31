"""
Run a set of s3 tests on rgw.
"""
from io import BytesIO
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

log = logging.getLogger(__name__)


def get_ragweed_branches(config, client_conf):
    """
    figure out the ragweed branch according to the per-client settings

    use force-branch is specified, and fall back to the ones deduced using ceph
    branch under testing
    """
    force_branch = client_conf.get('force-branch', None)
    if force_branch:
        return [force_branch]
    else:
        S3_BRANCHES = ['master', 'nautilus', 'mimic',
                       'luminous', 'kraken', 'jewel']
        ceph_branch = config.get('branch')
        suite_branch = config.get('suite_branch', ceph_branch)
        if suite_branch in S3_BRANCHES:
            branch = client_conf.get('branch', 'ceph-' + suite_branch)
        else:
            branch = client_conf.get('branch', suite_branch)
        default_branch = client_conf.get('default-branch', None)
        if default_branch:
            return [branch, default_branch]
        else:
            return [branch]

def get_ragweed_dir(testdir, client):
    return '{}/ragweed.{}'.format(testdir, client)

@contextlib.contextmanager
def download(ctx, config):
    """
    Download the s3 tests from the git builder.
    Remove downloaded s3 file upon exit.

    The context passed in should be identical to the context
    passed in to the main task.
    """
    assert isinstance(config, dict)
    log.info('Downloading ragweed...')
    testdir = teuthology.get_testdir(ctx)
    for (client, cconf) in config.items():
        ragweed_dir = get_ragweed_dir(testdir, client)
        ragweed_repo = ctx.config.get('ragweed_repo',
                                      teuth_config.ceph_git_base_url + 'ragweed.git')
        for branch in get_ragweed_branches(ctx.config, cconf):
            log.info("Using branch '%s' for ragweed", branch)
            try:
                ctx.cluster.only(client).sh(
                    script=f'git clone -b {branch} {ragweed_repo} {ragweed_dir}')
                break
            except Exception as e:
                exc = e
        else:
            raise exc

        sha1 = cconf.get('sha1')
        if sha1 is not None:
            ctx.cluster.only(client).run(
                args=[
                    'cd', ragweed_dir,
                    run.Raw('&&'),
                    'git', 'reset', '--hard', sha1,
                    ],
                )
    try:
        yield
    finally:
        log.info('Removing ragweed...')
        for client in config:
            ragweed_dir = get_ragweed_dir(testdir, client)
            ctx.cluster.only(client).run(
                args=['rm', '-rf', ragweed_dir]
                )


def _config_user(ragweed_conf, section, user):
    """
    Configure users for this section by stashing away keys, ids, and
    email addresses.
    """
    ragweed_conf[section].setdefault('user_id', user)
    ragweed_conf[section].setdefault('email', '{user}+test@test.test'.format(user=user))
    ragweed_conf[section].setdefault('display_name', 'Mr. {user}'.format(user=user))
    ragweed_conf[section].setdefault('access_key', ''.join(random.choice(string.ascii_uppercase) for i in range(20)))
    ragweed_conf[section].setdefault('secret_key', base64.b64encode(os.urandom(40)).decode('ascii'))


@contextlib.contextmanager
def create_users(ctx, config, run_stages):
    """
    Create a main and an alternate s3 user.
    """
    assert isinstance(config, dict)

    for client, properties in config['config'].items():
        run_stages[client] = properties.get('stages', 'prepare,check').split(',')

    log.info('Creating rgw users...')
    testdir = teuthology.get_testdir(ctx)
    users = {'user regular': 'ragweed', 'user system': 'sysuser'}
    for client in config['clients']:
        if not 'prepare' in run_stages[client]:
            # should have been prepared in a previous run
            continue

        ragweed_conf = config['ragweed_conf'][client]
        ragweed_conf.setdefault('fixtures', {})
        ragweed_conf['rgw'].setdefault('bucket_prefix', 'test-' + client)
        for section, user in users.items():
            _config_user(ragweed_conf, section, '{user}.{client}'.format(user=user, client=client))
            log.debug('Creating user {user} on {host}'.format(user=ragweed_conf[section]['user_id'], host=client))
            if user == 'sysuser':
                sys_str = 'true'
            else:
                sys_str = 'false'
            ctx.cluster.only(client).run(
                args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'radosgw-admin',
                    '-n', client,
                    'user', 'create',
                    '--uid', ragweed_conf[section]['user_id'],
                    '--display-name', ragweed_conf[section]['display_name'],
                    '--access-key', ragweed_conf[section]['access_key'],
                    '--secret', ragweed_conf[section]['secret_key'],
                    '--email', ragweed_conf[section]['email'],
                    '--system', sys_str,
                ],
            )
    try:
        yield
    finally:
        for client in config['clients']:
            if not 'check' in run_stages[client]:
                # only remove user if went through the check stage
                continue
            for user in users.values():
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
def configure(ctx, config, run_stages):
    """
    Configure the local config files.
    """
    assert isinstance(config, dict)
    log.info('Configuring ragweed...')
    testdir = teuthology.get_testdir(ctx)
    for client, properties in config['clients'].items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        preparing = 'prepare' in run_stages[client]
        if not preparing:
            # should have been prepared in a previous run
            continue

        ragweed_conf = config['ragweed_conf'][client]
        if properties is not None and 'slow_backend' in properties:
            ragweed_conf['fixtures']['slow backend'] = properties['slow_backend']

        conf_fp = BytesIO()
        ragweed_conf.write(conf_fp)
        remote.write_file(
            path='{tdir}/archive/ragweed.{client}.conf'.format(tdir=testdir, client=client),
            data=conf_fp.getvalue(),
            )

    log.info('Configuring boto...')
    boto_src = os.path.join(os.path.dirname(__file__), 'boto.cfg.template')
    for client, properties in config['clients'].items():
        with open(boto_src, 'r') as f:
            (remote,) = ctx.cluster.only(client).remotes.keys()
            conf = f.read().format(
                idle_timeout=config.get('idle_timeout', 30)
                )
            remote.write_file('{tdir}/boto.cfg'.format(tdir=testdir), conf)

    try:
        yield

    finally:
        log.info('Cleaning up boto...')
        for client, properties in config['clients'].items():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(
                args=[
                    'rm', '-f',
                    '{tdir}/boto.cfg'.format(tdir=testdir),
                    ],
                )

def get_toxvenv_dir(ctx):
    return ctx.tox.venv_path

def toxvenv_sh(ctx, remote, args, **kwargs):
    activate = get_toxvenv_dir(ctx) + '/bin/activate'
    return remote.sh(['source', activate, run.Raw('&&')] + args, **kwargs)

@contextlib.contextmanager
def run_tests(ctx, config, run_stages):
    """
    Run the ragweed after everything is set up.

    :param ctx: Context passed to task
    :param config: specific configuration information
    """
    assert isinstance(config, dict)
    testdir = teuthology.get_testdir(ctx)
    attrs = ["not fails_on_rgw"]
    for client, client_config in config.items():
        ragweed_dir = get_ragweed_dir(testdir, client)
        stages = ','.join(run_stages[client])
        args = [
            'cd', ragweed_dir, run.Raw('&&'),
            'RAGWEED_CONF={tdir}/archive/ragweed.{client}.conf'.format(tdir=testdir, client=client),
            'RAGWEED_STAGES={stages}'.format(stages=stages),
            'BOTO_CONFIG={tdir}/boto.cfg'.format(tdir=testdir),
            'tox',
            '--sitepackages',
            '--',
            '-v',
            '-m', ' and '.join(attrs),
            ]
        if client_config is not None and 'extra_args' in client_config:
            args.extend(client_config['extra_args'])

        (remote,) = ctx.cluster.only(client).remotes.keys()
        toxvenv_sh(ctx, remote, args, label="ragweed tests against rgw")
    yield

@contextlib.contextmanager
def task(ctx, config):
    """
    Run the ragweed suite against rgw.

    To run all tests on all clients::

        tasks:
        - ceph:
        - rgw:
        - ragweed:

    To restrict testing to particular clients::

        tasks:
        - ceph:
        - rgw: [client.0]
        - ragweed: [client.0]

    To run against a server on client.1 and increase the boto timeout to 10m::

        tasks:
        - ceph:
        - rgw: [client.1]
        - ragweed:
            client.0:
              rgw_server: client.1
              idle_timeout: 600
              stages: prepare,check

    To pass extra arguments to nose (e.g. to run a certain test)::

        tasks:
        - ceph:
        - rgw: [client.0]
        - ragweed:
            client.0:
              extra_args: ['test_s3:test_object_acl_grand_public_read']
            client.1:
              extra_args: ['--exclude', 'test_100_continue']
    """
    assert hasattr(ctx, 'rgw'), 'ragweed must run after the rgw task'
    assert hasattr(ctx, 'tox'), 'ragweed must run after the tox task'
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task ragweed only supports a list or dictionary for configuration"
    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)
    clients = config.keys()

    overrides = ctx.config.get('overrides', {})
    # merge each client section, not the top level.
    for client in config.keys():
        if not config[client]:
            config[client] = {}
        teuthology.deep_merge(config[client], overrides.get('ragweed', {}))

    log.debug('ragweed config is %s', config)

    ragweed_conf = {}
    for client in clients:
        # use rgw_server endpoint if given, or default to same client
        target = config[client].get('rgw_server', client)

        endpoint = ctx.rgw.role_endpoints.get(target)
        assert endpoint, 'ragweed: no rgw endpoint for {}'.format(target)

        ragweed_conf[client] = ConfigObj(
            indent_type='',
            infile={
                'rgw':
                    {
                    'host'      : endpoint.dns_name,
                    'port'      : endpoint.port,
                    'is_secure' : endpoint.cert is not None,
                    },
                'fixtures' : {},
                'user system'  : {},
                'user regular'   : {},
                'rados':
                    {
                    'ceph_conf'  : '/etc/ceph/ceph.conf',
                    },
                }
            )

    run_stages = {}

    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: create_users(ctx=ctx, config=dict(
                clients=clients,
                ragweed_conf=ragweed_conf,
                config=config,
                ),
                run_stages=run_stages),
        lambda: configure(ctx=ctx, config=dict(
                clients=config,
                ragweed_conf=ragweed_conf,
                ),
                run_stages=run_stages),
        lambda: run_tests(ctx=ctx, config=config, run_stages=run_stages),
        ):
        pass
    yield
