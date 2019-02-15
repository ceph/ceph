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

import util.rgw as rgw_utils

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.config import config as teuth_config
from teuthology.orchestra import run
from teuthology.orchestra.connection import split_user

log = logging.getLogger(__name__)

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
    s3_branches = [ 'master', 'nautilus', 'mimic', 'luminous', 'kraken', 'jewel' ]
    for (client, cconf) in config.items():
        default_branch = ''
        branch = cconf.get('force-branch', None)
        if not branch:
            default_branch = cconf.get('default-branch', None)
            ceph_branch = ctx.config.get('branch')
            suite_branch = ctx.config.get('suite_branch', ceph_branch)
            ragweed_repo = ctx.config.get('ragweed_repo', teuth_config.ceph_git_base_url + 'ragweed.git')
            if suite_branch in s3_branches:
                branch = cconf.get('branch', 'ceph-' + suite_branch)
	    else:
                branch = cconf.get('branch', suite_branch)
        if not branch:
            raise ValueError(
                "Could not determine what branch to use for ragweed!")
        else:
            log.info("Using branch '%s' for ragweed", branch)
        sha1 = cconf.get('sha1')
        try:
            ctx.cluster.only(client).run(
                args=[
                    'git', 'clone',
                    '-b', branch,
                    ragweed_repo,
                    '{tdir}/ragweed'.format(tdir=testdir),
                    ],
                )
        except Exception as e:
            if not default_branch:
                raise e
            ctx.cluster.only(client).run(
                args=[
                    'git', 'clone',
                    '-b', default_branch,
                    ragweed_repo,
                    '{tdir}/ragweed'.format(tdir=testdir),
                    ],
                )

        if sha1 is not None:
            ctx.cluster.only(client).run(
                args=[
                    'cd', '{tdir}/ragweed'.format(tdir=testdir),
                    run.Raw('&&'),
                    'git', 'reset', '--hard', sha1,
                    ],
                )
    try:
        yield
    finally:
        log.info('Removing ragweed...')
        testdir = teuthology.get_testdir(ctx)
        for client in config:
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/ragweed'.format(tdir=testdir),
                    ],
                )


def _config_user(ragweed_conf, section, user):
    """
    Configure users for this section by stashing away keys, ids, and
    email addresses.
    """
    ragweed_conf[section].setdefault('user_id', user)
    ragweed_conf[section].setdefault('email', '{user}+test@test.test'.format(user=user))
    ragweed_conf[section].setdefault('display_name', 'Mr. {user}'.format(user=user))
    ragweed_conf[section].setdefault('access_key', ''.join(random.choice(string.uppercase) for i in xrange(20)))
    ragweed_conf[section].setdefault('secret_key', base64.b64encode(os.urandom(40)))


@contextlib.contextmanager
def create_users(ctx, config, run_stages):
    """
    Create a main and an alternate s3 user.
    """
    assert isinstance(config, dict)

    for client, properties in config['config'].iteritems():
        run_stages[client] = string.split(properties.get('stages', 'prepare,check'), ',')

    log.info('Creating rgw users...')
    testdir = teuthology.get_testdir(ctx)
    users = {'user regular': 'foo', 'user system': 'sysuser'}
    for client in config['clients']:
        if not 'prepare' in run_stages[client]:
            # should have been prepared in a previous run
            continue

        ragweed_conf = config['ragweed_conf'][client]
        ragweed_conf.setdefault('fixtures', {})
        ragweed_conf['rgw'].setdefault('bucket_prefix', 'test-' + client)
        for section, user in users.iteritems():
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
def configure(ctx, config, run_stages):
    """
    Configure the ragweed.  This includes the running of the
    bootstrap code and the updating of local conf files.
    """
    assert isinstance(config, dict)
    log.info('Configuring ragweed...')
    testdir = teuthology.get_testdir(ctx)
    for client, properties in config['clients'].iteritems():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        remote.run(
            args=[
                'cd',
                '{tdir}/ragweed'.format(tdir=testdir),
                run.Raw('&&'),
                './bootstrap',
                ],
            )

        preparing = 'prepare' in run_stages[client]
        if not preparing:
            # should have been prepared in a previous run
            continue

        ragweed_conf = config['ragweed_conf'][client]
        if properties is not None and 'rgw_server' in properties:
            host = None
            for target, roles in zip(ctx.config['targets'].iterkeys(), ctx.config['roles']):
                log.info('roles: ' + str(roles))
                log.info('target: ' + str(target))
                if properties['rgw_server'] in roles:
                    _, host = split_user(target)
            assert host is not None, "Invalid client specified as the rgw_server"
            ragweed_conf['rgw']['host'] = host
        else:
            ragweed_conf['rgw']['host'] = 'localhost'

        if properties is not None and 'slow_backend' in properties:
	    ragweed_conf['fixtures']['slow backend'] = properties['slow_backend']

        conf_fp = StringIO()
        ragweed_conf.write(conf_fp)
        teuthology.write_file(
            remote=remote,
            path='{tdir}/archive/ragweed.{client}.conf'.format(tdir=testdir, client=client),
            data=conf_fp.getvalue(),
            )

    log.info('Configuring boto...')
    boto_src = os.path.join(os.path.dirname(__file__), 'boto.cfg.template')
    for client, properties in config['clients'].iteritems():
        with file(boto_src, 'rb') as f:
            (remote,) = ctx.cluster.only(client).remotes.keys()
            conf = f.read().format(
                idle_timeout=config.get('idle_timeout', 30)
                )
            teuthology.write_file(
                remote=remote,
                path='{tdir}/boto.cfg'.format(tdir=testdir),
                data=conf,
                )

    try:
        yield

    finally:
        log.info('Cleaning up boto...')
        for client, properties in config['clients'].iteritems():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(
                args=[
                    'rm',
                    '{tdir}/boto.cfg'.format(tdir=testdir),
                    ],
                )

@contextlib.contextmanager
def run_tests(ctx, config, run_stages):
    """
    Run the ragweed after everything is set up.

    :param ctx: Context passed to task
    :param config: specific configuration information
    """
    assert isinstance(config, dict)
    testdir = teuthology.get_testdir(ctx)
    attrs = ["!fails_on_rgw"]
    for client, client_config in config.iteritems():
        stages = string.join(run_stages[client], ',')
        args = [
            'RAGWEED_CONF={tdir}/archive/ragweed.{client}.conf'.format(tdir=testdir, client=client),
            'RAGWEED_STAGES={stages}'.format(stages=stages),
            'BOTO_CONFIG={tdir}/boto.cfg'.format(tdir=testdir),
            '{tdir}/ragweed/virtualenv/bin/nosetests'.format(tdir=testdir),
            '-w',
            '{tdir}/ragweed'.format(tdir=testdir),
            '-v',
            '-a', ','.join(attrs),
            ]
        if client_config is not None and 'extra_args' in client_config:
            args.extend(client_config['extra_args'])

        ctx.cluster.only(client).run(
            args=args,
            label="ragweed tests against rgw"
            )
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
    for client in config.iterkeys():
        if not config[client]:
            config[client] = {}
        teuthology.deep_merge(config[client], overrides.get('ragweed', {}))

    log.debug('ragweed config is %s', config)

    ragweed_conf = {}
    for client in clients:
        endpoint = ctx.rgw.role_endpoints.get(client)
        assert endpoint, 'ragweed: no rgw endpoint for {}'.format(client)

        ragweed_conf[client] = ConfigObj(
            indent_type='',
            infile={
                'rgw':
                    {
                    'port'      : endpoint.port,
                    'is_secure' : 'yes' if endpoint.cert else 'no',
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
