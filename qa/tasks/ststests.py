"""
Run a set of sts tests on rgw.
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
from teuthology.exceptions import ConfigError

log = logging.getLogger(__name__)

@contextlib.contextmanager
def download(ctx, config):
    """
    Download the sts tests from the git builder.
    Remove downloaded sts file upon exit.
    The context passed in should be identical to the context
    passed in to the main task.
    """
    assert isinstance(config, dict)
    log.info('Downloading sts-tests...')
    testdir = teuthology.get_testdir(ctx)
    for (client, client_config) in config.items():
        ststests_branch = client_config.get('force-branch', None)
        if not ststests_branch:
            raise ValueError(
                "Could not determine what branch to use for sts-tests. Please add 'force-branch: {sts-tests branch name}' to the .yaml config for this ststests task.")

        log.info("Using branch '%s' for ststests", ststests_branch)
        sha1 = client_config.get('sha1')
        git_remote = client_config.get('git_remote', teuth_config.ceph_git_base_url)
        ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', ststests_branch,
                git_remote + 'sts-tests.git',
                '{tdir}/sts-tests'.format(tdir=testdir),
                ],
            )
        if sha1 is not None:
            ctx.cluster.only(client).run(
                args=[
                    'cd', '{tdir}/sts-tests'.format(tdir=testdir),
                    run.Raw('&&'),
                    'git', 'reset', '--hard', sha1,
                    ],
                )
    try:
        yield
    finally:
        log.info('Removing sts-tests...')
        testdir = teuthology.get_testdir(ctx)
        for client in config:
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/sts-tests'.format(tdir=testdir),
                    ],
                )


def _config_user(ststests_conf, section, user):
    """
    Configure users for this section by stashing away keys, ids, and
    email addresses.
    """
    ststests_conf[section].setdefault('user_id', user)
    ststests_conf[section].setdefault('access_key', ''.join(random.choice(string.ascii_uppercase) for i in range(20)))
    ststests_conf[section].setdefault('secret_key', base64.b64encode(os.urandom(40)))

@contextlib.contextmanager
def create_users(ctx, config):
    """
    Create a main and an alternate sts user.
    """
    assert isinstance(config, dict)
    log.info('Creating rgw users...')
    testdir = teuthology.get_testdir(ctx)
    users = {'sts': 'foo', 'iam': 'bar', 's3 main': 'foobar'}
    for client in config['clients']:
        ststests_conf = config['ststests_conf'][client]
        ststests_conf.setdefault('fixtures', {})
        ststests_conf['fixtures'].setdefault('bucket prefix', 'test-' + client + '-{random}-')
        for section, user in users.items():
            _config_user(ststests_conf, section, '{user}.{client}'.format(user=user, client=client))
            log.debug('Creating user {user} on {host}'.format(user=ststests_conf[section]['user_id'], host=client))
            cluster_name, daemon_type, client_id = teuthology.split_role(client)
            client_with_id = daemon_type + '.' + client_id
            if section=='iam':
                ctx.cluster.only(client).run(
                    args=[
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'radosgw-admin',
                        '-n', client_with_id,
                        'user', 'create',
                        '--uid', ststests_conf[section]['user_id'],
                        '--access-key', ststests_conf[section]['access_key'],
                        '--secret', ststests_conf[section]['secret_key'],
                        '--caps', 'user-policy=*',
                        '--caps', 'roles=*',
                        '--cluster', cluster_name,
                    ],
                )
            else:
                ctx.cluster.only(client).run(
                    args=[
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'radosgw-admin',
                        '-n', client_with_id,
                        'user', 'create',
                        '--uid', ststests_conf[section]['user_id'],
                        '--access-key', ststests_conf[section]['access_key'],
                        '--secret', ststests_conf[section]['secret_key'],
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
    Configure the sts-tests.  This includes the running of the
    bootstrap code and the updating of local conf files.
    """
    assert isinstance(config, dict)
    log.info('Configuring sts-tests...')
    testdir = teuthology.get_testdir(ctx)
    for client, properties in config['clients'].items():
        properties = properties or {}
        ststests_conf = config['ststests_conf'][client]
        ststests_conf['DEFAULT']['calling_format'] = properties.get('calling-format', 'ordinary')

        # use rgw_server if given, or default to local client
        role = properties.get('rgw_server', client)

        endpoint = ctx.rgw.role_endpoints.get(role)
        assert endpoint, 'ststests: no rgw endpoint for {}'.format(role)

        ststests_conf['DEFAULT']['host'] = endpoint.dns_name

        slow_backend = properties.get('slow_backend')
        if slow_backend:
            ststests_conf['fixtures']['slow backend'] = slow_backend

        (remote,) = ctx.cluster.only(client).remotes.keys()
        remote.run(
            args=[
                'cd',
                '{tdir}/sts-tests'.format(tdir=testdir),
                run.Raw('&&'),
                './bootstrap',
                ],
            )
        conf_fp = StringIO()
        ststests_conf.write(conf_fp)
        teuthology.write_file(
            remote=remote,
            path='{tdir}/archive/sts-tests.{client}.conf'.format(tdir=testdir, client=client),
            data=conf_fp.getvalue(),
            )

    log.info('Configuring boto...')
    boto_src = os.path.join(os.path.dirname(__file__), 'boto.cfg.template')
    for client, properties in config['clients'].items():
        with open(boto_src, 'rb') as f:
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
        for client, properties in config['clients'].items():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(
                args=[
                    'rm',
                    '{tdir}/boto.cfg'.format(tdir=testdir),
                    ],
                )

@contextlib.contextmanager
def run_tests(ctx, config):
    """
    Run the ststests after everything is set up.
    :param ctx: Context passed to task
    :param config: specific configuration information
    """
    assert isinstance(config, dict)
    testdir = teuthology.get_testdir(ctx)
    for client, client_config in config.items():
        client_config = client_config or {}
        (remote,) = ctx.cluster.only(client).remotes.keys()
        args = [
            'STSTEST_CONF={tdir}/archive/sts-tests.{client}.conf'.format(tdir=testdir, client=client),
            'BOTO_CONFIG={tdir}/boto.cfg'.format(tdir=testdir)
            ]
        # the 'requests' library comes with its own ca bundle to verify ssl
        # certificates - override that to use the system's ca bundle, which
        # is where the ssl task installed this certificate
        if remote.os.package_type == 'deb':
            args += ['REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt']
        else:
            args += ['REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt']
        # civetweb > 1.8 && beast parsers are strict on rfc2616
        attrs = ["!fails_on_rgw", "!lifecycle_expiration", "!fails_strict_rfc2616"]
        if client_config.get('calling-format') != 'ordinary':
            attrs += ['!fails_with_subdomain']
        args += [
            '{tdir}/sts-tests/virtualenv/bin/python'.format(tdir=testdir),
            '-m', 'nose',
            '-w',
            '{tdir}/sts-tests'.format(tdir=testdir),
            '-v',
            '-a', ','.join(attrs),
            ]
        if 'extra_args' in client_config:
            args.append(client_config['extra_args'])

        remote.run(
            args=args,
            label="sts tests against rgw"
            )
    yield

@contextlib.contextmanager
def task(ctx, config):
    assert hasattr(ctx, 'rgw'), 'ststests must run after the rgw task'
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task ststests only supports a list or dictionary for configuration"
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
        teuthology.deep_merge(config[client], overrides.get('ststests', {}))

    log.debug('ststests config is %s', config)

    ststests_conf = {}
    for client in clients:
        endpoint = ctx.rgw.role_endpoints.get(client)
        assert endpoint, 'ststests: no rgw endpoint for {}'.format(client)

        ststests_conf[client] = ConfigObj(
            indent_type='',
            infile={
                'DEFAULT':
                    {
                    'port'      : endpoint.port,
                    'is_secure' : endpoint.cert is not None,
                    'api_name'  : 'default',
                    },
                'fixtures' : {},
                'sts'  : {},
                'iam'   : {},
                's3 main': {},
                }
            )

    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: create_users(ctx=ctx, config=dict(
                clients=clients,
                ststests_conf=ststests_conf,
                )),
        lambda: configure(ctx=ctx, config=dict(
                clients=config,
                ststests_conf=ststests_conf,
                )),
        lambda: run_tests(ctx=ctx, config=config),
        ):
        pass
    yield
