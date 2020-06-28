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
from teuthology.exceptions import ConfigError

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
    log.info('Downloading s3-tests...')
    testdir = teuthology.get_testdir(ctx)
    for (client, client_config) in config.items():
        s3tests_branch = client_config.get('force-branch', None)
        if not s3tests_branch:
            raise ValueError(
                "Could not determine what branch to use for s3-tests. Please add 'force-branch: {s3-tests branch name}' to the .yaml config for this s3tests task.")

        log.info("Using branch '%s' for s3tests", s3tests_branch)
        sha1 = client_config.get('sha1')
        git_remote = client_config.get('git_remote', teuth_config.ceph_git_base_url)
        ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', s3tests_branch,
                git_remote + 's3-tests.git',
                '{tdir}/s3-tests'.format(tdir=testdir),
                ],
            )
        if sha1 is not None:
            ctx.cluster.only(client).run(
                args=[
                    'cd', '{tdir}/s3-tests'.format(tdir=testdir),
                    run.Raw('&&'),
                    'git', 'reset', '--hard', sha1,
                    ],
                )
    try:
        yield
    finally:
        log.info('Removing s3-tests...')
        testdir = teuthology.get_testdir(ctx)
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
    s3tests_conf[section].setdefault('access_key',
        ''.join(random.choice(string.ascii_uppercase) for i in range(20)))
    s3tests_conf[section].setdefault('secret_key',
        base64.b64encode(os.urandom(40)).decode())
    s3tests_conf[section].setdefault('totp_serial',
        ''.join(random.choice(string.digits) for i in range(10)))
    s3tests_conf[section].setdefault('totp_seed',
        base64.b32encode(os.urandom(40)).decode())
    s3tests_conf[section].setdefault('totp_seconds', '5')


@contextlib.contextmanager
def create_users(ctx, config):
    """
    Create a main and an alternate s3 user.
    """
    assert isinstance(config, dict)
    log.info('Creating rgw users...')
    testdir = teuthology.get_testdir(ctx)
    users = {'s3 main': 'foo', 's3 alt': 'bar', 's3 tenant': 'testx$tenanteduser'}
    for client in config['clients']:
        s3tests_conf = config['s3tests_conf'][client]
        s3tests_conf.setdefault('fixtures', {})
        s3tests_conf['fixtures'].setdefault('bucket prefix', 'test-' + client + '-{random}-')
        for section, user in users.items():
            _config_user(s3tests_conf, section, '{user}.{client}'.format(user=user, client=client))
            log.debug('Creating user {user} on {host}'.format(user=s3tests_conf[section]['user_id'], host=client))
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
                    '--caps', 'user-policy=*',
                    '--cluster', cluster_name,
                ],
            )
            ctx.cluster.only(client).run(
                args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'radosgw-admin',
                    '-n', client_with_id,
                    'mfa', 'create',
                    '--uid', s3tests_conf[section]['user_id'],
                    '--totp-serial', s3tests_conf[section]['totp_serial'],
                    '--totp-seed', s3tests_conf[section]['totp_seed'],
                    '--totp-seconds', s3tests_conf[section]['totp_seconds'],
                    '--totp-window', '8',
                    '--totp-seed-type', 'base32',
                    '--cluster', cluster_name,
                ],
            )
    try:
        yield
    finally:
        for client in config['clients']:
            for user in users.values():
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
    log.info('Configuring s3-tests...')
    testdir = teuthology.get_testdir(ctx)
    for client, properties in config['clients'].items():
        properties = properties or {}
        s3tests_conf = config['s3tests_conf'][client]
        s3tests_conf['DEFAULT']['calling_format'] = properties.get('calling-format', 'ordinary')

        # use rgw_server if given, or default to local client
        role = properties.get('rgw_server', client)

        endpoint = ctx.rgw.role_endpoints.get(role)
        assert endpoint, 's3tests: no rgw endpoint for {}'.format(role)

        s3tests_conf['DEFAULT']['host'] = endpoint.dns_name

        website_role = properties.get('rgw_website_server')
        if website_role:
            website_endpoint = ctx.rgw.role_endpoints.get(website_role)
            assert website_endpoint, \
                    's3tests: no rgw endpoint for rgw_website_server {}'.format(website_role)
            assert website_endpoint.website_dns_name, \
                    's3tests: no dns-s3website-name for rgw_website_server {}'.format(website_role)
            s3tests_conf['DEFAULT']['s3website_domain'] = website_endpoint.website_dns_name

        if hasattr(ctx, 'barbican'):
            properties = properties['barbican']
            if properties is not None and 'kms_key' in properties:
                if not (properties['kms_key'] in ctx.barbican.keys):
                    raise ConfigError('Key '+properties['kms_key']+' not defined')

                if not (properties['kms_key2'] in ctx.barbican.keys):
                    raise ConfigError('Key '+properties['kms_key2']+' not defined')

                key = ctx.barbican.keys[properties['kms_key']]
                s3tests_conf['DEFAULT']['kms_keyid'] = key['id']

                key = ctx.barbican.keys[properties['kms_key2']]
                s3tests_conf['DEFAULT']['kms_keyid2'] = key['id']

        elif hasattr(ctx, 'vault'):
            properties = properties['vault_%s' % ctx.vault.engine]
            s3tests_conf['DEFAULT']['kms_keyid'] = properties['key_path']
            s3tests_conf['DEFAULT']['kms_keyid2'] = properties['key_path2']

        else:
            # Fallback scenario where it's the local (ceph.conf) kms being tested
            s3tests_conf['DEFAULT']['kms_keyid'] = 'testkey-1'
            s3tests_conf['DEFAULT']['kms_keyid2'] = 'testkey-2'

        slow_backend = properties.get('slow_backend')
        if slow_backend:
            s3tests_conf['fixtures']['slow backend'] = slow_backend

        (remote,) = ctx.cluster.only(client).remotes.keys()
        remote.run(
            args=[
                'cd',
                '{tdir}/s3-tests'.format(tdir=testdir),
                run.Raw('&&'),
                './bootstrap',
                ],
            )
        conf_fp = BytesIO()
        s3tests_conf.write(conf_fp)
        teuthology.write_file(
            remote=remote,
            path='{tdir}/archive/s3-tests.{client}.conf'.format(tdir=testdir, client=client),
            data=conf_fp.getvalue(),
            )

    log.info('Configuring boto...')
    boto_src = os.path.join(os.path.dirname(__file__), 'boto.cfg.template')
    for client, properties in config['clients'].items():
        with open(boto_src) as f:
            (remote,) = ctx.cluster.only(client).remotes.keys()
            conf = f.read().format(
                idle_timeout=config.get('idle_timeout', 30)
                )
            teuthology.write_file(
                remote=remote,
                path='{tdir}/boto.cfg'.format(tdir=testdir),
                data=conf.encode(),
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
    Run the s3tests after everything is set up.

    :param ctx: Context passed to task
    :param config: specific configuration information
    """
    assert isinstance(config, dict)
    testdir = teuthology.get_testdir(ctx)
    for client, client_config in config.items():
        client_config = client_config or {}
        (remote,) = ctx.cluster.only(client).remotes.keys()
        args = [
            'S3TEST_CONF={tdir}/archive/s3-tests.{client}.conf'.format(tdir=testdir, client=client),
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
            '{tdir}/s3-tests/virtualenv/bin/python'.format(tdir=testdir),
            '-m', 'nose',
            '-w',
            '{tdir}/s3-tests'.format(tdir=testdir),
            '-v',
            '-a', ','.join(attrs),
            ]
        if 'extra_args' in client_config:
            args.append(client_config['extra_args'])

        remote.run(
            args=args,
            label="s3 tests against rgw"
            )
    yield

@contextlib.contextmanager
def scan_for_leaked_encryption_keys(ctx, config):
    """
    Scan radosgw logs for the encryption keys used by s3tests to
    verify that we're not leaking secrets.

    :param ctx: Context passed to task
    :param config: specific configuration information
    """
    assert isinstance(config, dict)

    try:
        yield
    finally:
        # x-amz-server-side-encryption-customer-key
        s3test_customer_key = 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs='

        log.debug('Scanning radosgw logs for leaked encryption keys...')
        procs = list()
        for client, client_config in config.items():
            if not client_config.get('scan_for_encryption_keys', True):
                continue
            cluster_name, daemon_type, client_id = teuthology.split_role(client)
            client_with_cluster = '.'.join((cluster_name, daemon_type, client_id))
            (remote,) = ctx.cluster.only(client).remotes.keys()
            proc = remote.run(
                args=[
                    'grep',
                    '--binary-files=text',
                    s3test_customer_key,
                    '/var/log/ceph/rgw.{client}.log'.format(client=client_with_cluster),
                ],
                wait=False,
                check_status=False,
            )
            procs.append(proc)

        for proc in procs:
            proc.wait()
            if proc.returncode == 1: # 1 means no matches
                continue
            log.error('radosgw log is leaking encryption keys!')
            raise Exception('radosgw log is leaking encryption keys')

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
    for client in config.keys():
        if not config[client]:
            config[client] = {}
        teuthology.deep_merge(config[client], overrides.get('s3tests', {}))

    log.debug('s3tests config is %s', config)

    s3tests_conf = {}
    for client in clients:
        endpoint = ctx.rgw.role_endpoints.get(client)
        assert endpoint, 's3tests: no rgw endpoint for {}'.format(client)

        s3tests_conf[client] = ConfigObj(
            indent_type='',
            infile={
                'DEFAULT':
                    {
                    'port'      : endpoint.port,
                    'is_secure' : endpoint.cert is not None,
                    'api_name'  : 'default',
                    },
                'fixtures' : {},
                's3 main'  : {},
                's3 alt'   : {},
		's3 tenant': {},
                }
            )

    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: create_users(ctx=ctx, config=dict(
                clients=clients,
                s3tests_conf=s3tests_conf,
                )),
        lambda: configure(ctx=ctx, config=dict(
                clients=config,
                s3tests_conf=s3tests_conf,
                )),
        lambda: run_tests(ctx=ctx, config=config),
        lambda: scan_for_leaked_encryption_keys(ctx=ctx, config=config),
        ):
        pass
    yield
