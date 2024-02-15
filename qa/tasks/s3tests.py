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
import datetime
import uuid

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
                '{tdir}/s3-tests-{client}'.format(tdir=testdir, client=client),
                ],
            )
        if sha1 is not None:
            ctx.cluster.only(client).run(
                args=[
                    'cd', '{tdir}/s3-tests-{client}'.format(tdir=testdir, client=client),
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
                    '{tdir}/s3-tests-{client}'.format(tdir=testdir, client=client),
                    ],
                )


def _config_user(s3tests_conf, section, user, email):
    """
    Configure users for this section by stashing away keys, ids, and
    email addresses.
    """
    s3tests_conf[section].setdefault('user_id', user)
    s3tests_conf[section].setdefault('email', email)
    s3tests_conf[section].setdefault('display_name', 'Mr.{user}'.format(user=user))
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
def create_users(ctx, config, s3tests_conf):
    """
    Create a main and an alternate s3 user.
    """
    log.info('Creating rgw users...')
    testdir = teuthology.get_testdir(ctx)
    
    users = {'s3 main': 'foo', 's3 alt': 'bar', 's3 tenant': 'testx$tenanteduser', 'iam': 'foobar', 'iam root': 'root1', 'iam alt root': 'root2'}
    for client, cconfig in config.items():
        cluster_name, daemon_type, client_id = teuthology.split_role(client)
        client_with_id = daemon_type + '.' + client_id
        conf = s3tests_conf[client]
        conf.setdefault('fixtures', {})
        conf['fixtures'].setdefault('bucket prefix', 'test-{random}-')

        accounts = cconfig.get('accounts', {})
        keystone_users = cconfig.get('keystone users', {})
        for section, user in users.items():
            user_id = '{user}.{client}'.format(user=user, client=client)
            user_email = '{user}+test@test.test'.format(user=user)

            account_id = accounts.get(section)
            if account_id:
                # create account
                account_email = '{account_id}+test@test.test'.format(account_id=account_id)
                args=[
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'radosgw-admin',
                        '-n', client_with_id,
                        '--cluster', cluster_name,
                        'account', 'create',
                        '--account-id', account_id,
                        '--account-name', 'Mr.{user}'.format(user=account_id),
                        '--email', account_email,
                    ]
                if section == 's3 tenant':
                    args += ['--tenant', 'testx']
                ctx.cluster.only(client).run(args=args)
                _config_user(conf, section, account_id, account_email)
            else:
                _config_user(conf, section, user_id, user_email)

            # for keystone users, read ec2 credentials into s3tests.conf instead
            # of creating a local user
            keystone_user = keystone_users.get(section)
            if keystone_user:
                project_name = keystone_user.pop('project')
                creds = ctx.keystone.read_ec2_credentials(ctx, **keystone_user)
                access = creds['Access']
                secret = creds['Secret']
                project_id = creds['Project ID']

                conf[section]['access_key'] = access
                conf[section]['secret_key'] = secret
                conf[section]['user_id'] = project_id
                conf[section]['display_name'] = project_name

                log.debug('Using keystone user {kuser} credentials ({access} : {secret}) for {pname}:{pid} on {host}'.format(
                    kuser=keystone_user['user'], access=access, secret=secret,
                    pname=project_name, pid=project_id, host=client))
                continue

            log.debug('Creating user {user} on {host}'.format(user=conf[section]['user_id'], host=client))
            # create user
            user_args=[
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'radosgw-admin',
                    '-n', client_with_id,
                    'user', 'create',
                    '--uid', user_id,
                    '--display-name', conf[section]['display_name'],
                    '--email', user_email,
                    '--access-key', conf[section]['access_key'],
                    '--secret', conf[section]['secret_key'],
                    '--cluster', cluster_name,
                ]
            if account_id:
                user_args += ['--account-id', account_id, '--account-root']
            ctx.cluster.only(client).run(args=user_args)

            if not ctx.dbstore_variable:
                ctx.cluster.only(client).run(
                    args=[
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'radosgw-admin',
                        '-n', client_with_id,
                        'mfa', 'create',
                        '--uid', user_id,
                        '--totp-serial', conf[section]['totp_serial'],
                        '--totp-seed', conf[section]['totp_seed'],
                        '--totp-seconds', conf[section]['totp_seconds'],
                        '--totp-window', '8',
                        '--totp-seed-type', 'base32',
                        '--cluster', cluster_name,
                    ],
                )

            # add/configure caps for iam user
            if section=='iam':
                ctx.cluster.only(client).run(
                    args=[
                        'adjust-ulimits',
                        'ceph-coverage',
                        '{tdir}/archive/coverage'.format(tdir=testdir),
                        'radosgw-admin',
                        '-n', client_with_id,
                        'caps', 'add',
                        '--uid', user_id,
                        '--caps', 'oidc-provider=*;roles=*;user-policy=*',
                        '--cluster', cluster_name,
                    ],
                )

    if "TOKEN" in os.environ:
        conf.setdefault('webidentity', {})
        conf['webidentity'].setdefault('token',os.environ['TOKEN'])
        conf['webidentity'].setdefault('aud',os.environ['AUD'])
        conf['webidentity'].setdefault('sub',os.environ['SUB'])
        conf['webidentity'].setdefault('azp',os.environ['AZP'])
        conf['webidentity'].setdefault('user_token',os.environ['USER_TOKEN'])
        conf['webidentity'].setdefault('thumbprint',os.environ['THUMBPRINT'])
        conf['webidentity'].setdefault('KC_REALM',os.environ['KC_REALM'])

    try:
        yield
    finally:
        for client in config.keys():
            for section, user in users.items():
                # don't need to delete keystone users
                if not user in keystone_users:
                    continue
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
                account_id = accounts.get(section)
                if account_id:
                    ctx.cluster.only(client).run(
                        args=[
                            'adjust-ulimits',
                            'ceph-coverage',
                            '{tdir}/archive/coverage'.format(tdir=testdir),
                            'radosgw-admin',
                            '-n', client_with_id,
                            '--cluster', cluster_name,
                            'account', 'rm',
                            '--account-id', account_id,
                            ])


@contextlib.contextmanager
def configure(ctx, config):
    """
    Create the config files for s3tests an boto.
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
            engine_or_flavor = vars(ctx.vault).get('flavor',ctx.vault.engine)
            keys=[]
            for name in (x['Path'] for x in vars(ctx.vault).get('keys', {}).get(ctx.rgw.vault_role)):
                keys.append(name)

            keys.extend(['testkey-1','testkey-2'])
            if engine_or_flavor == "old":
                keys=[keys[i] + "/1" for i in range(len(keys))]

            properties = properties.get('vault_%s' % engine_or_flavor, {})
            s3tests_conf['DEFAULT']['kms_keyid'] = properties.get('key_path', keys[0])
            s3tests_conf['DEFAULT']['kms_keyid2'] = properties.get('key_path2', keys[1])
        elif hasattr(ctx.rgw, 'pykmip_role'):
            keys=[]
            for name in (x['Name'] for x in ctx.pykmip.keys[ctx.rgw.pykmip_role]):
                p=name.partition('-')
                keys.append(p[2] if p[2] else p[0])
            keys.extend(['testkey-1', 'testkey-2'])
            s3tests_conf['DEFAULT']['kms_keyid'] = properties.get('kms_key', keys[0])
            s3tests_conf['DEFAULT']['kms_keyid2'] = properties.get('kms_key2', keys[1])
        else:
            # Fallback scenario where it's the local (ceph.conf) kms being tested
            s3tests_conf['DEFAULT']['kms_keyid'] = 'testkey-1'
            s3tests_conf['DEFAULT']['kms_keyid2'] = 'testkey-2'

        slow_backend = properties.get('slow_backend')
        if slow_backend:
            s3tests_conf['fixtures']['slow backend'] = slow_backend

        storage_classes = properties.get('storage classes')
        if storage_classes:
            s3tests_conf['s3 main']['storage_classes'] = storage_classes

        lc_debug_interval = properties.get('lc_debug_interval')
        if lc_debug_interval:
            s3tests_conf['s3 main']['lc_debug_interval'] = lc_debug_interval

        if ctx.rgw_cloudtier is not None:
            log.info(' ctx.rgw_cloudtier config  is %s ...', ctx.rgw_cloudtier.config)
            client_rgw_config = ctx.rgw_cloudtier.config.get(client)
            if client_rgw_config:
                log.info(' ctx.rgw_cloudtier config  is %s ...', client_rgw_config)
                cloudtier_user = client_rgw_config.get('cloudtier_user')
                cloud_client = client_rgw_config.get('cloud_client')
                endpoint = ctx.rgw.role_endpoints.get(cloud_client)
                s3tests_conf['s3 cloud']['host'] = endpoint.dns_name
                s3tests_conf['s3 cloud']['port'] = endpoint.port
                s3tests_conf['s3 cloud']['access_key'] = cloudtier_user.get('cloud_access_key')
                s3tests_conf['s3 cloud']['secret_key'] = cloudtier_user.get('cloud_secret')
                s3tests_conf['s3 cloud']['cloud_storage_class'] = client_rgw_config.get('cloud_storage_class')
                s3tests_conf['s3 cloud']['storage_class'] = client_rgw_config.get('cloud_regular_storage_class')
                s3tests_conf['s3 cloud']['retain_head_object'] = client_rgw_config.get('cloud_retain_head_object')
                cloud_target_path = client_rgw_config.get('cloud_target_path')
                cloud_target_storage_class = client_rgw_config.get('cloud_target_storage_class')
                if (cloud_target_path != None):
                    s3tests_conf['s3 cloud']['target_path'] = cloud_target_path
                if (cloud_target_storage_class != None):
                    s3tests_conf['s3 cloud']['target_storage_class'] = cloud_target_storage_class

        (remote,) = ctx.cluster.only(client).remotes.keys()
        conf_fp = BytesIO()
        s3tests_conf.write(conf_fp)
        remote.write_file(
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
            remote.write_file('{tdir}/boto-{client}.cfg'.format(tdir=testdir, client=client), conf)

    try:
        yield

    finally:
        log.info('Cleaning up boto...')
        for client, properties in config['clients'].items():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            remote.run(
                args=[
                    'rm',
                    '{tdir}/boto-{client}.cfg'.format(tdir=testdir, client=client),
                    ],
                )

def get_toxvenv_dir(ctx):
    return ctx.tox.venv_path

def toxvenv_sh(ctx, remote, args, **kwargs):
    activate = get_toxvenv_dir(ctx) + '/bin/activate'
    return remote.sh(['source', activate, run.Raw('&&')] + args, **kwargs)

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
            'cd', '{tdir}/s3-tests-{client}'.format(tdir=testdir, client=client), run.Raw('&&'),
            'S3TEST_CONF={tdir}/archive/s3-tests.{client}.conf'.format(tdir=testdir, client=client),
            'BOTO_CONFIG={tdir}/boto-{client}.cfg'.format(tdir=testdir, client=client)
            ]
        # the 'requests' library comes with its own ca bundle to verify ssl
        # certificates - override that to use the system's ca bundle, which
        # is where the ssl task installed this certificate
        if remote.os.package_type == 'deb':
            args += ['REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt']
        else:
            args += ['REQUESTS_CA_BUNDLE=/etc/pki/tls/certs/ca-bundle.crt']
        attrs = ["not fails_on_rgw"]
        if not client_config.get('lifecycle_tests'):
            attrs += ['not lifecycle_expiration', 'not lifecycle_transition']
        if not client_config.get('cloudtier_tests'):
            attrs += ['not cloud_transition']
        if not client_config.get('sts_tests', False):
            attrs += ["not test_of_sts"]
        if not client_config.get('webidentity_tests', False):
            attrs += ["not webidentity_test"]
        if client_config.get('calling-format') != 'ordinary':
            attrs += ['not fails_with_subdomain']
        if not client_config.get('with-sse-s3'):
            attrs += ['not sse_s3']
       
        attrs += client_config.get('extra_attrs', [])
        if 'unit_test_scan' in client_config and client_config['unit_test_scan']:
            xmlfile_id = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S--") + str(uuid.uuid4())
            xmlpath= f'{testdir}/archive/s3test-{xmlfile_id}.xml'
            args += ['tox', '--', '-v', f'--junitxml={xmlpath}', '-m', ' and '.join(attrs)]
            args += client_config.get('extra_args', [])
            activate = get_toxvenv_dir(ctx) + '/bin/activate'
            remote.run_unit_test(
                args=['source', activate, run.Raw('&&')] + args,
                label="s3 tests against rgw",
                xml_path_regex=f'{testdir}/archive/s3test-*.xml',
                output_yaml=os.path.join(ctx.archive, 'unit_test_summary.yaml'),
            )
        else:
            args += ['tox', '--', '-v', '-m', ' and '.join(attrs)]
            args += client_config.get('extra_args', [])
            toxvenv_sh(ctx, remote, args, label="s3 tests against rgw")
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

    To pass extra arguments to pytest (e.g. to run a certain test)::

        tasks:
        - ceph:
        - rgw: [client.0]
        - s3tests:
            client.0:
              extra_args: ['test_s3:test_object_acl_grand_public_read']
            client.1:
              extra_args: ['--exclude', 'test_100_continue']

    To run any sts-tests don't forget to set a config variable named 'sts_tests' to 'True' as follows::

        tasks:
        - ceph:
        - rgw: [client.0]
        - s3tests:
            client.0:
              sts_tests: True
              rgw_server: client.0

    To run any cloud-transition tests don't forget to set a config variable named 'cloudtier_tests' to 'True' as follows::

        tasks:
        - ceph:
        - rgw: [client.0 client.1]
        - s3tests:
            client.0:
              cloudtier_tests: True
              rgw_server: client.0

    To test against Keystone users with EC2 credentials::

        tasks:
        - ceph:
        - rgw: [client.0 client.1]
        - keystone:
          client.0:
            projects:
              - name: myproject
                description: my project
            users:
              - name: myuser
                password: SECRET
                project: myproject
            ec2 credentials:
              - project: myproject
                user: myuser
        - s3tests:
            client.0:
              keystone users:
                s3 main:
                  client: client.0
                  project: myproject
                  user: myuser

    """
    assert hasattr(ctx, 'rgw'), 's3tests must run after the rgw task'
    assert hasattr(ctx, 'tox'), 's3tests must run after the tox task'
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

    for client, client_config in config.items():
        if 'sts_tests' in client_config:
            ctx.sts_variable = True
        else:
            ctx.sts_variable = False

        if 'cloudtier_tests' in client_config:
            ctx.cloudtier_variable = True
        else:
            ctx.cloudtier_variable = False

        if 'dbstore_tests' in client_config:
            ctx.dbstore_variable = True
        else:
            ctx.dbstore_variable = False

        #This will be the structure of config file when you want to run webidentity_test (sts-test)
        if ctx.sts_variable and "TOKEN" in os.environ:
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
                        'fixtures'   : {},
                        's3 main'    : {},
                        's3 alt'     : {},
                        's3 tenant'  : {},
                        'iam'        : {},
                        'iam root'   : {},
                        'iam alt root' : {},
                        'webidentity': {},
                    }
                )

        elif ctx.sts_variable:
            #This will be the structure of config file when you want to run assume_role_test and get_session_token_test (sts-test) or iam-tests
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
                        'fixtures'   : {},
                        's3 main'    : {},
                        's3 alt'     : {},
                        'iam'        : {},
                        'iam root'   : {},
                        'iam alt root' : {},
                        's3 tenant'  : {},
                        }
                    ) 

        elif ctx.cloudtier_variable:
            #This will be the structure of config file when you want to run normal s3-tests
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
                        'fixtures'   : {},
                        's3 main'    : {},
                        's3 alt'     : {},
                        's3 tenant'  : {},
                        's3 cloud'   : {},
                        'iam'        : {},
                        'iam root'   : {},
                        'iam alt root' : {},
                        }
                    ) 
        else:
            #This will be the structure of config file when you want to run normal s3-tests
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
                        'fixtures'   : {},
                        's3 main'    : {},
                        's3 alt'     : {},
                        's3 tenant'  : {},
                        'iam'        : {},
                        'iam root'   : {},
                        'iam alt root' : {},
                        }
                    )

    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: create_users(ctx=ctx, config=config, s3tests_conf=s3tests_conf),
        lambda: configure(ctx=ctx, config=dict(
                clients=config,
                s3tests_conf=s3tests_conf,
                )),
        lambda: run_tests(ctx=ctx, config=config),
        lambda: scan_for_leaked_encryption_keys(ctx=ctx, config=config),
        ):
        pass
    yield
