"""
Deploy and configure Barbican for Teuthology
"""
import argparse
import contextlib
import logging
import http
import json
import time
import math

from urllib.parse import urlparse

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run
from teuthology.exceptions import ConfigError

log = logging.getLogger(__name__)


@contextlib.contextmanager
def download(ctx, config):
    """
    Download the Barbican from github.
    Remove downloaded file upon exit.

    The context passed in should be identical to the context
    passed in to the main task.
    """
    assert isinstance(config, dict)
    log.info('Downloading barbican...')
    testdir = teuthology.get_testdir(ctx)
    for (client, cconf) in config.items():
        branch = cconf.get('force-branch', 'master')
        log.info("Using branch '%s' for barbican", branch)

        sha1 = cconf.get('sha1')
        log.info('sha1=%s', sha1)

        ctx.cluster.only(client).run(
            args=[
                'bash', '-l'
                ],
            )
        ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', branch,
                'https://github.com/openstack/barbican.git',
                '{tdir}/barbican'.format(tdir=testdir),
                ],
            )
        if sha1 is not None:
            ctx.cluster.only(client).run(
                args=[
                    'cd', '{tdir}/barbican'.format(tdir=testdir),
                    run.Raw('&&'),
                    'git', 'reset', '--hard', sha1,
                    ],
                )
    try:
        yield
    finally:
        log.info('Removing barbican...')
        testdir = teuthology.get_testdir(ctx)
        for client in config:
            ctx.cluster.only(client).run(
                args=[
                    'rm',
                    '-rf',
                    '{tdir}/barbican'.format(tdir=testdir),
                    ],
                )

def get_barbican_dir(ctx):
    return '{tdir}/barbican'.format(tdir=teuthology.get_testdir(ctx))

def run_in_barbican_dir(ctx, client, args):
    ctx.cluster.only(client).run(
        args=['cd', get_barbican_dir(ctx), run.Raw('&&'), ] + args,
    )

def run_in_barbican_venv(ctx, client, args):
    run_in_barbican_dir(ctx, client,
                        ['.',
                         '.barbicanenv/bin/activate',
                         run.Raw('&&')
                        ] + args)

@contextlib.contextmanager
def setup_venv(ctx, config):
    """
    Setup the virtualenv for Barbican using pip.
    """
    assert isinstance(config, dict)
    log.info('Setting up virtualenv for barbican...')
    for (client, _) in config.items():
        run_in_barbican_dir(ctx, client,
                            ['python3', '-m', 'venv', '.barbicanenv'])
        run_in_barbican_venv(ctx, client,
                             ['pip', 'install', '--upgrade', 'pip'])
        run_in_barbican_venv(ctx, client,
                             ['pip', 'install', 'pytz',
                              '-e', get_barbican_dir(ctx)])
    yield

def assign_ports(ctx, config, initial_port):
    """
    Assign port numbers starting from @initial_port
    """
    port = initial_port
    role_endpoints = {}
    for remote, roles_for_host in ctx.cluster.remotes.items():
        for role in roles_for_host:
            if role in config:
                role_endpoints[role] = (remote.name.split('@')[1], port)
                port += 1

    return role_endpoints

def set_authtoken_params(ctx, cclient, cconfig):
    section_config_list = cconfig['keystone_authtoken'].items()
    for config in section_config_list:
        (name, val) = config
        run_in_barbican_dir(ctx, cclient,
                            ['sed', '-i',
                             '/[[]filter:authtoken]/{p;s##'+'{} = {}'.format(name, val)+'#;}',
                             'etc/barbican/barbican-api-paste.ini'])

    keystone_role = cconfig.get('use-keystone-role', None)
    public_host, public_port = ctx.keystone.public_endpoints[keystone_role]
    url = 'http://{host}:{port}/v3'.format(host=public_host,
                                           port=public_port)
    run_in_barbican_dir(ctx, cclient,
                        ['sed', '-i',
                         '/[[]filter:authtoken]/{p;s##'+'auth_uri = {}'.format(url)+'#;}',
                         'etc/barbican/barbican-api-paste.ini'])
    admin_url = 'http://{host}:{port}/v3'.format(host=public_host,
                                                 port=public_port)
    run_in_barbican_dir(ctx, cclient,
                        ['sed', '-i',
                         '/[[]filter:authtoken]/{p;s##'+'auth_url = {}'.format(admin_url)+'#;}',
                         'etc/barbican/barbican-api-paste.ini'])

def fix_barbican_api_paste(ctx, cclient):
    run_in_barbican_dir(ctx, cclient,
                        ['sed', '-i', '-n',
                         '/\\[pipeline:barbican_api]/ {p;n; /^pipeline =/ '+
                         '{ s/.*/pipeline = unauthenticated-context apiapp/;p;d } } ; p',
                         './etc/barbican/barbican-api-paste.ini'])

def fix_barbican_api(ctx, cclient):
    run_in_barbican_dir(ctx, cclient,
                        ['sed', '-i',
                         '/prop_dir =/ s#etc/barbican#{}/etc/barbican#'.format(get_barbican_dir(ctx)),
                         'bin/barbican-api'])

def create_barbican_conf(ctx, cclient):
    barbican_host, barbican_port = ctx.barbican.endpoints[cclient]
    barbican_url = 'http://{host}:{port}'.format(host=barbican_host,
                                                 port=barbican_port)
    log.info("barbican url=%s", barbican_url)

    run_in_barbican_dir(ctx, cclient,
                        ['bash', '-c',
                         'echo -n -e "[DEFAULT]\nhost_href=' + barbican_url + '\n" ' + \
                         '>barbican.conf'])

    log.info("run barbican db upgrade")
    config_path = get_barbican_dir(ctx) + '/barbican.conf'
    run_in_barbican_venv(ctx, cclient, ['barbican-manage', '--config-file', config_path,
                                        'db', 'upgrade'])
    log.info("run barbican db sync_secret_stores")
    run_in_barbican_venv(ctx, cclient, ['barbican-manage', '--config-file', config_path,
                                        'db', 'sync_secret_stores'])

@contextlib.contextmanager
def configure_barbican(ctx, config):
    """
    Configure barbican paste-api and barbican-api.
    """
    assert isinstance(config, dict)
    (cclient, cconfig) = next(iter(config.items()))

    keystone_role = cconfig.get('use-keystone-role', None)
    if keystone_role is None:
        raise ConfigError('use-keystone-role not defined in barbican task')

    set_authtoken_params(ctx, cclient, cconfig)
    fix_barbican_api(ctx, cclient)
    fix_barbican_api_paste(ctx, cclient)
    create_barbican_conf(ctx, cclient)
    try:
        yield
    finally:
        pass

@contextlib.contextmanager
def run_barbican(ctx, config):
    assert isinstance(config, dict)
    log.info('Running barbican...')

    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        cluster_name, _, client_id = teuthology.split_role(client)

        # start the public endpoint
        client_public_with_id = 'barbican.public' + '.' + client_id

        run_cmd = ['cd', get_barbican_dir(ctx), run.Raw('&&'),
                   '.', '.barbicanenv/bin/activate', run.Raw('&&'),
                   'HOME={}'.format(get_barbican_dir(ctx)), run.Raw('&&'),
                   'bin/barbican-api',
                   run.Raw('& { read; kill %1; }')]
                   #run.Raw('1>/dev/null')

        run_cmd = 'cd ' + get_barbican_dir(ctx) + ' && ' + \
                  '. .barbicanenv/bin/activate && ' + \
                  'HOME={}'.format(get_barbican_dir(ctx)) + ' && ' + \
                  'exec bin/barbican-api & { read; kill %1; }'

        ctx.daemons.add_daemon(
            remote, 'barbican', client_public_with_id,
            cluster=cluster_name,
            args=['bash', '-c', run_cmd],
            logger=log.getChild(client),
            stdin=run.PIPE,
            cwd=get_barbican_dir(ctx),
            wait=False,
            check_status=False,
        )

        # sleep driven synchronization
        run_in_barbican_venv(ctx, client, ['sleep', '15'])
    try:
        yield
    finally:
        log.info('Stopping Barbican instance')
        ctx.daemons.get_daemon('barbican', client_public_with_id,
                               cluster_name).stop()


@contextlib.contextmanager
def create_secrets(ctx, config):
    """
    Create a main and an alternate s3 user.
    """
    assert isinstance(config, dict)
    (cclient, cconfig) = next(iter(config.items()))

    rgw_user = cconfig['rgw_user']

    keystone_role = cconfig.get('use-keystone-role', None)
    keystone_host, keystone_port = ctx.keystone.public_endpoints[keystone_role]
    barbican_host, barbican_port = ctx.barbican.endpoints[cclient]
    barbican_url = 'http://{host}:{port}'.format(host=barbican_host,
                                                 port=barbican_port)
    log.info("barbican_url=%s", barbican_url)
    #fetching user_id of user that gets secrets for radosgw
    token_req = http.client.HTTPConnection(keystone_host, keystone_port, timeout=30)
    token_req.request(
        'POST',
        '/v3/auth/tokens',
        headers={'Content-Type':'application/json'},
        body=json.dumps({
            "auth": {
                "identity": {
                    "methods": ["password"],
                    "password": {
                        "user": {
                            "domain": {"id": "default"},
                            "name": rgw_user["username"],
                            "password": rgw_user["password"]
                        }
                    }
                },
                "scope": {
                    "project": {
                        "domain": {"id": "default"},
                        "name": rgw_user["tenantName"]
                    }
                }
            }
        }))
    rgw_access_user_resp = token_req.getresponse()
    if not (rgw_access_user_resp.status >= 200 and
            rgw_access_user_resp.status < 300):
        raise Exception("Cannot authenticate user "+rgw_user["username"]+" for secret creation")
    #    baru_resp = json.loads(baru_req.data)
    rgw_access_user_data = json.loads(rgw_access_user_resp.read().decode())
    rgw_user_id = rgw_access_user_data['token']['user']['id']
    if 'secrets' in cconfig:
        for secret in cconfig['secrets']:
            if 'name' not in secret:
                raise ConfigError('barbican.secrets must have "name" field')
            if 'base64' not in secret:
                raise ConfigError('barbican.secrets must have "base64" field')
            if 'tenantName' not in secret:
                raise ConfigError('barbican.secrets must have "tenantName" field')
            if 'username' not in secret:
                raise ConfigError('barbican.secrets must have "username" field')
            if 'password' not in secret:
                raise ConfigError('barbican.secrets must have "password" field')

            token_req = http.client.HTTPConnection(keystone_host, keystone_port, timeout=30)
            token_req.request(
                'POST',
                '/v3/auth/tokens',
                headers={'Content-Type':'application/json'},
                body=json.dumps({
                    "auth": {
                        "identity": {
                            "methods": ["password"],
                            "password": {
                                "user": {
                                    "domain": {"id": "default"},
                                    "name": secret["username"],
                                    "password": secret["password"]
                                }
                            }
                        },
                        "scope": {
                            "project": {
                                "domain": {"id": "default"},
                                "name": secret["tenantName"]
                            }
                        }
                    }
                }))
            token_resp = token_req.getresponse()
            if not (token_resp.status >= 200 and
                    token_resp.status < 300):
                raise Exception("Cannot authenticate user "+secret["username"]+" for secret creation")

            expire = time.time() + 5400		# now + 90m
            (expire_fract,dummy) = math.modf(expire)
            expire_format = "%%FT%%T.%06d" % (round(expire_fract*1000000))
            expiration = time.strftime(expire_format, time.gmtime(expire))
            token_id = token_resp.getheader('x-subject-token')

            key1_json = json.dumps(
                {
                    "name": secret['name'],
                    "expiration": expiration,
                    "algorithm": "aes",
                    "bit_length": 256,
                    "mode": "cbc",
                    "payload": secret['base64'],
                    "payload_content_type": "application/octet-stream",
                    "payload_content_encoding": "base64"
                })

            sec_req = http.client.HTTPConnection(barbican_host, barbican_port, timeout=30)
            try:
                sec_req.request(
                    'POST',
                    '/v1/secrets',
                    headers={'Content-Type': 'application/json',
                             'Accept': '*/*',
                             'X-Auth-Token': token_id},
                    body=key1_json
                )
            except:
                log.info("catched exception!")
                run_in_barbican_venv(ctx, cclient, ['sleep', '900'])

            barbican_sec_resp = sec_req.getresponse()
            if not (barbican_sec_resp.status >= 200 and
                    barbican_sec_resp.status < 300):
                raise Exception("Cannot create secret")
            barbican_data = json.loads(barbican_sec_resp.read().decode())
            if 'secret_ref' not in barbican_data:
                raise ValueError("Malformed secret creation response")
            secret_ref = barbican_data["secret_ref"]
            log.info("secret_ref=%s", secret_ref)
            secret_url_parsed = urlparse(secret_ref)
            acl_json = json.dumps(
                {
                    "read": {
                        "users": [rgw_user_id],
                        "project-access": True
                    }
                })
            acl_req = http.client.HTTPConnection(secret_url_parsed.netloc, timeout=30)
            acl_req.request(
                'PUT',
                secret_url_parsed.path+'/acl',
                headers={'Content-Type': 'application/json',
                         'Accept': '*/*',
                         'X-Auth-Token': token_id},
                body=acl_json
            )
            barbican_acl_resp = acl_req.getresponse()
            if not (barbican_acl_resp.status >= 200 and
                    barbican_acl_resp.status < 300):
                raise Exception("Cannot set ACL for secret")

            key = {'id': secret_ref.split('secrets/')[1], 'payload': secret['base64']}
            ctx.barbican.keys[secret['name']] = key

    run_in_barbican_venv(ctx, cclient, ['sleep', '3'])
    try:
        yield
    finally:
        pass


@contextlib.contextmanager
def task(ctx, config):
    """
    Deploy and configure Keystone

    Example of configuration:

    tasks:
      - local_cluster:
          cluster_path: /home/adam/ceph-1/build
      - local_rgw:
      - tox: [ client.0 ]
      - keystone:
          client.0:
            sha1: 17.0.0.0rc2
            force-branch: master
            projects:
              - name: rgwcrypt
                description: Encryption Tenant
              - name: barbican
                description: Barbican
              - name: s3
                description: S3 project
            users:
              - name: rgwcrypt-user
                password: rgwcrypt-pass
                project: rgwcrypt
              - name: barbican-user
                password: barbican-pass
                project: barbican
              - name: s3-user
                password: s3-pass
                project: s3
            roles: [ name: Member, name: creator ]
            role-mappings:
              - name: Member
                user: rgwcrypt-user
                project: rgwcrypt
              - name: admin
                user: barbican-user
                project: barbican
              - name: creator
                user: s3-user
                project: s3
            services:
              - name: keystone
                type: identity
                description: Keystone Identity Service
      - barbican:
          client.0:
            force-branch: master
            use-keystone-role: client.0
            keystone_authtoken:
              auth_plugin: password
              username: barbican-user
              password: barbican-pass
              user_domain_name: Default
            rgw_user:
              tenantName: rgwcrypt
              username: rgwcrypt-user
              password: rgwcrypt-pass
            secrets:
              - name: my-key-1
                base64: a2V5MS5GcWVxKzhzTGNLaGtzQkg5NGVpb1FKcFpGb2c=
                tenantName: s3
                username: s3-user
                password: s3-pass
              - name: my-key-2
                base64: a2V5Mi5yNUNNMGFzMVdIUVZxcCt5NGVmVGlQQ1k4YWg=
                tenantName: s3
                username: s3-user
                password: s3-pass
      - s3tests:
          client.0:
            force-branch: master
            kms_key: my-key-1
      - rgw:
          client.0:
            use-keystone-role: client.0
            use-barbican-role: client.0
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task keystone only supports a list or dictionary for configuration"
    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)

    overrides = ctx.config.get('overrides', {})
    # merge each client section, not the top level.
    for client in config.keys():
        if not config[client]:
            config[client] = {}
        teuthology.deep_merge(config[client], overrides.get('barbican', {}))

    log.debug('Barbican config is %s', config)

    if not hasattr(ctx, 'keystone'):
        raise ConfigError('barbican must run after the keystone task')


    ctx.barbican = argparse.Namespace()
    ctx.barbican.endpoints = assign_ports(ctx, config, 9311)
    ctx.barbican.keys = {}
    
    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: setup_venv(ctx=ctx, config=config),
        lambda: configure_barbican(ctx=ctx, config=config),
        lambda: run_barbican(ctx=ctx, config=config),
        lambda: create_secrets(ctx=ctx, config=config),
        ):
        yield
