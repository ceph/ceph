"""
Deploy and configure Dex OIDC identity provider for Teuthology.
"""
import contextlib
import logging

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run

log = logging.getLogger(__name__)

DEX_DEFAULT_VERSION = '2.38.0'
GO_DEFAULT_VERSION = '1.21.13'
DEX_PORT = 5556
DEX_CLIENT_ID = 'rgw-notifications'
DEX_CLIENT_SECRET = 'client-secret'
DEX_SCOPE = 'openid'


def get_dex_version(config):
    for client, client_config in config.items():
        if isinstance(client_config, dict) and 'dex_version' in client_config:
            return client_config['dex_version']
    return DEX_DEFAULT_VERSION


def get_dex_src_dir(ctx):
    return '{tdir}/dex-src'.format(tdir=teuthology.get_testdir(ctx))


def get_dex_bin(ctx):
    return '{src}/bin/dex'.format(src=get_dex_src_dir(ctx))


@contextlib.contextmanager
def install_dex(ctx, config):
    """Clone the Dex repo and build the binary from source using Go."""
    assert isinstance(config, dict)
    log.info('Building Dex from source...')
    testdir = teuthology.get_testdir(ctx)
    dex_version = get_dex_version(config)
    go_version = GO_DEFAULT_VERSION
    go_tarball = 'go{ver}.linux-amd64.tar.gz'.format(ver=go_version)
    go_url = 'https://go.dev/dl/{tarball}'.format(tarball=go_tarball)
    go_root = '{tdir}/go'.format(tdir=testdir)
    dex_src = get_dex_src_dir(ctx)

    for (client, _) in config.items():
        # Download and extract the Go toolchain
        ctx.cluster.only(client).run(
            args=[
                'wget', '-q', '-O',
                '{tdir}/{tarball}'.format(tdir=testdir, tarball=go_tarball),
                go_url,
            ],
        )
        ctx.cluster.only(client).run(
            args=[
                'tar', '-C', testdir, '-xzf',
                '{tdir}/{tarball}'.format(tdir=testdir, tarball=go_tarball),
            ],
        )

        # Clone Dex at the specified version tag
        ctx.cluster.only(client).run(
            args=[
                'git', 'clone', '--depth', '1',
                '--branch', 'v{ver}'.format(ver=dex_version),
                'https://github.com/dexidp/dex.git',
                dex_src,
            ],
        )

        # Build the dex binary
        ctx.cluster.only(client).run(
            args=[
                'bash', '-c',
                'cd {src} && PATH={go_root}/bin:$PATH make build'.format(
                    src=dex_src, go_root=go_root),
            ],
        )
        log.info('Dex binary built at %s/bin/dex', dex_src)

    try:
        yield
    finally:
        log.info('Removing Dex source tree and Go toolchain...')
        for (client, _) in config.items():
            ctx.cluster.only(client).run(
                args=[
                    'rm', '-rf',
                    dex_src,
                    go_root,
                    '{tdir}/{tarball}'.format(tdir=testdir, tarball=go_tarball),
                ],
            )


@contextlib.contextmanager
def configure_dex(ctx, config):
    """Write the Dex config file and populate ctx.dex_* attributes."""
    assert isinstance(config, dict)
    log.info('Configuring Dex...')
    testdir = teuthology.get_testdir(ctx)

    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        ip = remote.ip_address

        dex_config = (
            'issuer: http://{ip}:{port}/dex\n'
            'storage:\n'
            '  type: memory\n'
            'web:\n'
            '  http: 0.0.0.0:{port}\n'
            'oauth2:\n'
            '  grantTypes:\n'
            '    - client_credentials\n'
            '  responseTypes:\n'
            '    - code\n'
            '  skipApprovalScreen: true\n'
            'staticClients:\n'
            '  - id: {client_id}\n'
            '    secret: {client_secret}\n'
            '    name: RGW Notifications\n'
            '    grantTypes:\n'
            '      - client_credentials\n'
            'enablePasswordDB: false\n'
            'connectors:\n'
            '  - type: mockCallback\n'
            '    id: mock\n'
            '    name: Mock\n'
        ).format(
            ip=ip,
            port=DEX_PORT,
            client_id=DEX_CLIENT_ID,
            client_secret=DEX_CLIENT_SECRET,
        )

        remote.write_file(
            path='{tdir}/dex-config.yaml'.format(tdir=testdir),
            data=dex_config.encode(),
        )

        ctx.dex_issuer = 'http://{ip}:{port}/dex'.format(ip=ip, port=DEX_PORT)
        ctx.dex_jwks_url = 'http://{ip}:{port}/dex/keys'.format(ip=ip, port=DEX_PORT)
        ctx.dex_token_endpoint = 'http://{ip}:{port}/dex/token'.format(ip=ip, port=DEX_PORT)
        ctx.dex_client_id = DEX_CLIENT_ID
        ctx.dex_client_secret = DEX_CLIENT_SECRET
        ctx.dex_scope = DEX_SCOPE

    try:
        yield
    finally:
        log.info('Removing Dex config...')
        for (client, _) in config.items():
            ctx.cluster.only(client).run(
                args=['rm', '-f', '{tdir}/dex-config.yaml'.format(tdir=testdir)],
            )


@contextlib.contextmanager
def run_dex(ctx, config):
    """Start the Dex server, wait for readiness, and fetch an access token."""
    assert isinstance(config, dict)
    log.info('Starting Dex...')
    testdir = teuthology.get_testdir(ctx)

    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        ip = remote.ip_address
        dex_bin = get_dex_bin(ctx)
        config_path = '{tdir}/dex-config.yaml'.format(tdir=testdir)

        ctx.cluster.only(client).run(
            args=[dex_bin, 'serve', config_path, run.Raw('&'), 'exit'],
        )

        # Poll until Dex OIDC discovery endpoint responds (up to 30 s)
        ctx.cluster.only(client).run(
            args=[
                'bash', '-c',
                (
                    'for i in $(seq 1 30); do '
                    'curl -sf http://{ip}:{port}/dex/.well-known/openid-configuration '
                    '&& break; sleep 1; done'
                ).format(ip=ip, port=DEX_PORT),
            ],
        )

        # Fetch an access token for the configured client
        token = remote.sh([
            'curl', '-s', '-X', 'POST',
            'http://{ip}:{port}/dex/token'.format(ip=ip, port=DEX_PORT),
            '-d', 'grant_type=client_credentials',
            '-d', 'client_id={}'.format(DEX_CLIENT_ID),
            '-d', 'client_secret={}'.format(DEX_CLIENT_SECRET),
            '-d', 'scope={}'.format(DEX_SCOPE),
            run.Raw('|'),
            'python3', '-c', "import json,sys; print(json.load(sys.stdin)['access_token'])",
        ])
        ctx.dex_access_token = token.strip()
        log.info('Dex access token acquired')

    try:
        yield
    finally:
        log.info('Stopping Dex...')
        for (client, _) in config.items():
            ctx.cluster.only(client).run(
                args=['bash', '-c', 'pkill -f "dex serve" || true'],
            )


@contextlib.contextmanager
def task(ctx, config):
    """
    Deploy Dex OIDC identity provider for SASL/OAUTHBEARER Kafka tests.

    Example suite YAML usage::

      tasks:
      - dex:
          client.0:
            dex_version: 2.38.0
      - kafka:
          client.0:
            kafka_version: 3.9.2
      - notification-tests:
          client.0:
            extra_attr: ["kafka_test", "kafka_security_test"]
            rgw_server: client.0

    This task must run before the kafka task so that ctx.dex_* attributes
    are available when kafka writes server.properties.
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task dex only supports a list or dictionary for configuration"

    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)

    log.debug('Dex config is %s', config)

    with contextutil.nested(
        lambda: install_dex(ctx=ctx, config=config),
        lambda: configure_dex(ctx=ctx, config=config),
        lambda: run_dex(ctx=ctx, config=config),
    ):
        yield
