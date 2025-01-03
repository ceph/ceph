"""
Deploy and configure Vault for Teuthology
"""

import argparse
import contextlib
import logging
import time
import json
from os import path
from http import client as http_client
from urllib.parse import urljoin

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run
from teuthology.exceptions import ConfigError, CommandFailedError


log = logging.getLogger(__name__)


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


@contextlib.contextmanager
def download(ctx, config):
    """
    Download Vault Release from Hashicorp website.
    Remove downloaded file upon exit.
    """
    assert isinstance(config, dict)
    log.info('Downloading Vault...')
    testdir = teuthology.get_testdir(ctx)

    for (client, cconf) in config.items():
        install_url = cconf.get('install_url')
        install_sha256 = cconf.get('install_sha256')
        if not install_url or not install_sha256:
            raise ConfigError("Missing Vault install_url and/or install_sha256")
        install_zip = path.join(testdir, 'vault.zip')
        install_dir = path.join(testdir, 'vault')

        log.info('Downloading Vault...')
        ctx.cluster.only(client).run(
            args=['curl', '-L', install_url, '-o', install_zip])

        log.info('Verifying SHA256 signature...')
        ctx.cluster.only(client).run(
            args=['echo', ' '.join([install_sha256, install_zip]), run.Raw('|'),
                  'sha256sum', '--check', '--status'])

        log.info('Extracting vault...')
        ctx.cluster.only(client).run(args=['mkdir', '-p', install_dir])
        # Using python in case unzip is not installed on hosts
        # Using python3 in case python is not installed on hosts
        failed=True
        for f in [
                lambda z,d: ['unzip', z, '-d', d],
                lambda z,d: ['python3', '-m', 'zipfile', '-e', z, d],
                lambda z,d: ['python', '-m', 'zipfile', '-e', z, d]]:
            try:
                ctx.cluster.only(client).run(args=f(install_zip, install_dir))
                failed = False
                break
            except CommandFailedError as e:
                failed = e
        if failed:
            raise failed

    try:
        yield
    finally:
        log.info('Removing Vault...')
        testdir = teuthology.get_testdir(ctx)
        for client in config:
            ctx.cluster.only(client).run(
                args=['rm', '-rf', install_dir, install_zip])


def get_vault_dir(ctx):
    return '{tdir}/vault'.format(tdir=teuthology.get_testdir(ctx))


@contextlib.contextmanager
def run_vault(ctx, config):
    assert isinstance(config, dict)

    for (client, cconf) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        cluster_name, _, client_id = teuthology.split_role(client)

        _, port = ctx.vault.endpoints[client]
        listen_addr = "0.0.0.0:{}".format(port)

        root_token = ctx.vault.root_token = cconf.get('root_token', 'root')

        log.info("Starting Vault listening on %s ...", listen_addr)
        v_params = [
            '-dev',
            '-dev-listen-address={}'.format(listen_addr),
            '-dev-no-store-token',
            '-dev-root-token-id={}'.format(root_token)
        ]

        cmd = "chmod +x {vdir}/vault && {vdir}/vault server {vargs}".format(vdir=get_vault_dir(ctx), vargs=" ".join(v_params))

        ctx.daemons.add_daemon(
            remote, 'vault', client_id,
            cluster=cluster_name,
            args=['bash', '-c', cmd, run.Raw('& { read; kill %1; }')],
            logger=log.getChild(client),
            stdin=run.PIPE,
            cwd=get_vault_dir(ctx),
            wait=False,
            check_status=False,
        )
        time.sleep(10)
    try:
        yield
    finally:
        log.info('Stopping Vault instance')
        ctx.daemons.get_daemon('vault', client_id, cluster_name).stop()


@contextlib.contextmanager
def setup_vault(ctx, config):
    """
    Mount Transit or KV version 2 secrets engine
    """
    (cclient, cconfig) = next(iter(config.items()))
    engine = cconfig.get('engine')

    if engine == 'kv':
        log.info('Mounting kv version 2 secrets engine')
        mount_path = '/v1/sys/mounts/kv'
        data = {
            "type": "kv",
            "options": {
                "version": "2"
            }
        }
    elif engine == 'transit':
        log.info('Mounting transit secrets engine')
        mount_path = '/v1/sys/mounts/transit'
        data = {
            "type": "transit"
        }
    else:
        raise Exception("Unknown or missing secrets engine")

    send_req(ctx, cconfig, cclient, mount_path, json.dumps(data))
    yield


def send_req(ctx, cconfig, client, path, body, method='POST'):
    host, port = ctx.vault.endpoints[client]
    req = http_client.HTTPConnection(host, port, timeout=30)
    token = cconfig.get('root_token', 'atoken')
    log.info("Send request to Vault: %s:%s at %s with token: %s", host, port, path, token)
    headers = {'X-Vault-Token': token}
    req.request(method, path, headers=headers, body=body)
    resp = req.getresponse()
    log.info(resp.read())
    if not (resp.status >= 200 and resp.status < 300):
        raise Exception("Request to Vault server failed with status %d" % resp.status)
    return resp


@contextlib.contextmanager
def create_secrets(ctx, config):
    (cclient, cconfig) = next(iter(config.items()))
    engine = cconfig.get('engine')
    prefix = cconfig.get('prefix')
    secrets = cconfig.get('secrets')
    flavor = cconfig.get('flavor')
    if secrets is None:
        raise ConfigError("No secrets specified, please specify some.")

    ctx.vault.keys[cclient] = []
    for secret in secrets:
        try:
            path = secret['path']
        except KeyError:
            raise ConfigError('Missing "path" field in secret')
        exportable = secret.get("exportable", flavor == "old")

        if engine == 'kv':
            try:
                data = {
                    "data": {
                        "key": secret['secret']
                    }
                }
            except KeyError:
                raise ConfigError('Missing "secret" field in secret')
        elif engine == 'transit':
            data = {"exportable": "true" if exportable else "false"}
        else:
            raise Exception("Unknown or missing secrets engine")

        send_req(ctx, cconfig, cclient, urljoin(prefix, path), json.dumps(data))

        ctx.vault.keys[cclient].append({ 'Path': path });

    log.info("secrets created")
    yield


@contextlib.contextmanager
def task(ctx, config):
    """
    Deploy and configure Vault

    Example of configuration:

    tasks:
    - vault:
        client.0:
          install_url: http://my.special.place/vault.zip
          install_sha256: zipfiles-sha256-sum-much-larger-than-this
          root_token: test_root_token
          engine: transit
          flavor: old
          prefix: /v1/transit/keys
          secrets:
            - path: kv/teuthology/key_a
              secret: base64_only_if_using_kv_aWxkCmNlcGguY29uZgo=
              exportable: true
            - path: kv/teuthology/key_b
              secret: base64_only_if_using_kv_dApzcmMKVGVzdGluZwo=

    engine can be 'kv' or 'transit'
    prefix should be /v1/kv/data/ for kv, /v1/transit/keys/ for transit
    flavor should be 'old' only if testing the original transit logic
        otherwise omit.
    for kv only: 256-bit key value should be specified via secret,
        otherwise should omit.
    for transit: exportable may be used to make individual keys exportable.
    flavor may be set to 'old' to make all keys exportable by default,
        which is required by the original transit logic.
    """
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
        teuthology.deep_merge(config[client], overrides.get('vault', {}))

    log.debug('Vault config is %s', config)

    ctx.vault = argparse.Namespace()
    ctx.vault.endpoints = assign_ports(ctx, config, 8200)
    ctx.vault.root_token = None
    ctx.vault.prefix = config[client].get('prefix')
    ctx.vault.engine = config[client].get('engine')
    ctx.vault.keys = {}
    q=config[client].get('flavor')
    if q:
        ctx.vault.flavor = q

    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: run_vault(ctx=ctx, config=config),
        lambda: setup_vault(ctx=ctx, config=config),
        lambda: create_secrets(ctx=ctx, config=config)
        ):
        yield

