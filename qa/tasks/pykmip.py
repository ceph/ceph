"""
Deploy and configure PyKMIP for Teuthology
"""
import argparse
import contextlib
import logging
import time
import tempfile
import json
import os
from io import BytesIO
from teuthology.orchestra.daemon import DaemonGroup
from teuthology.orchestra.remote import Remote

import pprint

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run
from teuthology.packaging import install_package
from teuthology.packaging import remove_package
from teuthology.exceptions import ConfigError
from tasks.util import get_remote_for_role

log = logging.getLogger(__name__)


def get_pykmip_dir(ctx):
    return '{tdir}/pykmip'.format(tdir=teuthology.get_testdir(ctx))

def run_in_pykmip_dir(ctx, client, args, **kwargs):
    (remote,) = [client] if isinstance(client,Remote) else ctx.cluster.only(client).remotes.keys()
    return remote.run(
        args=['cd', get_pykmip_dir(ctx), run.Raw('&&'), ] + args,
        **kwargs
    )

def run_in_pykmip_venv(ctx, client, args, **kwargs):
    return run_in_pykmip_dir(ctx, client,
        args = ['.', '.pykmipenv/bin/activate',
                         run.Raw('&&')
                        ] + args, **kwargs)

@contextlib.contextmanager
def download(ctx, config):
    """
    Download PyKMIP from github.
    Remove downloaded file upon exit.

    The context passed in should be identical to the context
    passed in to the main task.
    """
    assert isinstance(config, dict)
    log.info('Downloading pykmip...')
    pykmipdir = get_pykmip_dir(ctx)

    for (client, cconf) in config.items():
        branch = cconf.get('force-branch', 'master')
        repo = cconf.get('force-repo', 'https://github.com/OpenKMIP/PyKMIP')
        sha1 = cconf.get('sha1')
        log.info("Using branch '%s' for pykmip", branch)
        log.info('sha1=%s', sha1)

        ctx.cluster.only(client).run(
            args=[
                'git', 'clone', '-b', branch, repo,
                pykmipdir,
                ],
            )
        if sha1 is not None:
            run_in_pykmip_dir(ctx, client, [
                    'git', 'reset', '--hard', sha1,
                ],
            )
    try:
        yield
    finally:
        log.info('Removing pykmip...')
        for client in config:
            ctx.cluster.only(client).run(
                args=[ 'rm', '-rf', pykmipdir ],
            )

_bindep_txt = """# should be part of PyKMIP
libffi-dev [platform:dpkg]
libffi-devel [platform:rpm]
libssl-dev [platform:dpkg]
openssl-devel [platform:redhat]
libopenssl-devel [platform:suse]
libsqlite3-dev [platform:dpkg]
sqlite-devel [platform:rpm]
python-dev [platform:dpkg]
python-devel [(platform:redhat platform:base-py2)]
python3-dev [platform:dpkg]
python3-devel [(platform:redhat platform:base-py3) platform:suse]
python3 [platform:suse]
"""

@contextlib.contextmanager
def install_packages(ctx, config):
    """
    Download the packaged dependencies of PyKMIP.
    Remove install packages upon exit.

    The context passed in should be identical to the context
    passed in to the main task.
    """
    assert isinstance(config, dict)
    log.info('Installing system dependencies for PyKMIP...')

    packages = {}
    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        # use bindep to read which dependencies we need from temp/bindep.txt
        fd, local_temp_path = tempfile.mkstemp(suffix='.txt',
                                               prefix='bindep-')
        os.write(fd, _bindep_txt.encode())
        os.close(fd)
        fd, remote_temp_path = tempfile.mkstemp(suffix='.txt',
                                               prefix='bindep-')
        os.close(fd)
        remote.put_file(local_temp_path, remote_temp_path)
        os.remove(local_temp_path)
        run_in_pykmip_venv(ctx, remote, ['pip', 'install', 'bindep'])
        r = run_in_pykmip_venv(ctx, remote,
                ['bindep', '--brief', '--file', remote_temp_path],
                stdout=BytesIO(),
                check_status=False) # returns 1 on success?
        packages[client] = r.stdout.getvalue().decode().splitlines()
        for dep in packages[client]:
            install_package(dep, remote)
    try:
        yield
    finally:
        log.info('Removing system dependencies of PyKMIP...')

        for (client, _) in config.items():
            (remote,) = ctx.cluster.only(client).remotes.keys()
            for dep in packages[client]:
                remove_package(dep, remote)

@contextlib.contextmanager
def setup_venv(ctx, config):
    """
    Setup the virtualenv for PyKMIP using pip.
    """
    assert isinstance(config, dict)
    log.info('Setting up virtualenv for pykmip...')
    for (client, _) in config.items():
        run_in_pykmip_dir(ctx, client, ['python3', '-m', 'venv', '.pykmipenv'])
        run_in_pykmip_venv(ctx, client, ['pip', 'install', '--upgrade', 'pip'])
        run_in_pykmip_venv(ctx, client, ['pip', 'install', 'pytz', '-e', get_pykmip_dir(ctx)])
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
                r = get_remote_for_role(ctx, role)
                role_endpoints[role] = r.ip_address, port, r.hostname
                port += 1

    return role_endpoints

def copy_policy_json(ctx, cclient, cconfig):
    run_in_pykmip_dir(ctx, cclient,
                        ['cp',
                         get_pykmip_dir(ctx)+'/examples/policy.json',
                         get_pykmip_dir(ctx)])

_pykmip_configuration = """# configuration for pykmip
[server]
hostname={ipaddr}
port={port}
certificate_path={servercert}
key_path={serverkey}
ca_path={clientca}
auth_suite=TLS1.2
policy_path={confdir}
enable_tls_client_auth=False
tls_cipher_suites=
    TLS_RSA_WITH_AES_128_CBC_SHA256
    TLS_RSA_WITH_AES_256_CBC_SHA256
    TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384
logging_level=DEBUG
database_path={confdir}/pykmip.sqlite
[client]
host={hostname}
port=5696
certfile={clientcert}
keyfile={clientkey}
ca_certs={clientca}
ssl_version=PROTOCOL_TLSv1_2
"""

def create_pykmip_conf(ctx, cclient, cconfig):
    log.info('#0 cclient={} cconfig={}'.format(pprint.pformat(cclient),pprint.pformat(cconfig)))
    (remote,) = ctx.cluster.only(cclient).remotes.keys()
    pykmip_ipaddr, pykmip_port, pykmip_hostname = ctx.pykmip.endpoints[cclient]
    log.info('#1 ip,p,h {} {} {}'.format(pykmip_ipaddr, pykmip_port, pykmip_hostname))
    clientca = cconfig.get('clientca', None)
    log.info('#2 clientca {}'.format(clientca))
    serverkey = None
    servercert = cconfig.get('servercert', None)
    log.info('#3 servercert {}'.format(servercert))
    servercert = ctx.ssl_certificates.get(servercert)
    log.info('#4 servercert {}'.format(servercert))
    clientkey = None
    clientcert = cconfig.get('clientcert', None)
    log.info('#3 clientcert {}'.format(clientcert))
    clientcert = ctx.ssl_certificates.get(clientcert)
    log.info('#4 clientcert {}'.format(clientcert))
    clientca = ctx.ssl_certificates.get(clientca)
    log.info('#5 clientca {}'.format(clientca))
    if servercert != None:
      serverkey = servercert.key
      servercert = servercert.certificate
      log.info('#6 serverkey {} servercert {}'.format(serverkey, servercert))
    if clientcert != None:
      clientkey = clientcert.key
      clientcert = clientcert.certificate
      log.info('#6 clientkey {} clientcert {}'.format(clientkey, clientcert))
    if clientca != None:
      clientca = clientca.certificate
      log.info('#7 clientca {}'.format(clientca))
    if servercert == None or clientca == None or serverkey == None:
      log.info('#8 clientca {} serverkey {} servercert {}'.format(clientca, serverkey, servercert))
      raise ConfigError('pykmip: Missing/bad servercert or clientca')
    pykmipdir = get_pykmip_dir(ctx)
    kmip_conf = _pykmip_configuration.format(
        ipaddr=pykmip_ipaddr,
        port=pykmip_port,
        confdir=pykmipdir,
        hostname=pykmip_hostname,
        clientca=clientca,
        clientkey=clientkey,
        clientcert=clientcert,
        serverkey=serverkey,
        servercert=servercert
    )
    fd, local_temp_path = tempfile.mkstemp(suffix='.conf',
                                           prefix='pykmip')
    os.write(fd, kmip_conf.encode())
    os.close(fd)
    remote.put_file(local_temp_path, pykmipdir+'/pykmip.conf')
    os.remove(local_temp_path)

@contextlib.contextmanager
def configure_pykmip(ctx, config):
    """
    Configure pykmip paste-api and pykmip-api.
    """
    assert isinstance(config, dict)
    (cclient, cconfig) = next(iter(config.items()))

    copy_policy_json(ctx, cclient, cconfig)
    create_pykmip_conf(ctx, cclient, cconfig)
    try:
        yield
    finally:
        pass

def has_ceph_task(tasks):
    for task in tasks:
        for name, conf in task.items():
            if name == 'ceph':
                return True
    return False

@contextlib.contextmanager
def run_pykmip(ctx, config):
    assert isinstance(config, dict)
    if hasattr(ctx, 'daemons'):
        pass
    elif has_ceph_task(ctx.config['tasks']):
        log.info('Delay start pykmip so ceph can do once-only daemon logic')
        try:
            yield
        finally:
            pass
    else:
        ctx.daemons = DaemonGroup()
    log.info('Running pykmip...')

    pykmipdir = get_pykmip_dir(ctx)

    for (client, _) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        cluster_name, _, client_id = teuthology.split_role(client)

        # start the public endpoint
        client_public_with_id = 'pykmip.public' + '.' + client_id

        run_cmd = 'cd ' + pykmipdir + ' && ' + \
                  '. .pykmipenv/bin/activate && ' + \
                  'HOME={}'.format(pykmipdir) + ' && ' + \
                  'exec pykmip-server -f pykmip.conf -l ' + \
                  pykmipdir + '/pykmip.log & { read; kill %1; }'

        ctx.daemons.add_daemon(
            remote, 'pykmip', client_public_with_id,
            cluster=cluster_name,
            args=['bash', '-c', run_cmd],
            logger=log.getChild(client),
            stdin=run.PIPE,
            cwd=pykmipdir,
            wait=False,
            check_status=False,
        )

        # sleep driven synchronization
        time.sleep(10)
    try:
        yield
    finally:
        log.info('Stopping PyKMIP instance')
        ctx.daemons.get_daemon('pykmip', client_public_with_id,
                               cluster_name).stop()

make_keys_template = """
from kmip.pie import client
from kmip import enums
import ssl
import sys
import json
from io import BytesIO

c = client.ProxyKmipClient(config_file="{replace-with-config-file-path}")

rl=[]
for kwargs in {replace-with-secrets}:
 with c:
  key_id = c.create(
   enums.CryptographicAlgorithm.AES,
   256,
   operation_policy_name='default',
   cryptographic_usage_mask=[
    enums.CryptographicUsageMask.ENCRYPT,
    enums.CryptographicUsageMask.DECRYPT
   ],
   **kwargs
  )
  c.activate(key_id)
  attrs = c.get_attributes(uid=key_id)
  r = {}
  for a in attrs[1]:
   r[str(a.attribute_name)] = str(a.attribute_value)
  rl.append(r)
print(json.dumps(rl))
"""

@contextlib.contextmanager
def create_secrets(ctx, config):
    """
    Create and activate any requested keys in kmip
    """
    assert isinstance(config, dict)

    pykmipdir = get_pykmip_dir(ctx)
    pykmip_conf_path = pykmipdir + '/pykmip.conf'
    my_output = BytesIO()
    for (client,cconf) in config.items():
        (remote,) = ctx.cluster.only(client).remotes.keys()
        secrets=cconf.get('secrets')
        if secrets:
            secrets_json = json.dumps(cconf['secrets'])
            make_keys = make_keys_template \
                .replace("{replace-with-secrets}",secrets_json) \
                .replace("{replace-with-config-file-path}",pykmip_conf_path)
            my_output.truncate()
            remote.run(args=[run.Raw('. cephtest/pykmip/.pykmipenv/bin/activate;' \
                + 'python')], stdin=make_keys, stdout = my_output)
            ctx.pykmip.keys[client] = json.loads(my_output.getvalue().decode())
    try:
        yield
    finally:
        pass

@contextlib.contextmanager
def task(ctx, config):
    """
    Deploy and configure PyKMIP

    Example of configuration:

    tasks:
    - install:
    - ceph:
       conf:
        client:
         rgw crypt s3 kms backend: kmip
         rgw crypt kmip ca path: /home/ubuntu/cephtest/ca/kmiproot.crt
         rgw crypt kmip client cert: /home/ubuntu/cephtest/ca/kmip-client.crt
         rgw crypt kmip client key: /home/ubuntu/cephtest/ca/kmip-client.key
         rgw crypt kmip kms key template: pykmip-$keyid
    - openssl_keys:
       kmiproot:
         client: client.0
         cn: kmiproot
         key-type: rsa:4096
    - openssl_keys:
       kmip-server:
         client: client.0
         ca: kmiproot
       kmip-client:
         client: client.0
         ca: kmiproot
         cn: rgw-client
    - pykmip:
        client.0:
          force-branch: master
          clientca: kmiproot
          servercert: kmip-server
          clientcert: kmip-client
          secrets:
          - name: pykmip-key-1
          - name: pykmip-key-2
    - rgw:
        client.0:
          use-pykmip-role: client.0
    - s3tests:
        client.0:
          force-branch: master
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task pykmip only supports a list or dictionary for configuration"
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
        teuthology.deep_merge(config[client], overrides.get('pykmip', {}))

    log.debug('PyKMIP config is %s', config)

    if not hasattr(ctx, 'ssl_certificates'):
        raise ConfigError('pykmip must run after the openssl_keys task')


    ctx.pykmip = argparse.Namespace()
    ctx.pykmip.endpoints = assign_ports(ctx, config, 5696)
    ctx.pykmip.keys = {}
    
    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: setup_venv(ctx=ctx, config=config),
        lambda: install_packages(ctx=ctx, config=config),
        lambda: configure_pykmip(ctx=ctx, config=config),
        lambda: run_pykmip(ctx=ctx, config=config),
        lambda: create_secrets(ctx=ctx, config=config),
        ):
        yield
