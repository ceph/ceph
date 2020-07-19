"""
Deploy and configure Tempest for Teuthology
"""
import configparser
import contextlib
import logging

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.exceptions import ConfigError
from teuthology.orchestra import run

log = logging.getLogger(__name__)


def get_tempest_dir(ctx):
    return '{tdir}/tempest'.format(tdir=teuthology.get_testdir(ctx))

def run_in_tempest_dir(ctx, client, cmdargs, **kwargs):
    ctx.cluster.only(client).run(
        args=[ 'cd', get_tempest_dir(ctx), run.Raw('&&'), ] + cmdargs,
        **kwargs
    )

def run_in_tempest_rgw_dir(ctx, client, cmdargs, **kwargs):
    ctx.cluster.only(client).run(
        args=[ 'cd', get_tempest_dir(ctx) + '/rgw', run.Raw('&&'), ] + cmdargs,
        **kwargs
    )

def run_in_tempest_venv(ctx, client, cmdargs, **kwargs):
    run_in_tempest_dir(ctx, client,
                        [   'source',
                            '.tox/venv/bin/activate',
                            run.Raw('&&')
                        ] + cmdargs, **kwargs)

@contextlib.contextmanager
def download(ctx, config):
    """
    Download the Tempest from github.
    Remove downloaded file upon exit.

    The context passed in should be identical to the context
    passed in to the main task.
    """
    assert isinstance(config, dict)
    log.info('Downloading Tempest...')
    for (client, cconf) in config.items():
        ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', cconf.get('force-branch', 'master'),
                'https://github.com/openstack/tempest.git',
                get_tempest_dir(ctx)
            ],
        )

        sha1 = cconf.get('sha1')
        if sha1 is not None:
            run_in_tempest_dir(ctx, client, [ 'git', 'reset', '--hard', sha1 ])
    try:
        yield
    finally:
        log.info('Removing Tempest...')
        for client in config:
            ctx.cluster.only(client).run(
                args=[ 'rm', '-rf', get_tempest_dir(ctx) ],
            )

def get_toxvenv_dir(ctx):
    return ctx.tox.venv_path

@contextlib.contextmanager
def setup_venv(ctx, config):
    """
    Setup the virtualenv for Tempest using tox.
    """
    assert isinstance(config, dict)
    log.info('Setting up virtualenv for Tempest')
    for (client, _) in config.items():
        run_in_tempest_dir(ctx, client,
            [   '{tvdir}/bin/tox'.format(tvdir=get_toxvenv_dir(ctx)),
                '-e', 'venv', '--notest'
            ])
    yield

def setup_logging(ctx, cpar):
    cpar.set('DEFAULT', 'log_dir', teuthology.get_archive_dir(ctx))
    cpar.set('DEFAULT', 'log_file', 'tempest.log')

def to_config(config, params, section, cpar):
    for (k, v) in config[section].items():
        if isinstance(v, str):
            v = v.format(**params)
        elif isinstance(v, bool):
            v = 'true' if v else 'false'
        else:
            v = str(v)
        cpar.set(section, k, v)

@contextlib.contextmanager
def configure_instance(ctx, config):
    assert isinstance(config, dict)
    log.info('Configuring Tempest')

    for (client, cconfig) in config.items():
        run_in_tempest_venv(ctx, client,
            [
                'tempest',
                'init',
                '--workspace-path',
                get_tempest_dir(ctx) + '/workspace.yaml',
                'rgw'
            ])

        # prepare the config file
        tetcdir = '{tdir}/rgw/etc'.format(tdir=get_tempest_dir(ctx))
        (remote,) = ctx.cluster.only(client).remotes.keys()
        local_conf = remote.get_file(tetcdir + '/tempest.conf.sample')

        # fill the params dictionary which allows to use templatized configs
        keystone_role = cconfig.get('use-keystone-role', None)
        if keystone_role is None \
            or keystone_role not in ctx.keystone.public_endpoints:
            raise ConfigError('the use-keystone-role is misconfigured')
        public_host, public_port = ctx.keystone.public_endpoints[keystone_role]
        params = {
            'keystone_public_host': public_host,
            'keystone_public_port': str(public_port),
        }

        cpar = configparser.ConfigParser()
        cpar.read(local_conf)
        setup_logging(ctx, cpar)
        to_config(cconfig, params, 'auth', cpar)
        to_config(cconfig, params, 'identity', cpar)
        to_config(cconfig, params, 'object-storage', cpar)
        to_config(cconfig, params, 'object-storage-feature-enabled', cpar)
        cpar.write(open(local_conf, 'w+'))

        remote.put_file(local_conf, tetcdir + '/tempest.conf')
    yield

@contextlib.contextmanager
def run_tempest(ctx, config):
    assert isinstance(config, dict)
    log.info('Configuring Tempest')

    for (client, cconf) in config.items():
        blacklist = cconf.get('blacklist', [])
        assert isinstance(blacklist, list)
        run_in_tempest_venv(ctx, client,
            [
                'tempest',
                'run',
                '--workspace-path',
                get_tempest_dir(ctx) + '/workspace.yaml',
                '--workspace',
                'rgw',
                '--regex', '^tempest.api.object_storage',
                '--black-regex', '|'.join(blacklist)
            ])
    try:
        yield
    finally:
        pass


@contextlib.contextmanager
def task(ctx, config):
    """
    Deploy and run Tempest's object storage campaign

    Example of configuration:

      overrides:
        ceph:
          conf:
            client:
              rgw keystone api version: 3
              rgw keystone accepted roles: admin,Member
              rgw keystone implicit tenants: true
              rgw keystone accepted admin roles: admin
              rgw swift enforce content length: true
              rgw swift account in url: true
              rgw swift versioning enabled: true
              rgw keystone admin domain: Default
              rgw keystone admin user: admin
              rgw keystone admin password: ADMIN
              rgw keystone admin project: admin
      tasks:
      # typically, the task should be preceded with install, ceph, tox,
      # keystone and rgw. Tox and Keystone are specific requirements
      # of tempest.py.
      - rgw:
          # it's important to match the prefix with the endpoint's URL
          # in Keystone. Additionally, if we want to test /info and its
          # accompanying stuff, the whole Swift API must be put in root
          # of the whole URL  hierarchy (read: frontend_prefix == /swift).
          frontend_prefix: /swift
          client.0:
            use-keystone-role: client.0
      - tempest:
          client.0:
            force-branch: master
            use-keystone-role: client.0
            auth:
              admin_username: admin
              admin_project_name: admin
              admin_password: ADMIN
              admin_domain_name: Default
            identity:
              uri: http://{keystone_public_host}:{keystone_public_port}/v2.0/
              uri_v3: http://{keystone_public_host}:{keystone_public_port}/v3/
              admin_role: admin
            object-storage:
              reseller_admin_role: admin
            object-storage-feature-enabled:
              container_sync: false
              discoverability: false
            blacklist:
              # please strip half of these items after merging PRs #15369
              # and #12704
              - .*test_list_containers_reverse_order.*
              - .*test_list_container_contents_with_end_marker.*
              - .*test_delete_non_empty_container.*
              - .*test_container_synchronization.*
              - .*test_get_object_after_expiration_time.*
              - .*test_create_object_with_transfer_encoding.*
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        'task tempest only supports a list or dictionary for configuration'

    if not ctx.tox:
        raise ConfigError('tempest must run after the tox task')
    if not ctx.keystone:
        raise ConfigError('tempest must run after the keystone task')

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
        teuthology.deep_merge(config[client], overrides.get('keystone', {}))

    log.debug('Tempest config is %s', config)

    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: setup_venv(ctx=ctx, config=config),
        lambda: configure_instance(ctx=ctx, config=config),
        lambda: run_tempest(ctx=ctx, config=config),
        ):
        yield
