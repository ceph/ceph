"""
Deploy and configure Tempest for Teuthology
"""
from cStringIO import StringIO
from configobj import ConfigObj
import base64
import contextlib
import logging
import os
import random
import string
import sys

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology import safepath
from teuthology.config import config as teuth_config
from teuthology.orchestra import run
from teuthology.orchestra.connection import split_user

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
    s3_branches = [ 'giant', 'firefly', 'firefly-original', 'hammer' ]
    for (client, cconf) in config.items():
        branch = cconf.get('force-branch', None)
        if not branch:
            ceph_branch = ctx.config.get('branch')
            suite_branch = ctx.config.get('suite_branch', ceph_branch)
            if suite_branch in s3_branches:
                branch = cconf.get('branch', suite_branch)
	    else:
                branch = cconf.get('branch', 'ceph-' + suite_branch)
        if not branch:
            raise ValueError(
                "Could not determine what branch to use for Tempest!")
        else:
            log.info("Using branch '%s' for Tempest", branch)

        sha1 = cconf.get('sha1')
        ctx.cluster.only(client).run(
            args=[
                'git', 'clone',
                '-b', branch,
                'https://github.com/openstack/tempest.git',
                get_tempest_dir(ctx)
                ],
            )
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

def to_config(config, section, cpar):
    for (k, v) in config[section].items():
        cpar.set(section, k, v)

@contextlib.contextmanager
def configure_instance(ctx, config):
    assert isinstance(config, dict)
    log.info('Configuring Tempest')

    import ConfigParser
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

        cpar = ConfigParser.ConfigParser()
        cpar.read(local_conf)
        setup_logging(ctx, cpar)
        to_config(cconfig, 'auth', cpar)
        to_config(cconfig, 'identity', cpar)
        to_config(cconfig, 'object-storage', cpar)
        to_config(cconfig, 'object-storage-feature-enabled', cpar)
        cpar.write(file(local_conf, 'w+'))

        remote.put_file(local_conf, tetcdir + '/tempest.conf')
    yield

@contextlib.contextmanager
def run_tempest(ctx, config):
    assert isinstance(config, dict)
    log.info('Configuring Tempest')

    for (client, _) in config.items():
        run_in_tempest_venv(ctx, client,
            [
                'tempest',
                'run',
                '--workspace-path',
                get_tempest_dir(ctx) + '/workspace.yaml',
                '--workspace',
                'rgw',
                '--regex',
                    '(tempest.api.object_storage)' +
                    '(?!.*test_list_containers_reverse_order.*)' +
                    '(?!.*test_list_container_contents_with_end_marker.*)' +
                    '(?!.*test_delete_non_empty_container.*)' +
                    '(?!.*test_container_synchronization.*)' +
                    '(?!.*test_get_object_after_expiration_time.*)'
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
              rgw keystone url: http://localhost:35357
              rgw keystone admin token: ADMIN
              rgw keystone accepted roles: admin,Member
              rgw keystone implicit tenants: true
              rgw keystone accepted admin roles: admin
              rgw swift enforce content length: true
              rgw swift account in url: true
              rgw swift versioning enabled: true
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
      - tempest:
          client.0:
            force-branch: master
            auth:
              admin_username: admin
              admin_project_name: admin
              admin_password: ADMIN
              admin_domain_name: Default
            identity:
              uri: http://127.0.0.1:5000/v2.0/
              uri_v3: http://127.0.0.1:5000/v3/
              admin_role: admin
            object-storage:
              reseller_admin_role: admin
            object-storage-feature-enabled:
              container_sync: false
              discoverability: false
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task tempest only supports a list or dictionary for configuration"
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
        teuthology.deep_merge(config[client], overrides.get('keystone', {}))

    log.debug('Tempest config is %s', config)

    with contextutil.nested(
        lambda: download(ctx=ctx, config=config),
        lambda: setup_venv(ctx=ctx, config=config),
        lambda: configure_instance(ctx=ctx, config=config),
        lambda: run_tempest(ctx=ctx, config=config),
        ):
        yield
