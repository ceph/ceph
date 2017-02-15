"""
Execute ceph-deploy as a task
"""

import contextlib
import time
import logging

from teuthology import misc
from teuthology.orchestra.daemon import run
from tasks.set_repo import GA_BUILDS, set_cdn_repo
from teuthology.task.internal.redhat import _setup_latest_repo as setup_latest_repo

log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    """

    """
    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task ceph-deploy only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    misc.deep_merge(config, overrides.get('upgrades', {}))

    log.info('task upgrades with config ' + str(config))

    # first mon node is installer node
    (ceph_installer,) = ctx.cluster.only(
            misc.get_first_mon(ctx, config)).remotes.iterkeys()

    # check if we want to setup a cdn repo for upgrades
    rhbuild = config.get('rhbuild')
    if rhbuild in GA_BUILDS:
        set_cdn_repo(ctx, config)
    else:
        setup_latest_repo(ctx, config)

    # update ceph-ansible to current build under upgrade
    ceph_installer.run(args=[
        'sudo',
        'yum',
        'install',
        '-y',
        'ceph-ansible'])
    time.sleep(4)
    # Replace the old files at same location
    # debug print old playbook
    ceph_installer.run(args=[
            'cat',
            'ceph-ansible/rolling_update.yml',
            ])
    ceph_installer.run(args=[
        'cp',
        '-R',
        '/usr/share/ceph-ansible',
        '.'
        ])
    # Remove any .yml files from /usr/share/ceph-ansible for collision
    ceph_installer.run(args=[
            'sudo',
            'rm ',
            '-rf',
            run.Raw('/usr/share/ceph-ansible/*.yml')
            ])
    # debug print new playbook
    ceph_installer.run(args=[
            'cat',
            'ceph-ansible/rolling_update.yml',
            ])
    upg_cmd = ['cd', 'ceph-ansible', run.Raw(';'),
               'ansible-playbook', '-e', 'ireallymeanit=yes',
               '-vv', '-i', 'inven.yml', 'rolling_update.yml']

    ceph_installer.run(args=upg_cmd, timeout=4200)
    yield
