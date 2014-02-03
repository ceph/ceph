#!/usr/bin/env python
import contextlib
import logging

from ..orchestra import run
from teuthology import misc

"""
https://github.com/openstack-dev/devstack/blob/master/README.md
http://ceph.com/docs/master/rbd/rbd-openstack/
"""
log = logging.getLogger(__name__)

DEVSTACK_GIT_REPO = 'https://github.com/openstack-dev/devstack.git'
is_devstack_node = lambda role: role.startswith('devstack')
is_osd_node = lambda role: role.startswith('osd')


@contextlib.contextmanager
def task(ctx, config):
    """
    Install OpenStack DevStack and configure it to use a Ceph cluster for
    Glance and Cinder.

    Requires one node with a role 'devstack'
    """
    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise TypeError("config must be a dict")
    pool_size = config.get('pool_size', 128)

    # SETUP
    devstack_node = ctx.cluster.only(is_devstack_node).remotes[0]
    an_osd_node = ctx.cluster.only(is_osd_node).remotes[0]
    install_devstack(devstack_node)
    try:
        # OTHER STUFF

        # Create pools on Ceph cluster
        for pool_name in ['volumes', 'images', 'backups']:
            args = ['ceph', 'osd', 'pool', 'create', pool_name, pool_size]
            an_osd_node.run(args=args)

        # Copy ceph.conf to OpenStack node
        misc.copy_file(an_osd_node, '/etc/ceph/ceph.conf', devstack_node)
        # This is where we would install python-ceph and ceph-common but it
        # appears the ceph task will do that for us.
        # ceph auth get-or-create client.cinder mon 'allow r' osd 'allow class-read object_prefix rbd_children, allow rwx pool=volumes, allow rx pool=images'
        # ceph auth get-or-create client.glance mon 'allow r' osd 'allow class-read object_prefix rbd_children, allow rwx pool=images'
        # ceph auth get-or-create client.cinder-backup mon 'allow r' osd 'allow class-read object_prefix rbd_children, allow rwx pool=backups'

        yield
    #except Exception as e:
        # FAIL
        #pass
    finally:
        # CLEANUP
        pass


def install_devstack(devstack_node):
    log.info("Cloning devstack repo...")
    args = ['git', 'clone', DEVSTACK_GIT_REPO]
    devstack_node.run(args=args)

    log.info("Installing devstack...")
    args = ['cd', 'devstack', run.Raw('&&'), './stack.sh']
    devstack_node.run(args=args)
