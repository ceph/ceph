import contextlib
import logging
import os

import vstart_runner

from teuthology import misc as teuthology
from teuthology import safepath
from teuthology.orchestra import run


class LocalRemote(vstart_runner.LocalRemote):
    def run(self, args, **kwargs):
        filtered = []
        i = 0
        while i < len(args):
            if args[i] == 'adjust-ulimits':
                i += 1
            elif args[i] == 'ceph-coverage':
                i += 2
            elif args[i] == 'timeout':
                i += 2
            else:
                filtered.append(args[i])
                i += 1
        return super(LocalRemote, self).run(filtered, **kwargs)


class LocalCluster(object):
    def __init__(self, rolename="placeholder"):
        self.remotes = {
            LocalRemote(): ['client.0']
        }

    def only(self, requested):
        return self.__class__(rolename=requested)

    def run(self, **kwargs):
        """
        Run a command on all the nodes in this cluster.

        Goes through nodes in alphabetical order.

        If you don't specify wait=False, this will be sequentially.

        Returns a list of `RemoteProcess`.
        """
        remotes = sorted(self.remotes.iterkeys(), key=lambda rem: rem.name)
        return [remote.run(**kwargs) for remote in remotes]


def setup_one_keyring(cluster_path, remote, role):
    client_name = teuthology.ceph_role(role)
    master_keyring_path = \
        '{cluster_path}/keyring'.format(cluster_path=cluster_path)
    remote.run(
        args=[
            'ceph',
            'auth',
            'get-or-create',
            client_name,
            'osd', 'allow rwx',
            'mon', 'allow rwx',
            run.Raw('&&'),
            'ceph',
            'auth',
            'export',
            '-o', master_keyring_path,
            ],
        )

def setup_keyrings(ctx, cluster_name, cluster_path):
    """
    Setup keyrings for all clients
    """
    clients = ctx.cluster.only(teuthology.is_type('client', cluster_name))
    testdir = teuthology.get_testdir(ctx)
    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)
    for remote, roles_for_host in clients.remotes.iteritems():
        for role in teuthology.cluster_roles_of_type(roles_for_host, 'client',
                                                     cluster_name):
            setup_one_keyring(cluster_path, remote, role)

@contextlib.contextmanager
def task(ctx, config):
    """
    local_cluster: replacement for initial stages of the Teuthology
    task's pipeline. Instead of deploying a new Ceph cluster, it
    exposes a local one.

    Example of config:
      tasks:
      - local_cluster:
          cluster_path: /home/rzarzynski/ceph-1/build
      - local_rgw:
         client.rgw
      - s3tests:
          client.rgw:
            force-branch: ceph-master
            scan_for_encryption_keys: false
      - radosgw-admin:

    Example of invocation:
      $ source ~/teuthology/virtualenv/bin/activate
      $ python ~/ceph-1/qa/tasks/local_runner.py  --verbose \
               --suite-path ~/ceph-1/qa /tmp/config.yaml

    Remarks:
      - RadosGW must be liseting on 8000,
      - For radosgw-admin you need to add to ceph.conf:
        rgw gc obj min wait = 15
        rgw_enable_usage_log = true
      - everything is a very, very early PoC.
    """
    # we need to update the PATH variable to let shell find binaries
    # of a cluster deployed with vstart.sh
    try:
      cluster_bin_dir = config['cluster_path'] + os.sep + 'bin'
    except TypeError:
        raise ValueError("the mandatory cluster_path is undefined")
    os.environ['PATH'] += os.pathsep + cluster_bin_dir

    # create the test hierarchy. It defaults to /home/ubuntu/cephtest,
    # be ready for EACCESS here
    safepath.makedirs('/', teuthology.get_testdir(ctx))

    # create the archive space. /home/ubuntu/cephtest/archihve by default
    safepath.makedirs('/', teuthology.get_archive_dir(ctx))

    ctx.cluster = LocalCluster()

    # let's provision credentials for clients
    setup_keyrings(ctx, 'ceph', config['cluster_path'])
    yield
