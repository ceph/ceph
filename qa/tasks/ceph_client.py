"""
Set up client keyring
"""
import logging

from teuthology import misc as teuthology
from teuthology.orchestra import run

log = logging.getLogger(__name__)

def create_keyring(ctx, cluster_name):
    """
    Set up key ring on remote sites
    """
    log.info('Setting up client nodes...')
    clients = ctx.cluster.only(teuthology.is_type('client', cluster_name))
    testdir = teuthology.get_testdir(ctx)
    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)
    for remote, roles_for_host in clients.remotes.items():
        for role in teuthology.cluster_roles_of_type(roles_for_host, 'client',
                                                     cluster_name):
            name = teuthology.ceph_role(role)
            client_keyring = '/etc/ceph/{0}.{1}.keyring'.format(cluster_name, name)
            remote.run(
                args=[
                    'sudo',
                    'adjust-ulimits',
                    'ceph-coverage',
                    coverage_dir,
                    'ceph-authtool',
                    '--create-keyring',
                    '--gen-key',
                    # TODO this --name= is not really obeyed, all unknown "types" are munged to "client"
                    '--name={name}'.format(name=name),
                    client_keyring,
                    run.Raw('&&'),
                    'sudo',
                    'chmod',
                    '0644',
                    client_keyring,
                    ],
                )
