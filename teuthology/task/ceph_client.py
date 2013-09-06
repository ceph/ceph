import logging

from teuthology import misc as teuthology
from ..orchestra import run

log = logging.getLogger(__name__)

def create_keyring(ctx):
    log.info('Setting up client nodes...')
    clients = ctx.cluster.only(teuthology.is_type('client'))
    testdir = teuthology.get_testdir(ctx)
    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)
    for remote, roles_for_host in clients.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, 'client'):
            client_keyring = '/etc/ceph/ceph.client.{id}.keyring'.format(id=id_)
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
                    '--name=client.{id}'.format(id=id_),
                    client_keyring,
                    run.Raw('&&'),
                    'sudo',
                    'chmod',
                    '0644',
                    client_keyring,
                    ],
                )
