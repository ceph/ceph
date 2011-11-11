from cStringIO import StringIO
import logging

from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Test filestore/filejournal handling of non-idempotent events.

    Currently this is a kludge; we require the ceph task preceeds us just
    so that we get the tarball installed to run the test binary.
    """
    assert config is None or isinstance(config, list) \
        or isinstance(config, dict), \
        "task only supports a list or dictionary for configuration"
    all_clients = ['client.{id}'.format(id=id_)
                   for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')]
    if config is None:
        config = all_clients
    if isinstance(config, list):
        config = dict.fromkeys(config)
    clients = config.keys()

    # just use the first client...
    client = clients[0];
    (remote,) = ctx.cluster.only(client).remotes.iterkeys()

    dir = '/tmp/cephtest/data/test.%s' % client
    journal = '/tmp/cephtest/data/test.journal.%s' % client

    remote.run(args=['mkdir', dir])
    remote.run(args=['dd', 'if=/dev/zero', 'of=%s' % journal, 'bs=1M',
                     'count=100'])

    log.info('writing some data and simulating a failure')
    remote.run(args=[
            '/tmp/cephtest/binary/usr/local/bin/test_filestore_idempotent',
            '-c', '/tmp/cephtest/ceph.conf',
            'write', dir, journal
            ])

    log.info('verifying journal replay gives the correct result')
    remote.run(args=[
            '/tmp/cephtest/binary/usr/local/bin/test_filestore_idempotent',
            '-c', '/tmp/cephtest/ceph.conf',
            'verify', dir, journal
            ])

