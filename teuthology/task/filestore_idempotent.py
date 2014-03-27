"""
Filestore/filejournal handler
"""
import logging
from ..orchestra import run
import random

from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Test filestore/filejournal handling of non-idempotent events.

    Currently this is a kludge; we require the ceph task preceeds us just
    so that we get the tarball installed to run the test binary.

    :param ctx: Context
    :param config: Configuration
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

    testdir = teuthology.get_testdir(ctx)

    dir = '%s/data/test.%s' % (testdir, client)

    seed = str(int(random.uniform(1,100)))

    try:
        log.info('creating a working dir')
        remote.run(args=['mkdir', dir])
        remote.run(
            args=[
                'cd', dir,
                run.Raw('&&'),
                'wget','-q', '-Orun_seed_to.sh',
                'http://ceph.com/git/?p=ceph.git;a=blob_plain;f=src/test/objectstore/run_seed_to.sh;hb=HEAD',
                run.Raw('&&'),
                'wget','-q', '-Orun_seed_to_range.sh',
                'http://ceph.com/git/?p=ceph.git;a=blob_plain;f=src/test/objectstore/run_seed_to_range.sh;hb=HEAD',
                run.Raw('&&'),
                'chmod', '+x', 'run_seed_to.sh', 'run_seed_to_range.sh',
                ]);

        log.info('running a series of tests')
        proc = remote.run(
            args=[
                'cd', dir,
                run.Raw('&&'),
                './run_seed_to_range.sh', seed, '50', '300',
                ],
            wait=False,
            check_status=False)
        result = proc.exitstatus.get();
    
        if result != 0:
            remote.run(
                args=[
                    'cp', '-a', dir, '{tdir}/archive/idempotent_failure'.format(tdir=testdir),
                    ])
            raise Exception("./run_seed_to_range.sh errored out")

    finally:
        remote.run(args=[
                'rm', '-rf', '--', dir
                ])
        
