import logging

from ..orchestra import run
from .. import misc

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Run chef-solo on all nodes.
    """
    log.info('Running chef-solo...')

    run.wait(
        ctx.cluster.run(
            args=[
                'wget',
#                '-q',
                '-O-',
#                'https://raw.github.com/ceph/ceph-qa-chef/master/solo/solo-from-scratch',
                'http://ceph.com/git/?p=ceph-qa-chef.git;a=blob_plain;f=solo/solo-from-scratch;hb=HEAD',
                run.Raw('|'),
                'sh',
                '-x',
                ],
            wait=False,
            )
        )

    log.info('Reconnecting after ceph-qa-chef run')
    misc.reconnect(ctx, 10)     #Reconnect for ulimit and other ceph-qa-chef changes

