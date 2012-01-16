import logging

from ..orchestra import run

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
                '-q',
                '-O-',
#                'https://raw.github.com/NewDreamNetwork/ceph-qa-chef/master/solo/solo-from-scratch',
                'http://ceph.newdream.net/git/?p=ceph-qa-chef.git;a=blob_plain;f=solo/solo-from-scratch;hb=HEAD',
                run.Raw('|'),
                'sh',
                ],
            wait=False,
            )
        )
