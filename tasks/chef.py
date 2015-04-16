"""
Chef-solo task
"""
import logging

from teuthology.orchestra import run
from teuthology import misc

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Run chef-solo on all nodes.

    Optional parameters:
    tasks:
    -chef
        script_url: # override default location for solo-from-scratch for Chef
        chef_repo: # override default Chef repo used by solo-from-scratch
        chef_branch: # to choose a different upstream branch for ceph-qa-chef
    """
    log.info('Running chef-solo...')

    if config is None:
        config = {}

    assert isinstance(config, dict), "chef - need config"
    chef_script = config.get('script_url', 'http://git.ceph.com/?p=ceph-qa-chef.git;a=blob_plain;f=solo/solo-from-scratch;hb=HEAD')
    chef_repo = config.get('chef_repo', "")
    chef_branch = config.get('chef_branch', "")
    run.wait(
        ctx.cluster.run(
            args=[
                'wget',
#                '-q',
                '-O-',
#                'https://raw.github.com/ceph/ceph-qa-chef/master/solo/solo-from-scratch',
                chef_script,
                run.Raw('|'),
                run.Raw('CHEF_REPO={repo}'.format(repo=chef_repo)),
                run.Raw('CHEF_BRANCH={branch}'.format(branch=chef_branch)),
                'sh',
                '-x',
                ],
            wait=False,
            )
        )

    log.info('Reconnecting after ceph-qa-chef run')
    misc.reconnect(ctx, 10)     #Reconnect for ulimit and other ceph-qa-chef changes

