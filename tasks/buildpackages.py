"""
Build ceph packages
"""
import contextlib
import logging
import os
import subprocess
from teuthology import install
from teuthology import misc as teuthology
from teuthology import contextutil, packaging
import urlparse

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Build Ceph packages. This task will automagically be run
    before the task that need to install packages (this is taken
    care of by the internal teuthology task).

    The config should be as follows:

    buildpackages:
      machine:
        disk: 40 # GB
        ram: 15000 # MB
        cpus: 16

    example:

    tasks:
    - buildpackages:
        machine:
          disk: 40 # GB
          ram: 15000 # MB
          cpus: 16
    - install:
    """
    log.info('Beginning buildpackages...')
    assert isinstance(config, dict), \
        'task only accepts a dict for config not ' + str(config)
    d = os.path.join(os.path.dirname(__file__), 'buildpackages')
    for remote in ctx.cluster.remotes.iterkeys():
        gitbuilder = install._get_gitbuilder_project(ctx, remote, config)
        tag = _get_config_value_for_remote(ctx, remote, config, 'tag')
        branch = _get_config_value_for_remote(ctx, remote, config, 'branch')
        sha1 = _get_config_value_for_remote(ctx, remote, config, 'sha1')
        uri_reference = gitbuilder.uri_reference
        url = gitbuilder.base_url
        assert '/' + uri_reference in url, \
            url + ' (from template ' + config.baseurl_template + ') does not contain /' + uri_reference
        if 'ref/' in uri_reference:
            ref = os.path.basename(uri_reference)
        else:
            ref = ''
        url = urlparse.urlparse(url)
        subprocess.check_call(
            "make -C " + d + " " + os.environ['HOME'] + "/.ssh-agent")
        subprocess.check_call(
            "source ~/.ssh-agent ; make -C " + d +
            " CEPH_OS_TYPE=" + gitbuilder.os_type +
            " CEPH_OS_VERSION=" + gitbuilder.os_version +
            " CEPH_DIST=" + gitbuilder.distro +
            " CEPH_ARCH=" + gitbuilder.arch +
            " CEPH_SHA1=" + (sha1 or '')  +
            " CEPH_TAG=" + (tag or '') +
            " CEPH_BRANCH=" + (branch or '') +
            " CEPH_REF=" + ref +
            " GITBUILDER_URL=" + url +
            " packages-" + url.path.strip('/') +
            " ", shell=True)
    log.info('Finished buildpackages')
