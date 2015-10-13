"""
Build ceph packages

Unit tests:

py.test -v -s tests/test_buildpackages.py

Integration tests:

teuthology-openstack --verbose --key-name myself --key-filename ~/Downloads/myself --ceph infernalis --suite teuthology/buildpackages

"""
import copy
import logging
import os
import re
import types
from subprocess import check_output, check_call
from teuthology import misc as teuthology
from teuthology.config import config as teuth_config
from teuthology.task import install
from teuthology.openstack import OpenStack
import urlparse

log = logging.getLogger(__name__)

def get_tag_branch_sha1(gitbuilder):
    """The install config may have contradicting tag/branch and sha1.
    When suite.py prepares the jobs, it always overrides the sha1 with
    whatever default is provided on the command line with --distro
    and what is found in the gitbuilder. If it turns out that the
    tag or the branch in the install config task is about another sha1,
    it will override anyway. For instance:

    install:
       tag: v0.94.1

    will be changed into

    install:
       tag: v0.94.1
       sha1: 12345

    even though v0.94.1 is not sha1 12345. This is does not cause
    problem with the install task because
    GitbuilderProject._get_uri_reference is used to figure out what to
    install from the gitbuilder and this function gives priority to
    the tag, if not found the branch, if not found the sha1.

    It is however confusing and this function returns a sha1 that is
    consistent with the tag or the branch being used.

    """

    uri_reference = gitbuilder.uri_reference
    url = gitbuilder.base_url
    assert '/' + uri_reference in url, \
        (url + ' (from template ' + teuth_config.baseurl_template +
         ') does not contain /' + uri_reference)
    log.info('uri_reference ' + uri_reference)
    if uri_reference.startswith('ref/'):
        ref = re.sub('^ref/', '', uri_reference) # do not use basename because the ref may contain a /
        ceph_git_url = teuth_config.get_ceph_git_url()
        cmd = "git ls-remote " + ceph_git_url + " " + ref
        output = check_output(cmd, shell=True)
        if not output:
            raise Exception(cmd + " returns nothing")
        lines = output.splitlines()
        if len(lines) != 1:
            raise Exception(
                cmd + " returns " + output +
                " which contains " + str(len(lines)) +
                " lines instead of exactly one")
        log.info(cmd + " returns " + lines[0])
        (sha1, ref) = lines[0].split()
        if ref.startswith('refs/heads/'):
            tag = None
            branch = re.sub('^refs/heads/', '', ref)
        elif ref.startswith('refs/tags/'):
            tag = re.sub('^refs/tags/', '', ref)
            branch = None
    else:
        sha1 = os.path.basename(uri_reference)
        tag = None
        branch = None
    return (tag, branch, sha1)

def get_config_install(ctx, config):
    if config is None:
        config = {}
    else:
        config = copy.deepcopy(config)

    assert isinstance(config, dict), \
        "task install only supports a dictionary for configuration"

    project, = config.get('project', 'ceph'),
    log.debug('project %s' % project)
    overrides = ctx.config.get('overrides')
    if overrides:
        install_overrides = overrides.get('install', {})
        teuthology.deep_merge(config, install_overrides.get(project, {}))
    log.debug('install config %s' % config)
    return [config]

def get_config_install_upgrade(ctx, config):
    config = copy.deepcopy(config)
    r = install.upgrade_remote_to_config(ctx, config).values()
    log.info("get_config_install_upgrade " + str(r))
    return r

GET_CONFIG_FUNCTIONS = {
    'install': get_config_install,
    'install.upgrade': get_config_install_upgrade,
}

def lookup_configs(ctx, node):
    configs = []
    if type(node) is types.ListType:
        for leaf in node:
            configs.extend(lookup_configs(ctx, leaf))
    elif type(node) is types.DictType:
        for (key, value) in node.iteritems():
            if key in ('install', 'install.upgrade'):
                configs.extend(GET_CONFIG_FUNCTIONS[key](ctx, value))
            elif key in ('overrides',):
                pass
            else:
                configs.extend(lookup_configs(ctx, value))
    return configs

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
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'task only accepts a dict for config not ' + str(config)
    d = os.path.join(os.path.dirname(__file__), 'buildpackages')
    for remote in ctx.cluster.remotes.iterkeys():
        for install_config in lookup_configs(ctx, ctx.config):
            gitbuilder = install._get_gitbuilder_project(
                ctx, remote, install_config)
            (tag, branch, sha1) = get_tag_branch_sha1(gitbuilder)
            check_call(
                "flock --close /tmp/buildpackages " +
                "make -C " + d + " " + os.environ['HOME'] + "/.ssh_agent",
                shell=True)
            url = gitbuilder.base_url
            target = os.path.dirname(urlparse.urlparse(url).path.strip('/'))
            target = os.path.dirname(target) + '-' + sha1
            openstack = OpenStack()
            if 'cloud.ovh.net' in os.environ['OS_AUTH_URL']:
                select = '^(vps|eg)-'
            else:
                select = ''
            build_flavor = openstack.flavor(config['machine'], select)
            http_flavor = openstack.flavor({
                'disk': 10, # GB
                'ram': 1024, # MB
                'cpus': 1,
            }, select)
            cmd = (". " + os.environ['HOME'] + "/.ssh_agent ; " +
                   " flock --close /tmp/buildpackages-" + sha1 +
                   " make -C " + d +
                   " CEPH_GIT_URL=" + teuth_config.get_ceph_git_url() +
                   " CEPH_PKG_TYPE=" + gitbuilder.pkg_type +
                   " CEPH_OS_TYPE=" + gitbuilder.os_type +
                   # os_version is from the remote and will be 7.1.23 for CentOS 7
                   # instead of the expected 7.0 for all 7.* CentOS
                   " CEPH_OS_VERSION=" + gitbuilder._get_version() +
                   " CEPH_DIST=" + gitbuilder.distro +
                   " CEPH_ARCH=" + gitbuilder.arch +
                   " CEPH_SHA1=" + sha1 +
                   " CEPH_TAG=" + (tag or '') +
                   " CEPH_BRANCH=" + (branch or '') +
                   " CEPH_FLAVOR=" + gitbuilder.flavor +
                   " GITBUILDER_URL=" + url +
                   " BUILD_FLAVOR=" + build_flavor +
                   " HTTP_FLAVOR=" + http_flavor +
                   " " + target +
                   " ")
            log.info("buildpackages: " + cmd)
            check_call(cmd, shell=True)
        teuth_config.gitbuilder_host = openstack.get_ip('packages-repository', '')
        log.info('Finished buildpackages')
