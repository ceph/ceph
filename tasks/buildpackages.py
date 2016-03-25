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
import types
from teuthology import packaging
from teuthology import misc
from teuthology.config import config as teuth_config
from teuthology.openstack import OpenStack

log = logging.getLogger(__name__)

class LocalGitbuilderProject(packaging.GitbuilderProject):

    def __init__(self):
        pass


def get_pkg_type(os_type):
    if os_type in ('centos', 'fedora', 'opensuse', 'rhel', 'sles'):
        return 'rpm'
    else:
        return 'deb'

def apply_overrides(ctx, config):
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
        misc.deep_merge(config, install_overrides.get(project, {}))
    return config

def get_config_install(ctx, config):
    config = apply_overrides(ctx, config)
    log.debug('install config %s' % config)
    return [(config.get('flavor', 'basic'),
             config.get('tag', ''),
             config.get('branch', ''),
             config.get('sha1'))]

def get_config_install_upgrade(ctx, config):
    log.debug('install.upgrade config before override %s' % config)
    configs = []
    for (role, role_config) in config.iteritems():
        if role_config is None:
            role_config = {}
        o = apply_overrides(ctx, role_config)

        log.debug('install.upgrade config ' + str(role_config) +
                  ' and with overrides ' + str(o))
        # for install.upgrade overrides are actually defaults
        configs.append((o.get('flavor', 'basic'),
                        role_config.get('tag', o.get('tag', '')),
                        role_config.get('branch', o.get('branch', '')),
                        role_config.get('sha1', o.get('sha1'))))
    return configs

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

def get_sha1(ref):
    url = teuth_config.get_ceph_git_url()
    ls_remote = misc.sh("git ls-remote " + url + " " + ref)
    return ls_remote.split()[0]

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

    When a buildpackages task is already included, the values it contains can be
    overriden with:

    overrides:
      buildpackages:
        machine:
          disk: 10 # GB
          ram: 1000 # MB
          cpus: 1

    """
    log.info('Beginning buildpackages...')
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'task only accepts a dict for config not ' + str(config)
    overrides = ctx.config.get('overrides', {})
    misc.deep_merge(config, overrides.get('buildpackages', {}))
    d = os.path.join(os.path.dirname(__file__), 'buildpackages')
    os_type = misc.get_distro(ctx)
    os_version = misc.get_distro_version(ctx)
    arch = ctx.config.get('arch', OpenStack().get_default_arch())
    dist = LocalGitbuilderProject()._get_distro(distro=os_type,
                                                version=os_version)
    pkg_type = get_pkg_type(os_type)
    misc.sh(
        "flock --close /tmp/buildpackages " +
        "make -C " + d + " " + os.environ['HOME'] + "/.ssh_agent")
    for (flavor, tag, branch, sha1) in lookup_configs(ctx, ctx.config):
        if tag:
            sha1 = get_sha1(tag)
        elif branch:
            sha1 = get_sha1(branch)
        log.info("building flavor = " + flavor + "," +
                 " tag = " + tag + "," +
                 " branch = " + branch + "," +
                 " sha1 = " + sha1)
        target = ('ceph-' +
                  pkg_type + '-' +
                  dist + '-' +
                  arch + '-' +
                  flavor + '-' +
                  sha1)
        openstack = OpenStack()
        openstack.set_provider()
        if openstack.provider == 'ovh':
            select = '^(vps|eg)-'
        else:
            select = ''
        openstack.image(os_type, os_version) # create if it does not exist
        build_flavor = openstack.flavor(config['machine'], select)
        http_flavor = openstack.flavor({
            'disk': 40, # GB
            'ram': 1024, # MB
            'cpus': 1,
        }, select)
        lock = "/tmp/buildpackages-" + sha1 + "-" + os_type + "-" + os_version
        cmd = (". " + os.environ['HOME'] + "/.ssh_agent ; " +
               " flock --close " + lock +
               " make -C " + d +
               " CEPH_GIT_URL=" + teuth_config.get_ceph_git_url() +
               " CEPH_PKG_TYPE=" + pkg_type +
               " CEPH_OS_TYPE=" + os_type +
               " CEPH_OS_VERSION=" + os_version +
               " CEPH_DIST=" + dist +
               " CEPH_ARCH=" + arch +
               " CEPH_SHA1=" + sha1 +
               " CEPH_TAG=" + tag +
               " CEPH_BRANCH=" + branch +
               " CEPH_FLAVOR=" + flavor +
               " BUILD_FLAVOR=" + build_flavor +
               " HTTP_FLAVOR=" + http_flavor +
               " " + target +
               " ")
        log.info("buildpackages: " + cmd)
        misc.sh(cmd)
    teuth_config.gitbuilder_host = openstack.get_ip('packages-repository', '')
    log.info('Finished buildpackages')
