import contextlib
import logging
from teuthology import misc as teuthology
from teuthology.orchestra import run
from teuthology.parallel import parallel

log = logging.getLogger(__name__)

supported_repos = {'1.3.1': 'https://paste.fedoraproject.org/350766/14600017/raw/',
                   '1.3.2': 'http://paste.fedoraproject.org/354418/4224131/raw/',
                   }

rhel_7_rpms = ['rhel-7-server-rpms',
               'rhel-7-server-optional-rpms',
               'rhel-7-server-extras-rpms']

repos_13x = ['rhel-7-server-rhceph-1.3-mon-rpms',
             'rhel-7-server-rhceph-1.3-osd-rpms',
             'rhel-7-server-rhceph-1.3-calamari-rpms',
             'rhel-7-server-rhceph-1.3-installer-rpms',
             'rhel-7-server-rhceph-1.3-tools-rpms']

repos_20 = ['rhel-7-server-rhceph-2-mon-rpms',
             'rhel-7-server-rhceph-2-osd-rpms',
             'rhel-7-server-rhceph-2-tools-rpms',
             'rhel-7-server-rhscon-2-agent-rpms',
             'rhel-7-server-rhscon-2-installer-rpms',
             'rhel-7-server-rhscon-2-main-rpms']

repos_30 = [ 
             'rhel-7-server-rhceph-3-mon-rpms',
             'rhel-7-server-rhceph-3-osd-rpms',
             'rhel-7-server-rhceph-3-tools-rpms',
           ]

GA_BUILDS = ['1.3.2',
             '1.3.3',
             '2.0',
             '3.0']

@contextlib.contextmanager
def task(ctx, config):
    """
     Setup downstream repo's thats already released to customers
     rhbuild:
        1.3.1
     repo:
        2.0: 'repo_url'

    """
    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task set-repo only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('set-repo', {}))

    if config.get('repo'):
        log.info("Updating repo")
        repo = config.get('repo')
        supported_repos.update(repo)
    log.info("Backing up current repo's and disable firewall")
    for remote in ctx.cluster.remotes.iterkeys():
        if remote.os.package_type == 'rpm':
            remote.run(args=['mkdir', 'repo'], check_status=False)
            remote.run(args=['sudo', 'mv', run.Raw('/etc/yum.repos.d/*'), 'repo/'])
            remote.run(args=['sudo', 'yum', 'clean', 'metadata'])
            remote.run(args=['sudo', 'systemctl', 'stop', 'firewalld'], check_status=False)

    build = config.get('rhbuild')
    repo_url = supported_repos.get(build, None)
    log.info("Setting the repo for build %s", build)
    for remote in ctx.cluster.remotes.iterkeys():
        if remote.os.package_type == 'rpm':
            if build == '1.3.2' or build == '1.3.3':
                enable_cdn_repo(remote, repos_13x)
            elif build == '2.0':
                enable_cdn_repo(remote, repos_20)
            elif build == '3.0':
                enable_cdn_repo(remote, repos_30)
            else:
                remote.run(
                    args=[
                        'sudo',
                        'wget',
                        '-nv',
                        '-O',
                        '/etc/yum.repos.d/rh_ceph.repo',
                        repo_url])
                remote.run(args=['sudo', 'yum', 'clean', 'metadata'])

    try:
        yield
    finally:
        log.info("Resotring repo's")
        for remote in ctx.cluster.remotes.iterkeys():
            if remote.os.package_type == 'rpm':
                remote.run(args=['sudo', 'cp', run.Raw('repo/*'), '/etc/yum.repos.d/'])
                remote.run(args=['sudo', 'rm', '/etc/yum.repos.d/rh_ceph.repo'], check_status=False)
                remote.run(args=['sudo', 'yum', 'clean', 'metadata'])
                remote.run(args=['sudo', 'rm', '-rf', 'repo'])
                if build == '1.3.2':
                    disable_cdn_repo(remote, repos_13x)

def set_cdn_repo(ctx, config):
    build = config.get('rhbuild')
    with parallel() as p:
        for remote in ctx.cluster.remotes.iterkeys():
            if remote.os.package_type == 'rpm':
                    if build == '1.3.2' or build == '1.3.3':
                        p.spawn(enable_cdn_repo, remote, repos_13x)
                    elif build == '2.0':
                        p.spawn(enable_cdn_repo, remote, repos_20)
                    elif build == '3.0':
                        p.spawn(enable_cdn_repo, remote, repos_30)

def enable_cdn_repo(remote, repos):
    remote.run(args=['sudo', 'subscription-manager', 'repos', run.Raw('--disable=*')])
    remote.run(args=['sudo', 'rm', '-rf', run.Raw('/etc/yum.repos.d/*')],
               check_status=False)
    for repo in rhel_7_rpms:
        remote.run(args=['sudo', 'subscription-manager', 'repos', '--enable={r}'.format(r=repo)])
    for repo in repos:
        remote.run(args=['sudo', 'subscription-manager', 'repos', '--enable={r}'.format(r=repo)])
    remote.run(args=['sudo', 'subscription-manager', 'refresh'])
    remote.run(args=['sudo', 'yum', 'update', 'metadata'])


def disable_cdn_repo(remote, repos):
    for repo in repos:
        remote.run(args=['sudo', 'subscription-manager', 'repos', '--disable={r}'.format(r=repo)])
    remote.run(args=['sudo', 'subscription-manager', 'refresh'])


def set_repo_simple(remote, config):
    if config.get('latest'):
        build_repo = config.get('rhbuild-latest')
    else:
        build = config.get('build')
        build_repo = supported_repos[build]
    log.info("Setting the repo for build %s", build_repo)
    if remote.os.package_type == 'rpm':
        remote.run(args=['sudo', 'rm', run.Raw('/etc/yum.repos.d/*')])
        remote.run(
            args=[
                'sudo',
                'wget',
                '-nv',
                '-O',
                '/etc/yum.repos.d/rh_ceph.repo',
                build_repo])
        remote.run(args=['sudo', 'yum', 'clean', 'metadata'])
