"""
Internal tasks  for redhat downstream builds
"""
import contextlib
import logging
import requests
import yaml
import os
from tempfile import NamedTemporaryFile
from teuthology.config import config as teuthconfig
from teuthology.parallel import parallel
from teuthology.orchestra import run
from teuthology.task.install.redhat import set_deb_repo
from teuthology.exceptions import CommandFailedError, ConfigError

log = logging.getLogger(__name__)


@contextlib.contextmanager
def setup_stage_cdn(ctx, config):
    """
    Configure internal stage cdn
    """
    suite_path = ctx.config.get('suite_path')
    if not suite_path:
        raise ConfigError("suite_path missing")
    teuthconfig.suite_path = suite_path

    with parallel() as p:
        for remote in ctx.cluster.remotes.keys():
            if remote.os.name == 'rhel':
                log.info("subscribing stage cdn on : %s", remote.shortname)
                p.spawn(_subscribe_stage_cdn, remote)
    try:
        yield
    finally:
        with parallel() as p:
            for remote in ctx.cluster.remotes.keys():
                p.spawn(_unsubscribe_stage_cdn, remote)


def _subscribe_stage_cdn(remote):
    _unsubscribe_stage_cdn(remote)
    cdn_config = teuthconfig.get('cdn-config', dict())
    server_url = cdn_config.get('server-url', 'subscription.rhsm.stage.redhat.com:443/subscription')
    base_url = cdn_config.get('base-url', 'https://cdn.stage.redhat.com')
    username = cdn_config.get('username', 'cephuser')
    password = cdn_config.get('password')
    remote.run(
        args=[
            'sudo', 'subscription-manager', '--force', 'register',
            run.Raw('--serverurl=' + server_url),
            run.Raw('--baseurl=' + base_url),
            run.Raw('--username=' + username),
            run.Raw('--password=' + password),
            '--auto-attach'
            ],
        timeout=720)
    _enable_rhel_repos(remote)


def _unsubscribe_stage_cdn(remote):
    try:
        remote.run(args=['sudo', 'subscription-manager', 'unregister'],
                   check_status=False)
    except CommandFailedError:
        # FIX ME
        log.info("unregistring subscription-manager failed, ignoring")


@contextlib.contextmanager
def setup_cdn_repo(ctx, config):
    """
     setup repo if set_cdn_repo exists in config
     redhat:
      set_cdn_repo:
        rhbuild: 2.0 or 1.3.2 or 1.3.3
    """
    # do import of tasks here since the qa task path should be set here
    if ctx.config.get('redhat').get('set-cdn-repo', None):
        from tasks.set_repo import set_cdn_repo
        config = ctx.config.get('redhat').get('set-cdn-repo')
        set_cdn_repo(ctx, config)
    yield


@contextlib.contextmanager
def setup_additional_repo(ctx, config):
    """
    set additional repo's for testing
    redhat:
      set-add-repo: 'http://example.com/internal.repo'
    """
    if ctx.config.get('redhat').get('set-add-repo', None):
        add_repo = ctx.config.get('redhat').get('set-add-repo')
        for remote in ctx.cluster.remotes.keys():
            if remote.os.package_type == 'rpm':
                remote.run(args=['sudo', 'wget', '-O', '/etc/yum.repos.d/rh_add.repo',
                                 add_repo])
                if not remote.os.version.startswith('8'):
                    remote.run(args=['sudo', 'yum', 'update', 'metadata'])

    yield


def _enable_rhel_repos(remote):

    # Look for rh specific repos in <suite_path>/rh/downstream.yaml
    ds_yaml = os.path.join(
        teuthconfig.suite_path,
        'rh',
        'downstream.yaml',
    )

    rhel_repos = yaml.safe_load(open(ds_yaml))
    repos_to_subscribe = rhel_repos.get('rhel_repos').get(remote.os.version[0])

    for repo in repos_to_subscribe:
        remote.run(args=['sudo', 'subscription-manager',
                         'repos', '--enable={r}'.format(r=repo)])


@contextlib.contextmanager
def setup_base_repo(ctx, config):
    """
    Setup repo based on redhat nodes
    redhat:
      base-repo-url:  base url that provides Mon, OSD, Tools etc
      installer-repo-url: Installer url that provides Agent, Installer
      deb-repo-url: debian repo url
      deb-gpg-key: gpg key used for signing the build
    """
    rh_config = ctx.config.get('redhat')
    if not rh_config.get('base-repo-url'):
        # no repo defined
        yield
    if rh_config.get('set-cdn-repo'):
        log.info("CDN repo already set, skipping rh repo")
        yield
    else:
        _setup_latest_repo(ctx, rh_config)
        try:
            yield
        finally:
            log.info("Cleaning up repo's")
            for remote in ctx.cluster.remotes.keys():
                if remote.os.package_type == 'rpm':
                    remote.run(args=['sudo', 'rm',
                                     run.Raw('/etc/yum.repos.d/rh*.repo'),
                                     ], check_status=False)


def _setup_latest_repo(ctx, config):
    """
    Setup repo based on redhat nodes
    """
    with parallel():
        for remote in ctx.cluster.remotes.keys():
            if remote.os.package_type == 'rpm':
                # pre-cleanup
                remote.run(args=['sudo', 'rm', run.Raw('/etc/yum.repos.d/rh*')],
                           check_status=False)
                remote.run(args=['sudo', 'yum', 'clean', 'metadata'])
                if not remote.os.version.startswith('8'):
                    remote.run(args=['sudo', 'yum', 'update', 'metadata'])
                # skip is required for beta iso testing
                if config.get('skip-subscription-manager', False) is True:
                    log.info("Skipping subscription-manager command")
                else:
                    remote.run(args=['sudo', 'subscription-manager', 'repos',
                                    run.Raw('--disable=*ceph*')],
                               check_status=False
                               )
                base_url = config.get('base-repo-url', '')
                installer_url = config.get('installer-repo-url', '')
                repos = ['MON', 'OSD', 'Tools', 'Calamari', 'Installer']
                installer_repos = ['Agent', 'Main', 'Installer']
                if config.get('base-rh-repos'):
                    repos = ctx.config.get('base-rh-repos')
                if config.get('installer-repos'):
                    installer_repos = ctx.config.get('installer-repos')
                # create base repo
                if base_url.startswith('http'):
                    repo_to_use = _get_repos_to_use(base_url, repos)
                    base_repo_file = NamedTemporaryFile(mode='w', delete=False)
                    _create_temp_repo_file(repo_to_use, base_repo_file)
                    remote.put_file(base_repo_file.name, base_repo_file.name)
                    remote.run(args=['sudo', 'cp', base_repo_file.name,
                                     '/etc/yum.repos.d/rh_ceph.repo'])
                    remote.run(args=['sudo', 'yum', 'clean', 'metadata'])
                if installer_url.startswith('http'):
                    irepo_to_use = _get_repos_to_use(
                        installer_url, installer_repos)
                    installer_file = NamedTemporaryFile(delete=False)
                    _create_temp_repo_file(irepo_to_use, installer_file)
                    remote.put_file(installer_file.name, installer_file.name)
                    remote.run(args=['sudo', 'cp', installer_file.name,
                                     '/etc/yum.repos.d/rh_inst.repo'])
                    remote.run(args=['sudo', 'yum', 'clean', 'metadata'])
                    if not remote.os.version.startswith('8'):
                        remote.run(args=['sudo', 'yum', 'update', 'metadata'])
            else:
                if config.get('deb-repo-url'):
                    deb_repo = config.get('deb-repo-url')
                    deb_gpg_key = config.get('deb-gpg-key', None)
                    set_deb_repo(remote, deb_repo, deb_gpg_key)


def _get_repos_to_use(base_url, repos):
    repod = dict()
    for repo in repos:
        repo_to_use = base_url + "compose/" + repo + "/x86_64/os/"
        r = requests.get(repo_to_use)
        log.info("Checking %s", repo_to_use)
        if r.status_code == 200:
            log.info("Using %s", repo_to_use)
            repod[repo] = repo_to_use
    return repod


def _create_temp_repo_file(repos, repo_file):
    for repo in repos.keys():
        header = "[ceph-" + repo + "]" + "\n"
        name = "name=ceph-" + repo + "\n"
        baseurl = "baseurl=" + repos[repo] + "\n"
        gpgcheck = "gpgcheck=0\n"
        enabled = "enabled=1\n\n"
        repo_file.write(header)
        repo_file.write(name)
        repo_file.write(baseurl)
        repo_file.write(gpgcheck)
        repo_file.write(enabled)
    repo_file.close()
