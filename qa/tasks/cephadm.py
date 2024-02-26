"""
Ceph cluster task, deployed via cephadm orchestrator
"""
import argparse
import configobj
import contextlib
import functools
import json
import logging
import os
import re
import time
import uuid
import yaml

import jinja2

from copy import deepcopy
from io import BytesIO, StringIO
from tarfile import ReadError
from tasks.ceph_manager import CephManager
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology import packaging
from teuthology.orchestra import run
from teuthology.orchestra.daemon import DaemonGroup
from teuthology.config import config as teuth_config
from teuthology.exceptions import ConfigError, CommandFailedError
from textwrap import dedent
from tasks.cephfs.filesystem import MDSCluster, Filesystem
from tasks.util import chacra

# these items we use from ceph.py should probably eventually move elsewhere
from tasks.ceph import get_mons, healthy

CEPH_ROLE_TYPES = ['mon', 'mgr', 'osd', 'mds', 'rgw', 'prometheus']

log = logging.getLogger(__name__)


def _convert_strs_in(o, conv):
    """A function to walk the contents of a dict/list and recurisvely apply
    a conversion function (`conv`) to the strings within.
    """
    if isinstance(o, str):
        return conv(o)
    if isinstance(o, dict):
        for k in o:
            o[k] = _convert_strs_in(o[k], conv)
    if isinstance(o, list):
        o[:] = [_convert_strs_in(v, conv) for v in o]
    return o


def _apply_template(jinja_env, rctx, template):
    """Apply jinja2 templating to the template string `template` via the jinja
    environment `jinja_env`, passing a dictionary containing top-level context
    to render into the template.
    """
    if '{{' in template or '{%' in template:
        return jinja_env.from_string(template).render(**rctx)
    return template


def _template_transform(ctx, config, target):
    """Apply jinja2 based templates to strings within the target object,
    returning a transformed target. Target objects may be a list or dict or
    str.

    Note that only string values in the list or dict objects are modified.
    Therefore one can read & parse yaml or json that contain templates in
    string values without the risk of changing the structure of the yaml/json.
    """
    jenv = getattr(ctx, '_jinja_env', None)
    if jenv is None:
        loader = jinja2.BaseLoader()
        jenv = jinja2.Environment(loader=loader)
        jenv.filters['role_to_remote'] = _role_to_remote
        setattr(ctx, '_jinja_env', jenv)
    rctx = dict(ctx=ctx, config=config, cluster_name=config.get('cluster', ''))
    _vip_vars(rctx)
    conv = functools.partial(_apply_template, jenv, rctx)
    return _convert_strs_in(target, conv)


def _vip_vars(rctx):
    """For backwards compat with the previous subst_vip function."""
    ctx = rctx['ctx']
    if 'vnet' in getattr(ctx, 'vip', {}):
        rctx['VIPPREFIXLEN'] = str(ctx.vip["vnet"].prefixlen)
        rctx['VIPSUBNET'] = str(ctx.vip["vnet"].network_address)
    if 'vips' in getattr(ctx, 'vip', {}):
        vips = ctx.vip['vips']
        for idx, vip in enumerate(vips):
            rctx[f'VIP{idx}'] = str(vip)


@jinja2.pass_context
def _role_to_remote(rctx, role):
    """Return the first remote matching the given role."""
    ctx = rctx['ctx']
    for remote, roles in ctx.cluster.remotes.items():
        if role in roles:
            return remote
    return None


def _shell(ctx, cluster_name, remote, args, extra_cephadm_args=[], **kwargs):
    teuthology.get_testdir(ctx)
    return remote.run(
        args=[
            'sudo',
            ctx.cephadm,
            '--image', ctx.ceph[cluster_name].image,
            'shell',
            '-c', '/etc/ceph/{}.conf'.format(cluster_name),
            '-k', '/etc/ceph/{}.client.admin.keyring'.format(cluster_name),
            '--fsid', ctx.ceph[cluster_name].fsid,
            ] + extra_cephadm_args + [
            '--',
            ] + args,
        **kwargs
    )


def _cephadm_remotes(ctx, log_excluded=False):
    for remote, roles in ctx.cluster.remotes.items():
        if any(r.startswith('cephadm.exclude') for r in roles):
            if log_excluded:
                log.info(
                    f'Remote {remote.shortname} excluded from cephadm cluster by role'
                )
            continue
        yield remote, roles


def build_initial_config(ctx, config):
    cluster_name = config['cluster']

    path = os.path.join(os.path.dirname(__file__), 'cephadm.conf')
    conf = configobj.ConfigObj(path, file_error=True)

    conf.setdefault('global', {})
    conf['global']['fsid'] = ctx.ceph[cluster_name].fsid

    # overrides
    for section, keys in config.get('conf',{}).items():
        for key, value in keys.items():
            log.info(" override: [%s] %s = %s" % (section, key, value))
            if section not in conf:
                conf[section] = {}
            conf[section][key] = value

    return conf


def distribute_iscsi_gateway_cfg(ctx, conf_data):
    """
    Distribute common gateway config to get the IPs.
    These will help in iscsi clients with finding trusted_ip_list.
    """
    log.info('Distributing iscsi-gateway.cfg...')
    for remote, roles in _cephadm_remotes(ctx):
        remote.write_file(
            path='/etc/ceph/iscsi-gateway.cfg',
            data=conf_data,
            sudo=True)

def update_archive_setting(ctx, key, value):
    """
    Add logs directory to job's info log file
    """
    if ctx.archive is None:
        return
    with open(os.path.join(ctx.archive, 'info.yaml'), 'r+') as info_file:
        info_yaml = yaml.safe_load(info_file)
        info_file.seek(0)
        if 'archive' in info_yaml:
            info_yaml['archive'][key] = value
        else:
            info_yaml['archive'] = {key: value}
        yaml.safe_dump(info_yaml, info_file, default_flow_style=False)


@contextlib.contextmanager
def normalize_hostnames(ctx):
    """
    Ensure we have short hostnames throughout, for consistency between
    remote.shortname and socket.gethostname() in cephadm.
    """
    log.info('Normalizing hostnames...')
    cluster = ctx.cluster.filter(lambda r: '.' in r.hostname)
    cluster.run(args=[
        'sudo',
        'hostname',
        run.Raw('$(hostname -s)'),
    ])

    try:
        yield
    finally:
        pass


@contextlib.contextmanager
def download_cephadm(ctx, config, ref):
    cluster_name = config['cluster']

    if config.get('cephadm_mode') != 'cephadm-package':
        if ctx.config.get('redhat'):
            _fetch_cephadm_from_rpm(ctx)
        # TODO: come up with a sensible way to detect if we need an "old, uncompiled"
        # cephadm
        elif 'cephadm_git_url' in config and 'cephadm_branch' in config:
            _fetch_cephadm_from_github(ctx, config, ref)
        elif 'compiled_cephadm_branch' in config:
            _fetch_stable_branch_cephadm_from_chacra(ctx, config, cluster_name)
        else:
            _fetch_cephadm_from_chachra(ctx, config, cluster_name)

    try:
        yield
    finally:
        _rm_cluster(ctx, cluster_name)
        if config.get('cephadm_mode') == 'root':
            _rm_cephadm(ctx)


def _fetch_cephadm_from_rpm(ctx):
    log.info("Copying cephadm installed from an RPM package")
    # cephadm already installed from redhat.install task
    ctx.cluster.run(
        args=[
            'cp',
            run.Raw('$(which cephadm)'),
            ctx.cephadm,
            run.Raw('&&'),
            'ls', '-l',
            ctx.cephadm,
        ]
    )


def _fetch_cephadm_from_github(ctx, config, ref):
    ref = config.get('cephadm_branch', ref)
    git_url = config.get('cephadm_git_url', teuth_config.get_ceph_git_url())
    log.info('Downloading cephadm (repo %s ref %s)...' % (git_url, ref))
    if git_url.startswith('https://github.com/'):
        # git archive doesn't like https:// URLs, which we use with github.
        rest = git_url.split('https://github.com/', 1)[1]
        rest = re.sub(r'\.git/?$', '', rest).strip() # no .git suffix
        ctx.cluster.run(
            args=[
                'curl', '--silent',
                'https://raw.githubusercontent.com/' + rest + '/' + ref + '/src/cephadm/cephadm',
                run.Raw('>'),
                ctx.cephadm,
                run.Raw('&&'),
                'ls', '-l',
                ctx.cephadm,
            ],
        )
    else:
        ctx.cluster.run(
            args=[
                'git', 'clone', git_url, 'testrepo',
                run.Raw('&&'),
                'cd', 'testrepo',
                run.Raw('&&'),
                'git', 'show', f'{ref}:src/cephadm/cephadm',
                run.Raw('>'),
                ctx.cephadm,
                run.Raw('&&'),
                'ls', '-l', ctx.cephadm,
            ],
        )
    # sanity-check the resulting file and set executable bit
    cephadm_file_size = '$(stat -c%s {})'.format(ctx.cephadm)
    ctx.cluster.run(
        args=[
            'test', '-s', ctx.cephadm,
            run.Raw('&&'),
            'test', run.Raw(cephadm_file_size), "-gt", run.Raw('1000'),
            run.Raw('&&'),
            'chmod', '+x', ctx.cephadm,
        ],
    )


def _fetch_cephadm_from_chachra(ctx, config, cluster_name):
    log.info('Downloading "compiled" cephadm from cachra')
    bootstrap_remote = ctx.ceph[cluster_name].bootstrap_remote
    bp = packaging.get_builder_project()(
        config.get('project', 'ceph'),
        config,
        ctx=ctx,
        remote=bootstrap_remote,
    )
    log.info('builder_project result: %s' % (bp._result.json()))

    flavor = config.get('flavor', 'default')
    branch = config.get('branch')
    sha1 = config.get('sha1')

    # pull the cephadm binary from chacra
    url = chacra.get_binary_url(
            'cephadm',
            project=bp.project,
            distro=bp.distro.split('/')[0],
            release=bp.distro.split('/')[1],
            arch=bp.arch,
            flavor=flavor,
            branch=branch,
            sha1=sha1,
    )
    log.info("Discovered cachra url: %s", url)
    ctx.cluster.run(
        args=[
            'curl', '--silent', '-L', url,
            run.Raw('>'),
            ctx.cephadm,
            run.Raw('&&'),
            'ls', '-l',
            ctx.cephadm,
        ],
    )

    # sanity-check the resulting file and set executable bit
    cephadm_file_size = '$(stat -c%s {})'.format(ctx.cephadm)
    ctx.cluster.run(
        args=[
            'test', '-s', ctx.cephadm,
            run.Raw('&&'),
            'test', run.Raw(cephadm_file_size), "-gt", run.Raw('1000'),
            run.Raw('&&'),
            'chmod', '+x', ctx.cephadm,
        ],
    )

def _fetch_stable_branch_cephadm_from_chacra(ctx, config, cluster_name):
    branch = config.get('compiled_cephadm_branch', 'reef')
    flavor = config.get('flavor', 'default')

    log.info(f'Downloading "compiled" cephadm from cachra for {branch}')

    bootstrap_remote = ctx.ceph[cluster_name].bootstrap_remote
    bp = packaging.get_builder_project()(
        config.get('project', 'ceph'),
        config,
        ctx=ctx,
        remote=bootstrap_remote,
    )
    log.info('builder_project result: %s' % (bp._result.json()))

    # pull the cephadm binary from chacra
    url = chacra.get_binary_url(
            'cephadm',
            project=bp.project,
            distro=bp.distro.split('/')[0],
            release=bp.distro.split('/')[1],
            arch=bp.arch,
            flavor=flavor,
            branch=branch,
    )
    log.info("Discovered cachra url: %s", url)
    ctx.cluster.run(
        args=[
            'curl', '--silent', '-L', url,
            run.Raw('>'),
            ctx.cephadm,
            run.Raw('&&'),
            'ls', '-l',
            ctx.cephadm,
        ],
    )

    # sanity-check the resulting file and set executable bit
    cephadm_file_size = '$(stat -c%s {})'.format(ctx.cephadm)
    ctx.cluster.run(
        args=[
            'test', '-s', ctx.cephadm,
            run.Raw('&&'),
            'test', run.Raw(cephadm_file_size), "-gt", run.Raw('1000'),
            run.Raw('&&'),
            'chmod', '+x', ctx.cephadm,
        ],
    )


def _rm_cluster(ctx, cluster_name):
    log.info('Removing cluster...')
    for remote, _ in _cephadm_remotes(ctx):
        remote.run(args=[
            'sudo',
            ctx.cephadm,
            'rm-cluster',
            '--fsid', ctx.ceph[cluster_name].fsid,
            '--force',
        ])


def _rm_cephadm(ctx):
    log.info('Removing cephadm ...')
    ctx.cluster.run(
        args=[
            'rm',
            '-rf',
            ctx.cephadm,
        ],
    )


@contextlib.contextmanager
def ceph_log(ctx, config):
    cluster_name = config['cluster']
    fsid = ctx.ceph[cluster_name].fsid

    update_archive_setting(ctx, 'log', '/var/log/ceph')


    try:
        yield

    except Exception:
        # we need to know this below
        ctx.summary['success'] = False
        raise

    finally:
        log.info('Checking cluster log for badness...')
        def first_in_ceph_log(pattern, excludes):
            """
            Find the first occurrence of the pattern specified in the Ceph log,
            Returns None if none found.

            :param pattern: Pattern scanned for.
            :param excludes: Patterns to ignore.
            :return: First line of text (or None if not found)
            """
            args = [
                'sudo',
                'egrep', pattern,
                '/var/log/ceph/{fsid}/ceph.log'.format(
                    fsid=fsid),
            ]
            if excludes:
                for exclude in excludes:
                    args.extend([run.Raw('|'), 'egrep', '-v', exclude])
            args.extend([
                run.Raw('|'), 'head', '-n', '1',
            ])
            r = ctx.ceph[cluster_name].bootstrap_remote.run(
                stdout=StringIO(),
                args=args,
            )
            stdout = r.stdout.getvalue()
            if stdout != '':
                return stdout
            return None

        if first_in_ceph_log('\[ERR\]|\[WRN\]|\[SEC\]',
                             config.get('log-ignorelist')) is not None:
            log.warning('Found errors (ERR|WRN|SEC) in cluster log')
            ctx.summary['success'] = False
            # use the most severe problem as the failure reason
            if 'failure_reason' not in ctx.summary:
                for pattern in ['\[SEC\]', '\[ERR\]', '\[WRN\]']:
                    match = first_in_ceph_log(pattern, config['log-ignorelist'])
                    if match is not None:
                        ctx.summary['failure_reason'] = \
                            '"{match}" in cluster log'.format(
                                match=match.rstrip('\n'),
                            )
                        break

        if ctx.archive is not None and \
                not (ctx.config.get('archive-on-error') and ctx.summary['success']):
            # and logs
            log.info('Compressing logs...')
            run.wait(
                ctx.cluster.run(
                    args=[
                        'time',
                        'sudo',
                        'find',
                        '/var/log/ceph',   # all logs, not just for the cluster
                        '/var/log/rbd-target-api', # ceph-iscsi
                        '-name',
                        '*.log',
                        '-print0',
                        run.Raw('|'),
                        'sudo',
                        'xargs',
                        '--max-args=1',
                        '--max-procs=0',
                        '--verbose',
                        '-0',
                        '--no-run-if-empty',
                        '--',
                        'gzip',
                        '-5',
                        '--verbose',
                        '--',
                    ],
                    wait=False,
                ),
            )

            log.info('Archiving logs...')
            path = os.path.join(ctx.archive, 'remote')
            try:
                os.makedirs(path)
            except OSError:
                pass
            for remote in ctx.cluster.remotes.keys():
                sub = os.path.join(path, remote.shortname)
                try:
                    os.makedirs(sub)
                except OSError:
                    pass
                try:
                    teuthology.pull_directory(remote, '/var/log/ceph',  # everything
                                              os.path.join(sub, 'log'))
                except ReadError:
                    pass


@contextlib.contextmanager
def ceph_crash(ctx, config):
    """
    Gather crash dumps from /var/lib/ceph/$fsid/crash
    """
    cluster_name = config['cluster']
    fsid = ctx.ceph[cluster_name].fsid

    update_archive_setting(ctx, 'crash', '/var/lib/ceph/crash')

    try:
        yield

    finally:
        if ctx.archive is not None:
            log.info('Archiving crash dumps...')
            path = os.path.join(ctx.archive, 'remote')
            try:
                os.makedirs(path)
            except OSError:
                pass
            for remote in ctx.cluster.remotes.keys():
                sub = os.path.join(path, remote.shortname)
                try:
                    os.makedirs(sub)
                except OSError:
                    pass
                try:
                    teuthology.pull_directory(remote,
                                              '/var/lib/ceph/%s/crash' % fsid,
                                              os.path.join(sub, 'crash'))
                except ReadError:
                    pass


@contextlib.contextmanager
def pull_image(ctx, config):
    cluster_name = config['cluster']
    log.info(f'Pulling image {ctx.ceph[cluster_name].image} on all hosts...')
    run.wait(
        ctx.cluster.run(
            args=[
                'sudo',
                ctx.cephadm,
                '--image', ctx.ceph[cluster_name].image,
                'pull',
            ],
            wait=False,
        )
    )

    try:
        yield
    finally:
        pass

@contextlib.contextmanager
def setup_ca_signed_keys(ctx, config):
    # generate our ca key
    cluster_name = config['cluster']
    bootstrap_remote = ctx.ceph[cluster_name].bootstrap_remote
    bootstrap_remote.run(args=[
        'sudo', 'ssh-keygen', '-t', 'rsa', '-f', '/root/ca-key', '-N', ''
    ])

    # not using read_file here because it runs dd as a non-root
    # user and would hit permission issues
    r = bootstrap_remote.run(args=[
        'sudo', 'cat', '/root/ca-key.pub'
    ], stdout=StringIO())
    ca_key_pub_contents = r.stdout.getvalue()

    # make CA key accepted on each host
    for remote in ctx.cluster.remotes.keys():
        # write key to each host's /etc/ssh dir
        remote.run(args=[
            'sudo', 'echo', ca_key_pub_contents,
            run.Raw('|'),
            'sudo', 'tee', '-a', '/etc/ssh/ca-key.pub',
        ])
        # make sshd accept the CA signed key
        remote.run(args=[
            'sudo', 'echo', 'TrustedUserCAKeys /etc/ssh/ca-key.pub',
            run.Raw('|'),
            'sudo', 'tee', '-a', '/etc/ssh/sshd_config',
            run.Raw('&&'),
            'sudo', 'systemctl', 'restart', 'sshd',
        ])

    # generate a new key pair and sign the pub key to make a cert
    bootstrap_remote.run(args=[
        'sudo', 'ssh-keygen', '-t', 'rsa', '-f', '/root/cephadm-ssh-key', '-N', '',
        run.Raw('&&'),
        'sudo', 'ssh-keygen', '-s', '/root/ca-key', '-I', 'user_root', '-n', 'root', '-V', '+52w', '/root/cephadm-ssh-key',
    ])

    # for debugging, to make sure this setup has worked as intended
    for remote in ctx.cluster.remotes.keys():
        remote.run(args=[
            'sudo', 'cat', '/etc/ssh/ca-key.pub'
        ])
        remote.run(args=[
            'sudo', 'cat', '/etc/ssh/sshd_config',
            run.Raw('|'),
            'grep', 'TrustedUserCAKeys'
        ])
    bootstrap_remote.run(args=[
        'sudo', 'ls', '/root/'
    ])

    ctx.ca_signed_key_info = {}
    ctx.ca_signed_key_info['ca-key'] = '/root/ca-key'
    ctx.ca_signed_key_info['ca-key-pub'] = '/root/ca-key.pub'
    ctx.ca_signed_key_info['private-key'] = '/root/cephadm-ssh-key'
    ctx.ca_signed_key_info['ca-signed-cert'] = '/root/cephadm-ssh-key-cert.pub'

    try:
        yield
    finally:
        pass

@contextlib.contextmanager
def ceph_bootstrap(ctx, config):
    """
    Bootstrap ceph cluster.

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """
    cluster_name = config['cluster']
    testdir = teuthology.get_testdir(ctx)
    fsid = ctx.ceph[cluster_name].fsid

    bootstrap_remote = ctx.ceph[cluster_name].bootstrap_remote
    first_mon = ctx.ceph[cluster_name].first_mon
    first_mon_role = ctx.ceph[cluster_name].first_mon_role
    mons = ctx.ceph[cluster_name].mons

    ctx.cluster.run(args=[
        'sudo', 'mkdir', '-p', '/etc/ceph',
        ]);
    ctx.cluster.run(args=[
        'sudo', 'chmod', '777', '/etc/ceph',
        ]);
    try:
        # write seed config
        log.info('Writing seed config...')
        conf_fp = BytesIO()
        seed_config = build_initial_config(ctx, config)
        seed_config.write(conf_fp)
        bootstrap_remote.write_file(
            path='{}/seed.{}.conf'.format(testdir, cluster_name),
            data=conf_fp.getvalue())
        log.debug('Final config:\n' + conf_fp.getvalue().decode())
        ctx.ceph[cluster_name].conf = seed_config

        # register initial daemons
        ctx.daemons.register_daemon(
            bootstrap_remote, 'mon', first_mon,
            cluster=cluster_name,
            fsid=fsid,
            logger=log.getChild('mon.' + first_mon),
            wait=False,
            started=True,
        )
        if not ctx.ceph[cluster_name].roleless:
            first_mgr = ctx.ceph[cluster_name].first_mgr
            ctx.daemons.register_daemon(
                bootstrap_remote, 'mgr', first_mgr,
                cluster=cluster_name,
                fsid=fsid,
                logger=log.getChild('mgr.' + first_mgr),
                wait=False,
                started=True,
            )

        # bootstrap
        log.info('Bootstrapping...')
        cmd = [
            'sudo',
            ctx.cephadm,
            '--image', ctx.ceph[cluster_name].image,
            '-v',
            'bootstrap',
            '--fsid', fsid,
            '--config', '{}/seed.{}.conf'.format(testdir, cluster_name),
            '--output-config', '/etc/ceph/{}.conf'.format(cluster_name),
            '--output-keyring',
            '/etc/ceph/{}.client.admin.keyring'.format(cluster_name),
        ]

        if not config.get("use-ca-signed-key", False):
            cmd += ['--output-pub-ssh-key', '{}/{}.pub'.format(testdir, cluster_name)]
        else:
            # ctx.ca_signed_key_info should have been set up in
            # setup_ca_signed_keys function which we expect to have
            # run before bootstrap if use-ca-signed-key is true
            signed_key_info = ctx.ca_signed_key_info
            cmd += [
                "--ssh-private-key", signed_key_info['private-key'],
                "--ssh-signed-cert", signed_key_info['ca-signed-cert'],
            ]

        if config.get("no_cgroups_split") is True:
            cmd.insert(cmd.index("bootstrap"), "--no-cgroups-split")

        if config.get('registry-login'):
            registry = config['registry-login']
            cmd += [
                "--registry-url", registry['url'],
                "--registry-username", registry['username'],
                "--registry-password", registry['password'],
            ]

        if not ctx.ceph[cluster_name].roleless:
            cmd += [
                '--mon-id', first_mon,
                '--mgr-id', first_mgr,
                '--orphan-initial-daemons',   # we will do it explicitly!
                '--skip-monitoring-stack',    # we'll provision these explicitly
            ]

        if mons[first_mon_role].startswith('['):
            cmd += ['--mon-addrv', mons[first_mon_role]]
        else:
            cmd += ['--mon-ip', mons[first_mon_role]]
        if config.get('skip_dashboard'):
            cmd += ['--skip-dashboard']
        if config.get('skip_monitoring_stack'):
            cmd += ['--skip-monitoring-stack']
        if config.get('single_host_defaults'):
            cmd += ['--single-host-defaults']
        if not config.get('avoid_pacific_features', False):
            cmd += ['--skip-admin-label']
        # bootstrap makes the keyring root 0600, so +r it for our purposes
        cmd += [
            run.Raw('&&'),
            'sudo', 'chmod', '+r',
            '/etc/ceph/{}.client.admin.keyring'.format(cluster_name),
        ]
        bootstrap_remote.run(args=cmd)

        # fetch keys and configs
        log.info('Fetching config...')
        ctx.ceph[cluster_name].config_file = \
            bootstrap_remote.read_file(f'/etc/ceph/{cluster_name}.conf')
        log.info('Fetching client.admin keyring...')
        ctx.ceph[cluster_name].admin_keyring = \
            bootstrap_remote.read_file(f'/etc/ceph/{cluster_name}.client.admin.keyring')
        log.info('Fetching mon keyring...')
        ctx.ceph[cluster_name].mon_keyring = \
            bootstrap_remote.read_file(f'/var/lib/ceph/{fsid}/mon.{first_mon}/keyring', sudo=True)

        if not config.get("use-ca-signed-key", False):
            # fetch ssh key, distribute to additional nodes
            log.info('Fetching pub ssh key...')
            ssh_pub_key = bootstrap_remote.read_file(
                f'{testdir}/{cluster_name}.pub').decode('ascii').strip()

            log.info('Installing pub ssh key for root users...')
            ctx.cluster.run(args=[
                'sudo', 'install', '-d', '-m', '0700', '/root/.ssh',
                run.Raw('&&'),
                'echo', ssh_pub_key,
                run.Raw('|'),
                'sudo', 'tee', '-a', '/root/.ssh/authorized_keys',
                run.Raw('&&'),
                'sudo', 'chmod', '0600', '/root/.ssh/authorized_keys',
            ])

        # set options
        if config.get('allow_ptrace', True):
            _shell(ctx, cluster_name, bootstrap_remote,
                   ['ceph', 'config', 'set', 'mgr', 'mgr/cephadm/allow_ptrace', 'true'])

        if not config.get('avoid_pacific_features', False):
            log.info('Distributing conf and client.admin keyring to all hosts + 0755')
            _shell(ctx, cluster_name, bootstrap_remote,
                   ['ceph', 'orch', 'client-keyring', 'set', 'client.admin',
                    '*', '--mode', '0755'],
                   check_status=False)

        # add other hosts
        for remote, roles in _cephadm_remotes(ctx, log_excluded=True):
            if remote == bootstrap_remote:
                continue

            # note: this may be redundant (see above), but it avoids
            # us having to wait for cephadm to do it.
            log.info('Writing (initial) conf and keyring to %s' % remote.shortname)
            remote.write_file(
                path='/etc/ceph/{}.conf'.format(cluster_name),
                data=ctx.ceph[cluster_name].config_file)
            remote.write_file(
                path='/etc/ceph/{}.client.admin.keyring'.format(cluster_name),
                data=ctx.ceph[cluster_name].admin_keyring)

            log.info('Adding host %s to orchestrator...' % remote.shortname)
            _shell(ctx, cluster_name, bootstrap_remote, [
                'ceph', 'orch', 'host', 'add',
                remote.shortname
            ])
            r = _shell(ctx, cluster_name, bootstrap_remote,
                       ['ceph', 'orch', 'host', 'ls', '--format=json'],
                       stdout=StringIO())
            hosts = [node['hostname'] for node in json.loads(r.stdout.getvalue())]
            assert remote.shortname in hosts

        yield

    finally:
        log.info('Cleaning up testdir ceph.* files...')
        ctx.cluster.run(args=[
            'rm', '-f',
            '{}/seed.{}.conf'.format(testdir, cluster_name),
            '{}/{}.pub'.format(testdir, cluster_name),
        ])

        log.info('Stopping all daemons...')

        # this doesn't block until they are all stopped...
        #ctx.cluster.run(args=['sudo', 'systemctl', 'stop', 'ceph.target'])

        # stop the daemons we know
        for role in ctx.daemons.resolve_role_list(None, CEPH_ROLE_TYPES, True):
            cluster, type_, id_ = teuthology.split_role(role)
            try:
                ctx.daemons.get_daemon(type_, id_, cluster).stop()
            except Exception:
                log.exception(f'Failed to stop "{role}"')
                raise

        # tear down anything left (but leave the logs behind)
        ctx.cluster.run(
            args=[
                'sudo',
                ctx.cephadm,
                'rm-cluster',
                '--fsid', fsid,
                '--force',
                '--keep-logs',
            ],
            check_status=False,  # may fail if upgrading from old cephadm
        )

        # clean up /etc/ceph
        ctx.cluster.run(args=[
            'sudo', 'rm', '-f',
            '/etc/ceph/{}.conf'.format(cluster_name),
            '/etc/ceph/{}.client.admin.keyring'.format(cluster_name),
        ])


@contextlib.contextmanager
def ceph_mons(ctx, config):
    """
    Deploy any additional mons
    """
    cluster_name = config['cluster']
    fsid = ctx.ceph[cluster_name].fsid

    try:
        daemons = {}
        if config.get('add_mons_via_daemon_add'):
            # This is the old way of adding mons that works with the (early) octopus
            # cephadm scheduler.
            num_mons = 1
            for remote, roles in _cephadm_remotes(ctx):
                for mon in [r for r in roles
                            if teuthology.is_type('mon', cluster_name)(r)]:
                    c_, _, id_ = teuthology.split_role(mon)
                    if c_ == cluster_name and id_ == ctx.ceph[cluster_name].first_mon:
                        continue
                    log.info('Adding %s on %s' % (mon, remote.shortname))
                    num_mons += 1
                    _shell(ctx, cluster_name, remote, [
                        'ceph', 'orch', 'daemon', 'add', 'mon',
                        remote.shortname + ':' + ctx.ceph[cluster_name].mons[mon] + '=' + id_,
                    ])
                    ctx.daemons.register_daemon(
                        remote, 'mon', id_,
                        cluster=cluster_name,
                        fsid=fsid,
                        logger=log.getChild(mon),
                        wait=False,
                        started=True,
                    )
                    daemons[mon] = (remote, id_)

                    with contextutil.safe_while(sleep=1, tries=180) as proceed:
                        while proceed():
                            log.info('Waiting for %d mons in monmap...' % (num_mons))
                            r = _shell(
                                ctx=ctx,
                                cluster_name=cluster_name,
                                remote=remote,
                                args=[
                                    'ceph', 'mon', 'dump', '-f', 'json',
                                ],
                                stdout=StringIO(),
                            )
                            j = json.loads(r.stdout.getvalue())
                            if len(j['mons']) == num_mons:
                                break
        else:
            nodes = []
            for remote, roles in _cephadm_remotes(ctx):
                for mon in [r for r in roles
                            if teuthology.is_type('mon', cluster_name)(r)]:
                    c_, _, id_ = teuthology.split_role(mon)
                    log.info('Adding %s on %s' % (mon, remote.shortname))
                    nodes.append(remote.shortname
                                 + ':' + ctx.ceph[cluster_name].mons[mon]
                                 + '=' + id_)
                    if c_ == cluster_name and id_ == ctx.ceph[cluster_name].first_mon:
                        continue
                    daemons[mon] = (remote, id_)

            _shell(ctx, cluster_name, remote, [
                'ceph', 'orch', 'apply', 'mon',
                str(len(nodes)) + ';' + ';'.join(nodes)]
                   )
            for mgr, i in daemons.items():
                remote, id_ = i
                ctx.daemons.register_daemon(
                    remote, 'mon', id_,
                    cluster=cluster_name,
                    fsid=fsid,
                    logger=log.getChild(mon),
                    wait=False,
                    started=True,
                )

            with contextutil.safe_while(sleep=1, tries=180) as proceed:
                while proceed():
                    log.info('Waiting for %d mons in monmap...' % (len(nodes)))
                    r = _shell(
                        ctx=ctx,
                        cluster_name=cluster_name,
                        remote=remote,
                        args=[
                            'ceph', 'mon', 'dump', '-f', 'json',
                        ],
                        stdout=StringIO(),
                    )
                    j = json.loads(r.stdout.getvalue())
                    if len(j['mons']) == len(nodes):
                        break

        # refresh our (final) ceph.conf file
        bootstrap_remote = ctx.ceph[cluster_name].bootstrap_remote
        log.info('Generating final ceph.conf file...')
        r = _shell(
            ctx=ctx,
            cluster_name=cluster_name,
            remote=bootstrap_remote,
            args=[
                'ceph', 'config', 'generate-minimal-conf',
            ],
            stdout=StringIO(),
        )
        ctx.ceph[cluster_name].config_file = r.stdout.getvalue()

        yield

    finally:
        pass


@contextlib.contextmanager
def ceph_mgrs(ctx, config):
    """
    Deploy any additional mgrs
    """
    cluster_name = config['cluster']
    fsid = ctx.ceph[cluster_name].fsid

    try:
        nodes = []
        daemons = {}
        for remote, roles in _cephadm_remotes(ctx):
            for mgr in [r for r in roles
                        if teuthology.is_type('mgr', cluster_name)(r)]:
                c_, _, id_ = teuthology.split_role(mgr)
                log.info('Adding %s on %s' % (mgr, remote.shortname))
                nodes.append(remote.shortname + '=' + id_)
                if c_ == cluster_name and id_ == ctx.ceph[cluster_name].first_mgr:
                    continue
                daemons[mgr] = (remote, id_)
        if nodes:
            _shell(ctx, cluster_name, remote, [
                'ceph', 'orch', 'apply', 'mgr',
                str(len(nodes)) + ';' + ';'.join(nodes)]
            )
        for mgr, i in daemons.items():
            remote, id_ = i
            ctx.daemons.register_daemon(
                remote, 'mgr', id_,
                cluster=cluster_name,
                fsid=fsid,
                logger=log.getChild(mgr),
                wait=False,
                started=True,
            )

        yield

    finally:
        pass


@contextlib.contextmanager
def ceph_osds(ctx, config):
    """
    Deploy OSDs
    """
    cluster_name = config['cluster']
    fsid = ctx.ceph[cluster_name].fsid

    try:
        log.info('Deploying OSDs...')

        # provision OSDs in numeric order
        id_to_remote = {}
        devs_by_remote = {}
        for remote, roles in _cephadm_remotes(ctx):
            devs_by_remote[remote] = teuthology.get_scratch_devices(remote)
            for osd in [r for r in roles
                        if teuthology.is_type('osd', cluster_name)(r)]:
                _, _, id_ = teuthology.split_role(osd)
                id_to_remote[int(id_)] = (osd, remote)

        cur = 0
        for osd_id in sorted(id_to_remote.keys()):
            osd, remote = id_to_remote[osd_id]
            _, _, id_ = teuthology.split_role(osd)
            assert int(id_) == cur
            devs = devs_by_remote[remote]
            assert devs   ## FIXME ##
            dev = devs.pop()
            if all(_ in dev for _ in ('lv', 'vg')):
                short_dev = dev.replace('/dev/', '')
            else:
                short_dev = dev
            log.info('Deploying %s on %s with %s...' % (
                osd, remote.shortname, dev))
            _shell(ctx, cluster_name, remote, [
                'ceph-volume', 'lvm', 'zap', dev])
            add_osd_args = ['ceph', 'orch', 'daemon', 'add', 'osd',
                            remote.shortname + ':' + short_dev]
            osd_method = config.get('osd_method')
            if osd_method:
                add_osd_args.append(osd_method)
            _shell(ctx, cluster_name, remote, add_osd_args)
            ctx.daemons.register_daemon(
                remote, 'osd', id_,
                cluster=cluster_name,
                fsid=fsid,
                logger=log.getChild(osd),
                wait=False,
                started=True,
            )
            cur += 1

        if cur == 0:
            _shell(ctx, cluster_name, remote, [
                'ceph', 'orch', 'apply', 'osd', '--all-available-devices',
            ])
            # expect the number of scratch devs
            num_osds = sum(map(len, devs_by_remote.values()))
            assert num_osds
        else:
            # expect the number of OSDs we created
            num_osds = cur

        log.info(f'Waiting for {num_osds} OSDs to come up...')
        with contextutil.safe_while(sleep=1, tries=120) as proceed:
            while proceed():
                p = _shell(ctx, cluster_name, ctx.ceph[cluster_name].bootstrap_remote,
                           ['ceph', 'osd', 'stat', '-f', 'json'], stdout=StringIO())
                j = json.loads(p.stdout.getvalue())
                if int(j.get('num_up_osds', 0)) == num_osds:
                    break;

        if not hasattr(ctx, 'managers'):
            ctx.managers = {}
        ctx.managers[cluster_name] = CephManager(
            ctx.ceph[cluster_name].bootstrap_remote,
            ctx=ctx,
            logger=log.getChild('ceph_manager.' + cluster_name),
            cluster=cluster_name,
            cephadm=True,
        )

        yield
    finally:
        pass


@contextlib.contextmanager
def ceph_mdss(ctx, config):
    """
    Deploy MDSss
    """
    cluster_name = config['cluster']
    fsid = ctx.ceph[cluster_name].fsid

    nodes = []
    daemons = {}
    for remote, roles in _cephadm_remotes(ctx):
        for role in [r for r in roles
                    if teuthology.is_type('mds', cluster_name)(r)]:
            c_, _, id_ = teuthology.split_role(role)
            log.info('Adding %s on %s' % (role, remote.shortname))
            nodes.append(remote.shortname + '=' + id_)
            daemons[role] = (remote, id_)
    if nodes:
        _shell(ctx, cluster_name, remote, [
            'ceph', 'orch', 'apply', 'mds',
            'all',
            str(len(nodes)) + ';' + ';'.join(nodes)]
        )
    for role, i in daemons.items():
        remote, id_ = i
        ctx.daemons.register_daemon(
            remote, 'mds', id_,
            cluster=cluster_name,
            fsid=fsid,
            logger=log.getChild(role),
            wait=False,
            started=True,
        )

    yield

@contextlib.contextmanager
def cephfs_setup(ctx, config):
    mdss = list(teuthology.all_roles_of_type(ctx.cluster, 'mds'))

    # If there are any MDSs, then create a filesystem for them to use
    # Do this last because requires mon cluster to be up and running
    if len(mdss) > 0:
        log.info('Setting up CephFS filesystem(s)...')
        cephfs_config = config.get('cephfs', {})
        fs_configs =  cephfs_config.pop('fs', [{'name': 'cephfs'}])
        set_allow_multifs = len(fs_configs) > 1

        # wait for standbys to become available (slow due to valgrind, perhaps)
        mdsc = MDSCluster(ctx)
        with contextutil.safe_while(sleep=2,tries=150) as proceed:
            while proceed():
                if len(mdsc.get_standby_daemons()) >= len(mdss):
                    break

        fss = []
        for fs_config in fs_configs:
            assert isinstance(fs_config, dict)
            name = fs_config.pop('name')
            temp = deepcopy(cephfs_config)
            teuthology.deep_merge(temp, fs_config)
            subvols = config.get('subvols', None)
            if subvols:
                teuthology.deep_merge(temp, {'subvols': subvols})
            fs = Filesystem(ctx, fs_config=temp, name=name, create=True)
            if set_allow_multifs:
                fs.set_allow_multifs()
                set_allow_multifs = False
            fss.append(fs)

        yield

        for fs in fss:
            fs.destroy()
    else:
        yield

@contextlib.contextmanager
def ceph_monitoring(daemon_type, ctx, config):
    """
    Deploy prometheus, node-exporter, etc.
    """
    cluster_name = config['cluster']
    fsid = ctx.ceph[cluster_name].fsid

    nodes = []
    daemons = {}
    for remote, roles in _cephadm_remotes(ctx):
        for role in [r for r in roles
                    if teuthology.is_type(daemon_type, cluster_name)(r)]:
            c_, _, id_ = teuthology.split_role(role)
            log.info('Adding %s on %s' % (role, remote.shortname))
            nodes.append(remote.shortname + '=' + id_)
            daemons[role] = (remote, id_)
    if nodes:
        _shell(ctx, cluster_name, remote, [
            'ceph', 'orch', 'apply', daemon_type,
            str(len(nodes)) + ';' + ';'.join(nodes)]
        )
    for role, i in daemons.items():
        remote, id_ = i
        ctx.daemons.register_daemon(
            remote, daemon_type, id_,
            cluster=cluster_name,
            fsid=fsid,
            logger=log.getChild(role),
            wait=False,
            started=True,
        )

    yield


@contextlib.contextmanager
def ceph_rgw(ctx, config):
    """
    Deploy rgw
    """
    cluster_name = config['cluster']
    fsid = ctx.ceph[cluster_name].fsid

    nodes = {}
    daemons = {}
    for remote, roles in _cephadm_remotes(ctx):
        for role in [r for r in roles
                    if teuthology.is_type('rgw', cluster_name)(r)]:
            c_, _, id_ = teuthology.split_role(role)
            log.info('Adding %s on %s' % (role, remote.shortname))
            svc = '.'.join(id_.split('.')[0:2])
            if svc not in nodes:
                nodes[svc] = []
            nodes[svc].append(remote.shortname + '=' + id_)
            daemons[role] = (remote, id_)

    for svc, nodes in nodes.items():
        _shell(ctx, cluster_name, remote, [
            'ceph', 'orch', 'apply', 'rgw', svc,
             '--placement',
             str(len(nodes)) + ';' + ';'.join(nodes)]
        )
    for role, i in daemons.items():
        remote, id_ = i
        ctx.daemons.register_daemon(
            remote, 'rgw', id_,
            cluster=cluster_name,
            fsid=fsid,
            logger=log.getChild(role),
            wait=False,
            started=True,
        )

    yield


@contextlib.contextmanager
def ceph_iscsi(ctx, config):
    """
    Deploy iSCSIs
    """
    cluster_name = config['cluster']
    fsid = ctx.ceph[cluster_name].fsid

    nodes = []
    daemons = {}
    ips = []

    for remote, roles in _cephadm_remotes(ctx):
        for role in [r for r in roles
                     if teuthology.is_type('iscsi', cluster_name)(r)]:
            c_, _, id_ = teuthology.split_role(role)
            log.info('Adding %s on %s' % (role, remote.shortname))
            nodes.append(remote.shortname + '=' + id_)
            daemons[role] = (remote, id_)
            ips.append(remote.ip_address)
    trusted_ip_list = ','.join(ips)
    if nodes:
        poolname = 'datapool'
        # ceph osd pool create datapool 3 3 replicated
        _shell(ctx, cluster_name, remote, [
            'ceph', 'osd', 'pool', 'create',
            poolname, '3', '3', 'replicated']
        )

        _shell(ctx, cluster_name, remote, [
            'rbd', 'pool', 'init', poolname]
        )

        # ceph orch apply iscsi datapool (admin)user (admin)password
        _shell(ctx, cluster_name, remote, [
            'ceph', 'orch', 'apply', 'iscsi',
            poolname, 'admin', 'admin',
            '--trusted_ip_list', trusted_ip_list,
            '--placement', str(len(nodes)) + ';' + ';'.join(nodes)]
        )

        # used by iscsi client to identify valid gateway ip's
        conf_data = dedent(f"""
        [config]
        trusted_ip_list = {trusted_ip_list}
        """)
        distribute_iscsi_gateway_cfg(ctx, conf_data)

    for role, i in daemons.items():
        remote, id_ = i
        ctx.daemons.register_daemon(
            remote, 'iscsi', id_,
            cluster=cluster_name,
            fsid=fsid,
            logger=log.getChild(role),
            wait=False,
            started=True,
        )

    yield


@contextlib.contextmanager
def ceph_clients(ctx, config):
    cluster_name = config['cluster']

    log.info('Setting up client nodes...')
    clients = ctx.cluster.only(teuthology.is_type('client', cluster_name))
    for remote, roles_for_host in clients.remotes.items():
        for role in teuthology.cluster_roles_of_type(roles_for_host, 'client',
                                                     cluster_name):
            name = teuthology.ceph_role(role)
            client_keyring = '/etc/ceph/{0}.{1}.keyring'.format(cluster_name,
                                                                name)
            r = _shell(
                ctx=ctx,
                cluster_name=cluster_name,
                remote=remote,
                args=[
                    'ceph', 'auth',
                    'get-or-create', name,
                    'mon', 'allow *',
                    'osd', 'allow *',
                    'mds', 'allow *',
                    'mgr', 'allow *',
                ],
                stdout=StringIO(),
            )
            keyring = r.stdout.getvalue()
            remote.sudo_write_file(client_keyring, keyring, mode='0644')
    yield


@contextlib.contextmanager
def ceph_initial():
    try:
        yield
    finally:
        log.info('Teardown complete')


## public methods
@contextlib.contextmanager
def stop(ctx, config):
    """
    Stop ceph daemons

    For example::
      tasks:
      - ceph.stop: [mds.*]

      tasks:
      - ceph.stop: [osd.0, osd.2]

      tasks:
      - ceph.stop:
          daemons: [osd.0, osd.2]

    """
    if config is None:
        config = {}
    elif isinstance(config, list):
        config = {'daemons': config}

    daemons = ctx.daemons.resolve_role_list(
        config.get('daemons', None), CEPH_ROLE_TYPES, True)
    clusters = set()

    for role in daemons:
        cluster, type_, id_ = teuthology.split_role(role)
        ctx.daemons.get_daemon(type_, id_, cluster).stop()
        clusters.add(cluster)

#    for cluster in clusters:
#        ctx.ceph[cluster].watchdog.stop()
#        ctx.ceph[cluster].watchdog.join()

    yield


def shell(ctx, config):
    """
    Execute (shell) commands
    """
    cluster_name = config.get('cluster', 'ceph')

    args = []
    for k in config.pop('env', []):
        args.extend(['-e', k + '=' + ctx.config.get(k, '')])
    for k in config.pop('volumes', []):
        args.extend(['-v', k])

    if 'all-roles' in config and len(config) == 1:
        a = config['all-roles']
        roles = teuthology.all_roles(ctx.cluster)
        config = dict((id_, a) for id_ in roles if not id_.startswith('host.'))
    elif 'all-hosts' in config and len(config) == 1:
        a = config['all-hosts']
        roles = teuthology.all_roles(ctx.cluster)
        config = dict((id_, a) for id_ in roles if id_.startswith('host.'))

    config = _template_transform(ctx, config, config)
    for role, cmd in config.items():
        (remote,) = ctx.cluster.only(role).remotes.keys()
        log.info('Running commands on role %s host %s', role, remote.name)
        if isinstance(cmd, list):
            for cobj in cmd:
                sh_cmd, stdin = _shell_command(cobj)
                _shell(
                    ctx,
                    cluster_name,
                    remote,
                    ['bash', '-c', sh_cmd],
                    extra_cephadm_args=args,
                    stdin=stdin,
                )

        else:
            assert isinstance(cmd, str)
            _shell(ctx, cluster_name, remote,
                   ['bash', '-ex', '-c', cmd],
                   extra_cephadm_args=args)


def _shell_command(obj):
    if isinstance(obj, str):
        return obj, None
    if isinstance(obj, dict):
        cmd = obj['cmd']
        stdin = obj.get('stdin', None)
        return cmd, stdin
    raise ValueError(f'invalid command item: {obj!r}')


def exec(ctx, config):
    """
    This is similar to the standard 'exec' task, but does template substitutions.

    TODO: this should probably be moved out of cephadm.py as it's pretty generic.
    """
    assert isinstance(config, dict), "task exec got invalid config"

    testdir = teuthology.get_testdir(ctx)

    if 'all-roles' in config and len(config) == 1:
        a = config['all-roles']
        roles = teuthology.all_roles(ctx.cluster)
        config = dict((id_, a) for id_ in roles if not id_.startswith('host.'))
    elif 'all-hosts' in config and len(config) == 1:
        a = config['all-hosts']
        roles = teuthology.all_roles(ctx.cluster)
        config = dict((id_, a) for id_ in roles if id_.startswith('host.'))

    for role, ls in config.items():
        (remote,) = ctx.cluster.only(role).remotes.keys()
        log.info('Running commands on role %s host %s', role, remote.name)
        for c in ls:
            c.replace('$TESTDIR', testdir)
            remote.run(
                args=[
                    'sudo',
                    'TESTDIR={tdir}'.format(tdir=testdir),
                    'bash',
                    '-ex',
                    '-c',
                    _template_transform(ctx, config, c)],
                )


def apply(ctx, config):
    """
    Apply spec
    
      tasks:
        - cephadm.apply:
            specs:
            - service_type: rgw
              service_id: foo
              spec:
                rgw_frontend_port: 8000
            - service_type: rgw
              service_id: bar
              spec:
                rgw_frontend_port: 9000
                zone: bar
                realm: asdf

    """
    cluster_name = config.get('cluster', 'ceph')

    specs = config.get('specs', [])
    specs = _template_transform(ctx, config, specs)
    y = yaml.dump_all(specs)

    log.info(f'Applying spec(s):\n{y}')
    _shell(
        ctx, cluster_name, ctx.ceph[cluster_name].bootstrap_remote,
        ['ceph', 'orch', 'apply', '-i', '-'],
        stdin=y,
    )


def wait_for_service(ctx, config):
    """
    Wait for a service to be fully started

      tasks:
        - cephadm.wait_for_service:
            service: rgw.foo
            timeout: 60    # defaults to 300

    """
    cluster_name = config.get('cluster', 'ceph')
    timeout = config.get('timeout', 300)
    service = config.get('service')
    assert service

    log.info(
        f'Waiting for {cluster_name} service {service} to start (timeout {timeout})...'
    )
    with contextutil.safe_while(sleep=1, tries=timeout) as proceed:
        while proceed():
            r = _shell(
                ctx=ctx,
                cluster_name=cluster_name,
                remote=ctx.ceph[cluster_name].bootstrap_remote,
                args=[
                    'ceph', 'orch', 'ls', '-f', 'json',
                ],
                stdout=StringIO(),
            )
            j = json.loads(r.stdout.getvalue())
            svc = None
            for s in j:
                if s['service_name'] == service:
                    svc = s
                    break
            if svc:
                log.info(
                    f"{service} has {s['status']['running']}/{s['status']['size']}"
                )
                if s['status']['running'] == s['status']['size']:
                    break


@contextlib.contextmanager
def tweaked_option(ctx, config):
    """
    set an option, and then restore it with its original value

    Note, due to the way how tasks are executed/nested, it's not suggested to
    use this method as a standalone task. otherwise, it's likely that it will
    restore the tweaked option at the /end/ of 'tasks' block.
    """
    saved_options = {}
    # we can complicate this when necessary
    options = ['mon-health-to-clog']
    type_, id_ = 'mon', '*'
    cluster = config.get('cluster', 'ceph')
    manager = ctx.managers[cluster]
    if id_ == '*':
        get_from = next(teuthology.all_roles_of_type(ctx.cluster, type_))
    else:
        get_from = id_
    for option in options:
        if option not in config:
            continue
        value = 'true' if config[option] else 'false'
        option = option.replace('-', '_')
        old_value = manager.get_config(type_, get_from, option)
        if value != old_value:
            saved_options[option] = old_value
            manager.inject_args(type_, id_, option, value)
    yield
    for option, value in saved_options.items():
        manager.inject_args(type_, id_, option, value)


@contextlib.contextmanager
def restart(ctx, config):
    """
   restart ceph daemons

   For example::
      tasks:
      - ceph.restart: [all]

   For example::
      tasks:
      - ceph.restart: [osd.0, mon.1, mds.*]

   or::

      tasks:
      - ceph.restart:
          daemons: [osd.0, mon.1]
          wait-for-healthy: false
          wait-for-osds-up: true

    :param ctx: Context
    :param config: Configuration
    """
    if config is None:
        config = {}
    elif isinstance(config, list):
        config = {'daemons': config}

    daemons = ctx.daemons.resolve_role_list(
        config.get('daemons', None), CEPH_ROLE_TYPES, True)
    clusters = set()

    log.info('daemons %s' % daemons)
    with tweaked_option(ctx, config):
        for role in daemons:
            cluster, type_, id_ = teuthology.split_role(role)
            d = ctx.daemons.get_daemon(type_, id_, cluster)
            assert d, 'daemon %s does not exist' % role
            d.stop()
            if type_ == 'osd':
                ctx.managers[cluster].mark_down_osd(id_)
            d.restart()
            clusters.add(cluster)

    if config.get('wait-for-healthy', True):
        for cluster in clusters:
            healthy(ctx=ctx, config=dict(cluster=cluster))
    if config.get('wait-for-osds-up', False):
        for cluster in clusters:
            ctx.managers[cluster].wait_for_all_osds_up()
    yield


@contextlib.contextmanager
def distribute_config_and_admin_keyring(ctx, config):
    """
    Distribute a sufficient config and keyring for clients
    """
    cluster_name = config['cluster']
    log.info('Distributing (final) config and client.admin keyring...')
    for remote, roles in _cephadm_remotes(ctx):
        remote.write_file(
            '/etc/ceph/{}.conf'.format(cluster_name),
            ctx.ceph[cluster_name].config_file,
            sudo=True)
        remote.write_file(
            path='/etc/ceph/{}.client.admin.keyring'.format(cluster_name),
            data=ctx.ceph[cluster_name].admin_keyring,
            sudo=True)
    try:
        yield
    finally:
        ctx.cluster.run(args=[
            'sudo', 'rm', '-f',
            '/etc/ceph/{}.conf'.format(cluster_name),
            '/etc/ceph/{}.client.admin.keyring'.format(cluster_name),
        ])


@contextlib.contextmanager
def crush_setup(ctx, config):
    cluster_name = config['cluster']

    profile = config.get('crush_tunables', 'default')
    log.info('Setting crush tunables to %s', profile)
    _shell(ctx, cluster_name, ctx.ceph[cluster_name].bootstrap_remote,
        args=['ceph', 'osd', 'crush', 'tunables', profile])
    yield


@contextlib.contextmanager
def create_rbd_pool(ctx, config):
    if config.get('create_rbd_pool', False):
      cluster_name = config['cluster']
      log.info('Waiting for OSDs to come up')
      teuthology.wait_until_osds_up(
          ctx,
          cluster=ctx.cluster,
          remote=ctx.ceph[cluster_name].bootstrap_remote,
          ceph_cluster=cluster_name,
      )
      log.info('Creating RBD pool')
      _shell(ctx, cluster_name, ctx.ceph[cluster_name].bootstrap_remote,
          args=['sudo', 'ceph', '--cluster', cluster_name,
                'osd', 'pool', 'create', 'rbd', '8'])
      _shell(ctx, cluster_name, ctx.ceph[cluster_name].bootstrap_remote,
          args=['sudo', 'ceph', '--cluster', cluster_name,
                'osd', 'pool', 'application', 'enable',
                'rbd', 'rbd', '--yes-i-really-mean-it'
          ])
    yield


@contextlib.contextmanager
def _bypass():
    yield


@contextlib.contextmanager
def initialize_config(ctx, config):
    cluster_name = config['cluster']
    testdir = teuthology.get_testdir(ctx)

    ctx.ceph[cluster_name].thrashers = []
    # fixme: setup watchdog, ala ceph.py

    ctx.ceph[cluster_name].roleless = False  # see below

    first_ceph_cluster = False
    if not hasattr(ctx, 'daemons'):
        first_ceph_cluster = True

    # cephadm mode?
    if 'cephadm_mode' not in config:
        config['cephadm_mode'] = 'root'
    assert config['cephadm_mode'] in ['root', 'cephadm-package']
    if config['cephadm_mode'] == 'root':
        ctx.cephadm = testdir + '/cephadm'
    else:
        ctx.cephadm = 'cephadm'  # in the path

    if first_ceph_cluster:
        # FIXME: this is global for all clusters
        ctx.daemons = DaemonGroup(
            use_cephadm=ctx.cephadm)

    # uuid
    fsid = str(uuid.uuid1())
    log.info('Cluster fsid is %s' % fsid)
    ctx.ceph[cluster_name].fsid = fsid

    # mon ips
    log.info('Choosing monitor IPs and ports...')
    remotes_and_roles = _cephadm_remotes(ctx)
    ips = [host for (host, port) in
           (remote.ssh.get_transport().getpeername() for (remote, role_list) in remotes_and_roles)]

    if config.get('roleless', False):
        # mons will be named after hosts
        first_mon = None
        max_mons = config.get('max_mons', 5)
        for remote, _ in remotes_and_roles:
            ctx.cluster.remotes[remote].append('mon.' + remote.shortname)
            if not first_mon:
                first_mon = remote.shortname
                bootstrap_remote = remote
            max_mons -= 1
            if not max_mons:
                break
        log.info('No mon roles; fabricating mons')

    roles = [role_list for (remote, role_list) in ctx.cluster.remotes.items()]

    ctx.ceph[cluster_name].mons = get_mons(
        roles, ips, cluster_name,
        mon_bind_msgr2=config.get('mon_bind_msgr2', True),
        mon_bind_addrvec=config.get('mon_bind_addrvec', True),
    )
    log.info('Monitor IPs: %s' % ctx.ceph[cluster_name].mons)

    if config.get('roleless', False):
        ctx.ceph[cluster_name].roleless = True
        ctx.ceph[cluster_name].bootstrap_remote = bootstrap_remote
        ctx.ceph[cluster_name].first_mon = first_mon
        ctx.ceph[cluster_name].first_mon_role = 'mon.' + first_mon
    else:
        first_mon_role = sorted(ctx.ceph[cluster_name].mons.keys())[0]
        _, _, first_mon = teuthology.split_role(first_mon_role)
        (bootstrap_remote,) = ctx.cluster.only(first_mon_role).remotes.keys()
        log.info('First mon is mon.%s on %s' % (first_mon,
                                                bootstrap_remote.shortname))
        ctx.ceph[cluster_name].bootstrap_remote = bootstrap_remote
        ctx.ceph[cluster_name].first_mon = first_mon
        ctx.ceph[cluster_name].first_mon_role = first_mon_role

        others = ctx.cluster.remotes[bootstrap_remote]
        mgrs = sorted([r for r in others
                       if teuthology.is_type('mgr', cluster_name)(r)])
        if not mgrs:
            raise RuntimeError('no mgrs on the same host as first mon %s' % first_mon)
        _, _, first_mgr = teuthology.split_role(mgrs[0])
        log.info('First mgr is %s' % (first_mgr))
        ctx.ceph[cluster_name].first_mgr = first_mgr
    yield


def _disable_systemd_resolved(ctx, remote):
    r = remote.run(args=['ss', '-lunH'], stdout=StringIO())
    # this heuristic tries to detect if systemd-resolved is running
    if '%lo:53' not in r.stdout.getvalue():
        return
    log.info('Disabling systemd-resolved on %s', remote.shortname)
    # Samba AD DC container DNS support conflicts with resolved stub
    # resolver when using host networking. And we want host networking
    # because it is the simplest thing to set up.  We therefore will turn
    # off the stub resolver.
    r = remote.run(
        args=['sudo', 'cat', '/etc/systemd/resolved.conf'],
        stdout=StringIO(),
    )
    resolved_conf = r.stdout.getvalue()
    setattr(ctx, 'orig_resolved_conf', resolved_conf)
    new_resolved_conf = (
        resolved_conf + '\n# EDITED BY TEUTHOLOGY: deploy_samba_ad_dc\n'
    )
    if '[Resolve]' not in new_resolved_conf.splitlines():
        new_resolved_conf += '[Resolve]\n'
    new_resolved_conf += 'DNSStubListener=no\n'
    remote.write_file(
        path='/etc/systemd/resolved.conf',
        data=new_resolved_conf,
        sudo=True,
    )
    remote.run(args=['sudo', 'systemctl', 'restart', 'systemd-resolved'])
    r = remote.run(args=['ss', '-lunH'], stdout=StringIO())
    assert '%lo:53' not in r.stdout.getvalue()
    # because docker is a big fat persistent deamon, we need to bounce it
    # after resolved is restarted
    remote.run(args=['sudo', 'systemctl', 'restart', 'docker'])


def _reset_systemd_resolved(ctx, remote):
    orig_resolved_conf = getattr(ctx, 'orig_resolved_conf', None)
    if not orig_resolved_conf:
        return  # no orig_resolved_conf means nothing to reset
    log.info('Resetting systemd-resolved state on %s', remote.shortname)
    remote.write_file(
        path='/etc/systemd/resolved.conf',
        data=orig_resolved_conf,
        sudo=True,
    )
    remote.run(args=['sudo', 'systemctl', 'restart', 'systemd-resolved'])
    setattr(ctx, 'orig_resolved_conf', None)


def _samba_ad_dc_conf(ctx, remote, cengine):
    # this config has not been tested outside of smithi nodes. it's possible
    # that this will break when used elsewhere because we have to list
    # interfaces explicitly. Later I may add a feature to sambacc to exclude
    # known-unwanted interfaces that having to specify known good interfaces.
    cf = {
        "samba-container-config": "v0",
        "configs": {
            "demo": {
                "instance_features": ["addc"],
                "domain_settings": "sink",
                "instance_name": "dc1",
            }
        },
        "domain_settings": {
            "sink": {
                "realm": "DOMAIN1.SINK.TEST",
                "short_domain": "DOMAIN1",
                "admin_password": "Passw0rd",
                "interfaces": {
                    "exclude_pattern": "^docker[0-9]+$",
                },
            }
        },
        "domain_groups": {
            "sink": [
                {"name": "supervisors"},
                {"name": "employees"},
                {"name": "characters"},
                {"name": "bulk"},
            ]
        },
        "domain_users": {
            "sink": [
                {
                    "name": "bwayne",
                    "password": "1115Rose.",
                    "given_name": "Bruce",
                    "surname": "Wayne",
                    "member_of": ["supervisors", "characters", "employees"],
                },
                {
                    "name": "ckent",
                    "password": "1115Rose.",
                    "given_name": "Clark",
                    "surname": "Kent",
                    "member_of": ["characters", "employees"],
                },
                {
                    "name": "user0",
                    "password": "1115Rose.",
                    "given_name": "George0",
                    "surname": "Hue-Sir",
                    "member_of": ["bulk"],
                },
                {
                    "name": "user1",
                    "password": "1115Rose.",
                    "given_name": "George1",
                    "surname": "Hue-Sir",
                    "member_of": ["bulk"],
                },
                {
                    "name": "user2",
                    "password": "1115Rose.",
                    "given_name": "George2",
                    "surname": "Hue-Sir",
                    "member_of": ["bulk"],
                },
                {
                    "name": "user3",
                    "password": "1115Rose.",
                    "given_name": "George3",
                    "surname": "Hue-Sir",
                    "member_of": ["bulk"],
                },
            ]
        },
    }
    cf_json = json.dumps(cf)
    remote.run(args=['sudo', 'mkdir', '-p', '/var/tmp/samba'])
    remote.write_file(
        path='/var/tmp/samba/container.json', data=cf_json, sudo=True
    )
    return [
        '--volume=/var/tmp/samba:/etc/samba-container:ro',
        '-eSAMBACC_CONFIG=/etc/samba-container/container.json',
    ]


@contextlib.contextmanager
def deploy_samba_ad_dc(ctx, config):
    role = config.get('role')
    ad_dc_image = config.get(
        'ad_dc_image', 'quay.io/samba.org/samba-ad-server:latest'
    )
    samba_client_image = config.get(
        'samba_client_image', 'quay.io/samba.org/samba-client:latest'
    )
    test_user_pass = config.get('test_user_pass', 'DOMAIN1\\ckent%1115Rose.')
    if not role:
        raise ConfigError(
            "you must specify a role to allocate a host for the AD DC"
        )
    (remote,) = ctx.cluster.only(role).remotes.keys()
    ip = remote.ssh.get_transport().getpeername()[0]
    cengine = 'podman'
    try:
        log.info("Testing if podman is available")
        remote.run(args=['sudo', cengine, '--help'])
    except CommandFailedError:
        log.info("Failed to find podman. Using docker")
        cengine = 'docker'
    remote.run(args=['sudo', cengine, 'pull', ad_dc_image])
    remote.run(args=['sudo', cengine, 'pull', samba_client_image])
    _disable_systemd_resolved(ctx, remote)
    remote.run(
        args=[
            'sudo',
            'mkdir',
            '-p',
            '/var/lib/samba/container/logs',
            '/var/lib/samba/container/data',
        ]
    )
    remote.run(
        args=[
            'sudo',
            cengine,
            'run',
            '-d',
            '--name=samba-ad',
            '--network=host',
            '--privileged',
        ]
        + _samba_ad_dc_conf(ctx, remote, cengine)
        + [ad_dc_image]
    )

    # test that the ad dc is running and basically works
    connected = False
    samba_client_container_cmd = [
        'sudo',
        cengine,
        'run',
        '--rm',
        '--net=host',
        f'--dns={ip}',
        '-eKRB5_CONFIG=/dev/null',
        samba_client_image,
    ]
    for idx in range(10):
        time.sleep((2 ** (1 + idx)) / 8)
        log.info("Probing SMB status of DC %s, idx=%s", ip, idx)
        cmd = samba_client_container_cmd + [
            'smbclient',
            '-U',
            test_user_pass,
            '//domain1.sink.test/sysvol',
            '-c',
            'ls',
        ]
        try:
            remote.run(args=cmd)
            connected = True
            log.info("SMB status probe succeeded")
            break
        except CommandFailedError:
            pass
    if not connected:
        raise RuntimeError('failed to connect to AD DC SMB share')

    setattr(ctx, 'samba_ad_dc_ip', ip)
    setattr(ctx, 'samba_client_container_cmd', samba_client_container_cmd)
    try:
        yield
    finally:
        try:
            remote.run(args=['sudo', cengine, 'stop', 'samba-ad'])
        except CommandFailedError:
            log.error("Failed to stop samba-ad container")
        try:
            remote.run(args=['sudo', cengine, 'rm', 'samba-ad'])
        except CommandFailedError:
            log.error("Failed to remove samba-ad container")
        remote.run(
            args=[
                'sudo',
                'rm',
                '-rf',
                '/var/lib/samba/container/logs',
                '/var/lib/samba/container/data',
            ]
        )
        _reset_systemd_resolved(ctx, remote)
        setattr(ctx, 'samba_ad_dc_ip', None)
        setattr(ctx, 'samba_client_container_cmd', None)


@contextlib.contextmanager
def task(ctx, config):
    """
    Deploy ceph cluster using cephadm

    For example, teuthology.yaml can contain the 'defaults' section:

        defaults:
          cephadm:
            containers:
              image: 'quay.io/ceph-ci/ceph'

    Using overrides makes it possible to customize it per run.
    The equivalent 'overrides' section looks like:

        overrides:
          cephadm:
            containers:
              image: 'quay.io/ceph-ci/ceph'
            registry-login:
              url:  registry-url
              username: registry-user
              password: registry-password

    :param ctx: the argparse.Namespace object
    :param config: the config dict
    """
    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('ceph', {}))
    teuthology.deep_merge(config, overrides.get('cephadm', {}))
    log.info('Config: ' + str(config))

    # set up cluster context
    if not hasattr(ctx, 'ceph'):
        ctx.ceph = {}
    if 'cluster' not in config:
        config['cluster'] = 'ceph'
    cluster_name = config['cluster']
    if cluster_name not in ctx.ceph:
        ctx.ceph[cluster_name] = argparse.Namespace()
        ctx.ceph[cluster_name].bootstrapped = False

    # image
    teuth_defaults = teuth_config.get('defaults', {})
    cephadm_defaults = teuth_defaults.get('cephadm', {})
    containers_defaults = cephadm_defaults.get('containers', {})
    container_image_name = containers_defaults.get('image', None)

    containers = config.get('containers', {})
    container_image_name = containers.get('image', container_image_name)

    if not hasattr(ctx.ceph[cluster_name], 'image'):
        ctx.ceph[cluster_name].image = config.get('image')
    ref = ctx.config.get("branch", "main")
    if not ctx.ceph[cluster_name].image:
        if not container_image_name:
            raise Exception("Configuration error occurred. "
                            "The 'image' value is undefined for 'cephadm' task. "
                            "Please provide corresponding options in the task's "
                            "config, task 'overrides', or teuthology 'defaults' "
                            "section.")
        sha1 = config.get('sha1')
        flavor = config.get('flavor', 'default')

        if sha1:
            if flavor == "crimson":
                ctx.ceph[cluster_name].image = container_image_name + ':' + sha1 + '-' + flavor
            else:
                ctx.ceph[cluster_name].image = container_image_name + ':' + sha1
            ref = sha1
        else:
            # fall back to using the branch value
            ctx.ceph[cluster_name].image = container_image_name + ':' + ref
    log.info('Cluster image is %s' % ctx.ceph[cluster_name].image)


    with contextutil.nested(
            #if the cluster is already bootstrapped bypass corresponding methods
            lambda: _bypass() if (ctx.ceph[cluster_name].bootstrapped) \
                              else initialize_config(ctx=ctx, config=config),
            lambda: ceph_initial(),
            lambda: normalize_hostnames(ctx=ctx),
            lambda: _bypass() if (ctx.ceph[cluster_name].bootstrapped) \
                              else download_cephadm(ctx=ctx, config=config, ref=ref),
            lambda: ceph_log(ctx=ctx, config=config),
            lambda: ceph_crash(ctx=ctx, config=config),
            lambda: pull_image(ctx=ctx, config=config),
            lambda: _bypass() if not (config.get('use-ca-signed-key', False)) \
                              else setup_ca_signed_keys(ctx, config),
            lambda: _bypass() if (ctx.ceph[cluster_name].bootstrapped) \
                              else ceph_bootstrap(ctx, config),
            lambda: crush_setup(ctx=ctx, config=config),
            lambda: ceph_mons(ctx=ctx, config=config),
            lambda: distribute_config_and_admin_keyring(ctx=ctx, config=config),
            lambda: ceph_mgrs(ctx=ctx, config=config),
            lambda: ceph_osds(ctx=ctx, config=config),
            lambda: ceph_mdss(ctx=ctx, config=config),
            lambda: cephfs_setup(ctx=ctx, config=config),
            lambda: ceph_rgw(ctx=ctx, config=config),
            lambda: ceph_iscsi(ctx=ctx, config=config),
            lambda: ceph_monitoring('prometheus', ctx=ctx, config=config),
            lambda: ceph_monitoring('node-exporter', ctx=ctx, config=config),
            lambda: ceph_monitoring('alertmanager', ctx=ctx, config=config),
            lambda: ceph_monitoring('grafana', ctx=ctx, config=config),
            lambda: ceph_clients(ctx=ctx, config=config),
            lambda: create_rbd_pool(ctx=ctx, config=config),
    ):
        try:
            if config.get('wait-for-healthy', True):
                healthy(ctx=ctx, config=config)

            log.info('Setup complete, yielding')
            yield

        finally:
            log.info('Teardown begin')

