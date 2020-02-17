"""
Ceph cluster task, deployed via cephadm orchestrator
"""
from cStringIO import StringIO

import argparse
import configobj
import contextlib
import logging
import os
import json
import uuid

from ceph_manager import CephManager
from tarfile import ReadError
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.orchestra import run
from teuthology.orchestra.daemon import DaemonGroup
from teuthology.config import config as teuth_config

# these items we use from ceph.py should probably eventually move elsewhere
from tasks.ceph import get_mons, healthy

CEPH_ROLE_TYPES = ['mon', 'mgr', 'osd', 'mds', 'rgw']

log = logging.getLogger(__name__)


def _shell(ctx, cluster_name, remote, args, **kwargs):
    testdir = teuthology.get_testdir(ctx)
    return remote.run(
        args=[
            'sudo',
            ctx.cephadm,
            '--image', ctx.ceph[cluster_name].image,
            'shell',
            '-c', '{}/{}.conf'.format(testdir, cluster_name),
            '-k', '{}/{}.keyring'.format(testdir, cluster_name),
            '--fsid', ctx.ceph[cluster_name].fsid,
            '--',
            ] + args,
        **kwargs
    )

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

@contextlib.contextmanager
def normalize_hostnames(ctx):
    """
    Ensure we have short hostnames throughout, for consistency between
    remote.shortname and socket.gethostname() in cephadm.
    """
    log.info('Normalizing hostnames...')
    ctx.cluster.run(args=[
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
        ref = config.get('cephadm_branch', ref)
        git_url = teuth_config.get_ceph_git_url()
        log.info('Downloading cephadm (repo %s ref %s)...' % (git_url, ref))
        ctx.cluster.run(
            args=[
                'git', 'archive',
                '--remote=' + git_url,
                ref,
                'src/cephadm/cephadm',
                run.Raw('|'),
                'tar', '-xO', 'src/cephadm/cephadm',
                run.Raw('>'),
                ctx.cephadm,
                run.Raw('&&'),
                'test', '-s',
                ctx.cephadm,
                run.Raw('&&'),
                'chmod', '+x',
                ctx.cephadm,
            ],
        )

    try:
        yield
    finally:
        log.info('Removing cluster...')
        ctx.cluster.run(args=[
            'sudo',
            ctx.cephadm,
            'rm-cluster',
            '--fsid', ctx.ceph[cluster_name].fsid,
            '--force',
        ])

        if config.get('cephadm_mode') == 'root':
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
                             config['log-whitelist']) is not None:
            log.warning('Found errors (ERR|WRN|SEC) in cluster log')
            ctx.summary['success'] = False
            # use the most severe problem as the failure reason
            if 'failure_reason' not in ctx.summary:
                for pattern in ['\[SEC\]', '\[ERR\]', '\[WRN\]']:
                    match = first_in_ceph_log(pattern, config['log-whitelist'])
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
                        'sudo',
                        'find',
                        '/var/log/ceph',   # all logs, not just for the cluster
                        '-name',
                        '*.log',
                        '-print0',
                        run.Raw('|'),
                        'sudo',
                        'xargs',
                        '-0',
                        '--no-run-if-empty',
                        '--',
                        'gzip',
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
                sub = os.path.join(path, remote.name)
                try:
                    os.makedirs(sub)
                except OSError:
                    pass
                teuthology.pull_directory(remote, '/var/log/ceph',  # everything
                                          os.path.join(sub, 'log'))

@contextlib.contextmanager
def ceph_crash(ctx, config):
    """
    Gather crash dumps from /var/lib/ceph/$fsid/crash
    """
    cluster_name = config['cluster']
    fsid = ctx.ceph[cluster_name].fsid

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
                sub = os.path.join(path, remote.name)
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
def ceph_bootstrap(ctx, config):
    cluster_name = config['cluster']
    testdir = teuthology.get_testdir(ctx)
    fsid = ctx.ceph[cluster_name].fsid

    mons = ctx.ceph[cluster_name].mons
    first_mon_role = sorted(mons.keys())[0]
    _, _, first_mon = teuthology.split_role(first_mon_role)
    (bootstrap_remote,) = ctx.cluster.only(first_mon_role).remotes.keys()
    log.info('First mon is mon.%s on %s' % (first_mon,
                                            bootstrap_remote.shortname))
    ctx.ceph[cluster_name].bootstrap_remote = bootstrap_remote
    ctx.ceph[cluster_name].first_mon = first_mon

    others = ctx.cluster.remotes[bootstrap_remote]
    log.info('others %s' % others)
    mgrs = sorted([r for r in others
                   if teuthology.is_type('mgr', cluster_name)(r)])
    if not mgrs:
        raise RuntimeError('no mgrs on the same host as first mon %s' % first_mon)
    _, _, first_mgr = teuthology.split_role(mgrs[0])
    log.info('First mgr is %s' % (first_mgr))
    ctx.ceph[cluster_name].first_mgr = first_mgr

    try:
        # write seed config
        log.info('Writing seed config...')
        conf_fp = StringIO()
        seed_config = build_initial_config(ctx, config)
        seed_config.write(conf_fp)
        teuthology.write_file(
            remote=bootstrap_remote,
            path='{}/seed.{}.conf'.format(testdir, cluster_name),
            data=conf_fp.getvalue())
        log.debug('Final config:\n' + conf_fp.getvalue())
        ctx.ceph[cluster_name].conf = seed_config

        # bootstrap
        log.info('Bootstrapping...')
        cmd = [
            'sudo',
            ctx.cephadm,
            '--image', ctx.ceph[cluster_name].image,
            'bootstrap',
            '--fsid', fsid,
            '--mon-id', first_mon,
            '--mgr-id', first_mgr,
            '--config', '{}/seed.{}.conf'.format(testdir, cluster_name),
            '--output-config', '{}/{}.conf'.format(testdir, cluster_name),
            '--output-keyring', '{}/{}.keyring'.format(testdir, cluster_name),
            '--output-pub-ssh-key', '{}/{}.pub'.format(testdir, cluster_name),
        ]
        if mons[first_mon_role].startswith('['):
            cmd += ['--mon-addrv', mons[first_mon_role]]
        else:
            cmd += ['--mon-ip', mons[first_mon_role]]
        if config.get('skip_dashboard'):
            cmd += ['--skip-dashboard']
        # bootstrap makes the keyring root 0600, so +r it for our purposes
        cmd += [
            run.Raw('&&'),
            'sudo', 'chmod', '+r', '{}/{}.keyring'.format(testdir, cluster_name),
        ]
        bootstrap_remote.run(args=cmd)

        # register initial daemons
        ctx.daemons.register_daemon(
            bootstrap_remote, 'mon', first_mon,
            cluster=cluster_name,
            fsid=fsid,
            logger=log.getChild('mon.' + first_mon),
            wait=False,
            started=True,
        )
        ctx.daemons.register_daemon(
            bootstrap_remote, 'mgr', first_mgr,
            cluster=cluster_name,
            fsid=fsid,
            logger=log.getChild('mgr.' + first_mgr),
            wait=False,
            started=True,
        )

        # fetch keys and configs
        log.info('Fetching config...')
        ctx.ceph[cluster_name].config_file = teuthology.get_file(
            remote=bootstrap_remote,
            path='{}/{}.conf'.format(testdir, cluster_name))
        log.info('Fetching client.admin keyring...')
        ctx.ceph[cluster_name].admin_keyring = teuthology.get_file(
            remote=bootstrap_remote,
            path='{}/{}.keyring'.format(testdir, cluster_name))
        log.info('Fetching mon keyring...')
        ctx.ceph[cluster_name].mon_keyring = teuthology.get_file(
            remote=bootstrap_remote,
            path='/var/lib/ceph/%s/mon.%s/keyring' % (fsid, first_mon),
            sudo=True)

        # fetch ssh key, distribute to additional nodes
        log.info('Fetching pub ssh key...')
        ssh_pub_key = teuthology.get_file(
            remote=bootstrap_remote,
            path='{}/{}.pub'.format(testdir, cluster_name)
        ).strip()

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

        # add other hosts
        for remote in ctx.cluster.remotes.keys():
            if remote == bootstrap_remote:
                continue
            log.info('Writing conf and keyring to %s' % remote.shortname)
            teuthology.write_file(
                remote=remote,
                path='{}/{}.conf'.format(testdir, cluster_name),
                data=ctx.ceph[cluster_name].config_file)
            teuthology.write_file(
                remote=remote,
                path='{}/{}.keyring'.format(testdir, cluster_name),
                data=ctx.ceph[cluster_name].admin_keyring)

            log.info('Adding host %s to orchestrator...' % remote.shortname)
            _shell(ctx, cluster_name, remote, [
                'ceph', 'orchestrator', 'host', 'add',
                remote.shortname
            ])

        yield

    finally:
        log.info('Cleaning up testdir ceph.* files...')
        ctx.cluster.run(args=[
            'rm', '-f',
            '{}/seed.{}.conf'.format(testdir, cluster_name),
            '{}/{}.pub'.format(testdir, cluster_name),
            '{}/{}.conf'.format(testdir, cluster_name),
            '{}/{}.keyring'.format(testdir, cluster_name),
        ])

        log.info('Stopping all daemons...')

        # this doesn't block until they are all stopped...
        #ctx.cluster.run(args=['sudo', 'systemctl', 'stop', 'ceph.target'])

        # so, stop them individually
        for role in ctx.daemons.resolve_role_list(None, CEPH_ROLE_TYPES):
            cluster, type_, id_ = teuthology.split_role(role)
            ctx.daemons.get_daemon(type_, id_, cluster).stop()

@contextlib.contextmanager
def ceph_mons(ctx, config):
    """
    Deploy any additional mons
    """
    cluster_name = config['cluster']
    fsid = ctx.ceph[cluster_name].fsid
    num_mons = 1

    try:
        for remote, roles in ctx.cluster.remotes.items():
            for mon in [r for r in roles
                        if teuthology.is_type('mon', cluster_name)(r)]:
                c_, _, id_ = teuthology.split_role(mon)
                if c_ == cluster_name and id_ == ctx.ceph[cluster_name].first_mon:
                    continue
                log.info('Adding %s on %s' % (mon, remote.shortname))
                num_mons += 1
                _shell(ctx, cluster_name, remote, [
                    'ceph', 'orchestrator', 'mon', 'update',
                    str(num_mons),
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

        # refresh ceph.conf files for all mons + first mgr
        for remote, roles in ctx.cluster.remotes.items():
            for mon in [r for r in roles
                        if teuthology.is_type('mon', cluster_name)(r)]:
                c_, _, id_ = teuthology.split_role(mon)
                _shell(ctx, cluster_name, remote, [
                    'ceph', 'orchestrator', 'service-instance', 'reconfig',
                    'mon', id_,
                ])
        _shell(ctx, cluster_name, ctx.ceph[cluster_name].bootstrap_remote, [
            'ceph', 'orchestrator', 'service-instance', 'reconfig',
            'mgr', ctx.ceph[cluster_name].first_mgr,
        ])

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
        for remote, roles in ctx.cluster.remotes.items():
            for mgr in [r for r in roles
                        if teuthology.is_type('mgr', cluster_name)(r)]:
                c_, _, id_ = teuthology.split_role(mgr)
                if c_ == cluster_name and id_ == ctx.ceph[cluster_name].first_mgr:
                    continue
                log.info('Adding %s on %s' % (mgr, remote.shortname))
                nodes.append(remote.shortname + '=' + id_)
                daemons[mgr] = (remote, id_)
        if nodes:
            _shell(ctx, cluster_name, remote, [
                'ceph', 'orchestrator', 'mgr', 'update',
                str(len(nodes) + 1)] + nodes
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
        for remote, roles in ctx.cluster.remotes.items():
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
            log.info('Deploying %s on %s with %s...' % (
                osd, remote.shortname, dev))
            _shell(ctx, cluster_name, remote, [
                'ceph-volume', 'lvm', 'zap', dev])
            _shell(ctx, cluster_name, remote, [
                'ceph', 'orchestrator', 'osd', 'create',
                remote.shortname + ':' + dev
            ])
            ctx.daemons.register_daemon(
                remote, 'osd', id_,
                cluster=cluster_name,
                fsid=fsid,
                logger=log.getChild(osd),
                wait=False,
                started=True,
            )
            cur += 1

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
    for remote, roles in ctx.cluster.remotes.items():
        for role in [r for r in roles
                    if teuthology.is_type('mds', cluster_name)(r)]:
            c_, _, id_ = teuthology.split_role(role)
            log.info('Adding %s on %s' % (role, remote.shortname))
            nodes.append(remote.shortname + '=' + id_)
            daemons[role] = (remote, id_)
    if nodes:
        _shell(ctx, cluster_name, remote, [
            'ceph', 'orchestrator', 'mds', 'update',
            'all',
            str(len(nodes))] + nodes
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
def ceph_clients(ctx, config):
    cluster_name = config['cluster']
    testdir = teuthology.get_testdir(ctx)

    log.info('Setting up client nodes...')
    clients = ctx.cluster.only(teuthology.is_type('client', cluster_name))
    testdir = teuthology.get_testdir(ctx)
    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)
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
            teuthology.sudo_write_file(
                remote=remote,
                path=client_keyring,
                data=keyring,
                perms='0644'
            )
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

    if 'all' in config and len(config) == 1:
        a = config['all']
        roles = teuthology.all_roles(ctx.cluster)
        config = dict((id_, a) for id_ in roles)

    for role, ls in config.items():
        (remote,) = ctx.cluster.only(role).remotes.keys()
        log.info('Running commands on role %s host %s', role, remote.name)
        for c in ls:
            _shell(ctx, cluster_name, remote, c.split(' '))

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
    log.info('Distributing config and client.admin keyring...')
    for remote, roles in ctx.cluster.remotes.items():
        remote.run(args=['sudo', 'mkdir', '-p', '/etc/ceph'])
        teuthology.sudo_write_file(
            remote=remote,
            path='/etc/ceph/{}.conf'.format(cluster_name),
            data=ctx.ceph[cluster_name].config_file)
        teuthology.sudo_write_file(
            remote=remote,
            path='/etc/ceph/{}.client.admin.keyring'.format(cluster_name),
            data=ctx.ceph[cluster_name].admin_keyring)
    try:
        yield
    finally:
        ctx.cluster.run(args=[
            'sudo', 'rm', '-f',
            '/etc/ceph/{}.conf'.format(cluster_name),
            '/etc/ceph/{}.client.admin.keyring'.format(cluster_name),
        ])

@contextlib.contextmanager
def task(ctx, config):
    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('ceph', {}))
    log.info('Config: ' + str(config))

    testdir = teuthology.get_testdir(ctx)

    # set up cluster context
    first_ceph_cluster = False
    if not hasattr(ctx, 'daemons'):
        first_ceph_cluster = True
    if not hasattr(ctx, 'ceph'):
        ctx.ceph = {}
        ctx.managers = {}
    if 'cluster' not in config:
        config['cluster'] = 'ceph'
    cluster_name = config['cluster']
    ctx.ceph[cluster_name] = argparse.Namespace()

    ctx.ceph[cluster_name].thrashers = []
    # fixme: setup watchdog, ala ceph.py

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

    # image
    ctx.ceph[cluster_name].image = config.get('image')
    ref = None
    if not ctx.ceph[cluster_name].image:
        sha1 = config.get('sha1')
        if sha1:
            ctx.ceph[cluster_name].image = 'quay.io/ceph-ci/ceph:%s' % sha1
            ref = sha1
        else:
            # hmm, fall back to branch?
            branch = config.get('branch', 'master')
            ref = branch
            ctx.ceph[cluster_name].image = 'quay.io/ceph-ci/ceph:%s' % branch
    log.info('Cluster image is %s' % ctx.ceph[cluster_name].image)

    # uuid
    fsid = str(uuid.uuid1())
    log.info('Cluster fsid is %s' % fsid)
    ctx.ceph[cluster_name].fsid = fsid

    # mon ips
    log.info('Choosing monitor IPs and ports...')
    remotes_and_roles = ctx.cluster.remotes.items()
    roles = [role_list for (remote, role_list) in remotes_and_roles]
    ips = [host for (host, port) in
           (remote.ssh.get_transport().getpeername() for (remote, role_list) in remotes_and_roles)]
    ctx.ceph[cluster_name].mons = get_mons(
        roles, ips, cluster_name,
        mon_bind_msgr2=config.get('mon_bind_msgr2', True),
        mon_bind_addrvec=config.get('mon_bind_addrvec', True),
        )
    log.info('Monitor IPs: %s' % ctx.ceph[cluster_name].mons)

    with contextutil.nested(
            lambda: ceph_initial(),
            lambda: normalize_hostnames(ctx=ctx),
            lambda: download_cephadm(ctx=ctx, config=config, ref=ref),
            lambda: ceph_log(ctx=ctx, config=config),
            lambda: ceph_crash(ctx=ctx, config=config),
            lambda: ceph_bootstrap(ctx=ctx, config=config),
            lambda: ceph_mons(ctx=ctx, config=config),
            lambda: ceph_mgrs(ctx=ctx, config=config),
            lambda: ceph_osds(ctx=ctx, config=config),
            lambda: ceph_mdss(ctx=ctx, config=config),
            lambda: ceph_clients(ctx=ctx, config=config),
            lambda: distribute_config_and_admin_keyring(ctx=ctx, config=config),
    ):
        ctx.managers[cluster_name] = CephManager(
            ctx.ceph[cluster_name].bootstrap_remote,
            ctx=ctx,
            logger=log.getChild('ceph_manager.' + cluster_name),
            cluster=cluster_name,
            cephadm=True,
        )

        try:
            if config.get('wait-for-healthy', True):
                healthy(ctx=ctx, config=config)

            log.info('Setup complete, yielding')
            yield

        finally:
            log.info('Teardown begin')
