"""
Ceph cluster task.

Handle the setup, starting, and clean-up of a Ceph cluster.
"""
from cStringIO import StringIO

import argparse
import contextlib
import logging
import os
import json
import time
import gevent

from ceph_manager import CephManager, write_conf
from tasks.cephfs.filesystem import Filesystem
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology import exceptions
from teuthology.orchestra import run
import ceph_client as cclient
from teuthology.orchestra.daemon import DaemonGroup

CEPH_ROLE_TYPES = ['mon', 'osd', 'mds', 'rgw']

log = logging.getLogger(__name__)


@contextlib.contextmanager
def ceph_log(ctx, config):
    """
    Create /var/log/ceph log directory that is open to everyone.
    Add valgrind and profiling-logger directories.

    :param ctx: Context
    :param config: Configuration
    """
    log.info('Making ceph log dir writeable by non-root...')
    run.wait(
        ctx.cluster.run(
            args=[
                'sudo',
                'chmod',
                '777',
                '/var/log/ceph',
            ],
            wait=False,
        )
    )
    log.info('Disabling ceph logrotate...')
    run.wait(
        ctx.cluster.run(
            args=[
                'sudo',
                'rm', '-f', '--',
                '/etc/logrotate.d/ceph',
            ],
            wait=False,
        )
    )
    log.info('Creating extra log directories...')
    run.wait(
        ctx.cluster.run(
            args=[
                'sudo',
                'install', '-d', '-m0777', '--',
                '/var/log/ceph/valgrind',
                '/var/log/ceph/profiling-logger',
            ],
            wait=False,
        )
    )

    class Rotater(object):
        stop_event = gevent.event.Event()

        def invoke_logrotate(self):
            # 1) install ceph-test.conf in /etc/logrotate.d
            # 2) continuously loop over logrotate invocation with ceph-test.conf
            while not self.stop_event.is_set():
                self.stop_event.wait(timeout=30)
                run.wait(
                    ctx.cluster.run(
                        args=['sudo', 'logrotate', '/etc/logrotate.d/ceph-test.conf'
                              ],
                        wait=False,
                    )
                )

        def begin(self):
            self.thread = gevent.spawn(self.invoke_logrotate)

        def end(self):
            self.stop_event.set()
            self.thread.get()

    def write_rotate_conf(ctx, daemons):
        testdir = teuthology.get_testdir(ctx)
        rotate_conf_path = os.path.join(os.path.dirname(__file__), 'logrotate.conf')
        with file(rotate_conf_path, 'rb') as f:
            conf = ""
            for daemon, size in daemons.iteritems():
                log.info('writing logrotate stanza for {daemon}'.format(daemon=daemon))
                conf += f.read().format(daemon_type=daemon, max_size=size)
                f.seek(0, 0)

            for remote in ctx.cluster.remotes.iterkeys():
                teuthology.write_file(remote=remote,
                                      path='{tdir}/logrotate.ceph-test.conf'.format(tdir=testdir),
                                      data=StringIO(conf)
                                      )
                remote.run(
                    args=[
                        'sudo',
                        'mv',
                        '{tdir}/logrotate.ceph-test.conf'.format(tdir=testdir),
                        '/etc/logrotate.d/ceph-test.conf',
                        run.Raw('&&'),
                        'sudo',
                        'chmod',
                        '0644',
                        '/etc/logrotate.d/ceph-test.conf',
                        run.Raw('&&'),
                        'sudo',
                        'chown',
                        'root.root',
                        '/etc/logrotate.d/ceph-test.conf'
                    ]
                )
                remote.chcon('/etc/logrotate.d/ceph-test.conf',
                             'system_u:object_r:etc_t:s0')

    if ctx.config.get('log-rotate'):
        daemons = ctx.config.get('log-rotate')
        log.info('Setting up log rotation with ' + str(daemons))
        write_rotate_conf(ctx, daemons)
        logrotater = Rotater()
        logrotater.begin()
    try:
        yield

    finally:
        if ctx.config.get('log-rotate'):
            log.info('Shutting down logrotate')
            logrotater.end()
            ctx.cluster.run(
                args=['sudo', 'rm', '/etc/logrotate.d/ceph-test.conf'
                      ]
            )
        if ctx.archive is not None and \
                not (ctx.config.get('archive-on-error') and ctx.summary['success']):
            # and logs
            log.info('Compressing logs...')
            run.wait(
                ctx.cluster.run(
                    args=[
                        'sudo',
                        'find',
                        '/var/log/ceph',
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
            os.makedirs(path)
            for remote in ctx.cluster.remotes.iterkeys():
                sub = os.path.join(path, remote.shortname)
                os.makedirs(sub)
                teuthology.pull_directory(remote, '/var/log/ceph',
                                          os.path.join(sub, 'log'))


def assign_devs(roles, devs):
    """
    Create a dictionary of devs indexed by roles

    :param roles: List of roles
    :param devs: Corresponding list of devices.
    :returns: Dictionary of devs indexed by roles.
    """
    return dict(zip(roles, devs))


@contextlib.contextmanager
def valgrind_post(ctx, config):
    """
    After the tests run, look throught all the valgrind logs.  Exceptions are raised
    if textual errors occured in the logs, or if valgrind exceptions were detected in
    the logs.

    :param ctx: Context
    :param config: Configuration
    """
    try:
        yield
    finally:
        lookup_procs = list()
        log.info('Checking for errors in any valgrind logs...')
        for remote in ctx.cluster.remotes.iterkeys():
            # look at valgrind logs for each node
            proc = remote.run(
                args=[
                    'sudo',
                    'zgrep',
                    '<kind>',
                    run.Raw('/var/log/ceph/valgrind/*'),
                    '/dev/null',  # include a second file so that we always get a filename prefix on the output
                    run.Raw('|'),
                    'sort',
                    run.Raw('|'),
                    'uniq',
                ],
                wait=False,
                check_status=False,
                stdout=StringIO(),
            )
            lookup_procs.append((proc, remote))

        valgrind_exception = None
        for (proc, remote) in lookup_procs:
            proc.wait()
            out = proc.stdout.getvalue()
            for line in out.split('\n'):
                if line == '':
                    continue
                try:
                    (file, kind) = line.split(':')
                except Exception:
                    log.error('failed to split line %s', line)
                    raise
                log.debug('file %s kind %s', file, kind)
                if (file.find('mds') >= 0) and kind.find('Lost') > 0:
                    continue
                log.error('saw valgrind issue %s in %s', kind, file)
                valgrind_exception = Exception('saw valgrind issues')

        if config.get('expect_valgrind_errors'):
            if not valgrind_exception:
                raise Exception('expected valgrind issues and found none')
        else:
            if valgrind_exception:
                raise valgrind_exception


@contextlib.contextmanager
def crush_setup(ctx, config):
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon_remote,) = ctx.cluster.only(first_mon).remotes.iterkeys()

    profile = config.get('crush_tunables', 'default')
    log.info('Setting crush tunables to %s', profile)
    mon_remote.run(
        args=['sudo', 'ceph', 'osd', 'crush', 'tunables', profile])
    yield


@contextlib.contextmanager
def cephfs_setup(ctx, config):
    testdir = teuthology.get_testdir(ctx)
    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)

    first_mon = teuthology.get_first_mon(ctx, config)
    (mon_remote,) = ctx.cluster.only(first_mon).remotes.iterkeys()
    mdss = ctx.cluster.only(teuthology.is_type('mds'))
    # If there are any MDSs, then create a filesystem for them to use
    # Do this last because requires mon cluster to be up and running
    if mdss.remotes:
        log.info('Setting up CephFS filesystem...')

        ceph_fs = Filesystem(ctx)
        if not ceph_fs.legacy_configured():
            ceph_fs.create()

        is_active_mds = lambda role: role.startswith('mds.') and not role.endswith('-s') and role.find('-s-') == -1
        all_roles = [item for remote_roles in mdss.remotes.values() for item in remote_roles]
        num_active = len([r for r in all_roles if is_active_mds(r)])
        mon_remote.run(
            args=[
                'sudo',
                'adjust-ulimits',
                'ceph-coverage',
                coverage_dir,
                'ceph', 'mds', 'set', 'allow_multimds', 'true',
                '--yes-i-really-mean-it'],
	    check_status=False,  # probably old version, upgrade test
        )
        mon_remote.run(args=[
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            coverage_dir,
            'ceph',
            'mds', 'set_max_mds', str(num_active)])

    yield


@contextlib.contextmanager
def cluster(ctx, config):
    """
    Handle the creation and removal of a ceph cluster.

    On startup:
        Create directories needed for the cluster.
        Create remote journals for all osds.
        Create and set keyring.
        Copy the monmap to tht test systems.
        Setup mon nodes.
        Setup mds nodes.
        Mkfs osd nodes.
        Add keyring information to monmaps
        Mkfs mon nodes.

    On exit:
        If errors occured, extract a failure message and store in ctx.summary.
        Unmount all test files and temporary journaling files.
        Save the monitor information and archive all ceph logs.
        Cleanup the keyring setup, and remove all monitor map and data files left over.

    :param ctx: Context
    :param config: Configuration
    """
    if ctx.config.get('use_existing_cluster', False) is True:
        log.info("'use_existing_cluster' is true; skipping cluster creation")
        yield

    testdir = teuthology.get_testdir(ctx)
    cluster_name = config['cluster']
    data_dir = '{tdir}/{cluster}.data'.format(tdir=testdir, cluster=cluster_name)
    log.info('Creating ceph cluster %s...', cluster_name)
    run.wait(
        ctx.cluster.run(
            args=[
                'install', '-d', '-m0755', '--',
                data_dir,
            ],
            wait=False,
        )
    )

    run.wait(
        ctx.cluster.run(
            args=[
                'sudo',
                'install', '-d', '-m0777', '--', '/var/run/ceph',
            ],
            wait=False,
        )
    )

    devs_to_clean = {}
    remote_to_roles_to_devs = {}
    remote_to_roles_to_journals = {}
    osds = ctx.cluster.only(teuthology.is_type('osd', cluster_name))
    for remote, roles_for_host in osds.remotes.iteritems():
        devs = teuthology.get_scratch_devices(remote)
        roles_to_devs = {}
        roles_to_journals = {}
        if config.get('fs'):
            log.info('fs option selected, checking for scratch devs')
            log.info('found devs: %s' % (str(devs),))
            devs_id_map = teuthology.get_wwn_id_map(remote, devs)
            iddevs = devs_id_map.values()
            roles_to_devs = assign_devs(
                teuthology.cluster_roles_of_type(roles_for_host, 'osd', cluster_name), iddevs
            )
            if len(roles_to_devs) < len(iddevs):
                iddevs = iddevs[len(roles_to_devs):]
            devs_to_clean[remote] = []

        if config.get('block_journal'):
            log.info('block journal enabled')
            roles_to_journals = assign_devs(
                teuthology.cluster_roles_of_type(roles_for_host, 'osd', cluster_name), iddevs
            )
            log.info('journal map: %s', roles_to_journals)

        if config.get('tmpfs_journal'):
            log.info('tmpfs journal enabled')
            roles_to_journals = {}
            remote.run(args=['sudo', 'mount', '-t', 'tmpfs', 'tmpfs', '/mnt'])
            for role in teuthology.cluster_roles_of_type(roles_for_host, 'osd', cluster_name):
                tmpfs = '/mnt/' + role
                roles_to_journals[role] = tmpfs
                remote.run(args=['truncate', '-s', '1500M', tmpfs])
            log.info('journal map: %s', roles_to_journals)

        log.info('dev map: %s' % (str(roles_to_devs),))
        remote_to_roles_to_devs[remote] = roles_to_devs
        remote_to_roles_to_journals[remote] = roles_to_journals

    log.info('Generating config...')
    remotes_and_roles = ctx.cluster.remotes.items()
    roles = [role_list for (remote, role_list) in remotes_and_roles]
    ips = [host for (host, port) in
           (remote.ssh.get_transport().getpeername() for (remote, role_list) in remotes_and_roles)]
    conf = teuthology.skeleton_config(ctx, roles=roles, ips=ips, cluster=cluster_name)
    for remote, roles_to_journals in remote_to_roles_to_journals.iteritems():
        for role, journal in roles_to_journals.iteritems():
            name = teuthology.ceph_role(role)
            if name not in conf:
                conf[name] = {}
            conf[name]['osd journal'] = journal
    for section, keys in config['conf'].iteritems():
        for key, value in keys.iteritems():
            log.info("[%s] %s = %s" % (section, key, value))
            if section not in conf:
                conf[section] = {}
            conf[section][key] = value

    if config.get('tmpfs_journal'):
        conf['journal dio'] = False

    ctx.ceph = argparse.Namespace()
    ctx.ceph.conf = conf

    default_keyring = '/etc/ceph/{cluster}.keyring'.format(cluster=cluster_name)
    keyring_path = config.get('keyring_path', default_keyring)

    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)

    firstmon = teuthology.get_first_mon(ctx, config, cluster_name)

    log.info('Setting up %s...' % firstmon)
    ctx.cluster.only(firstmon).run(
        args=[
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            coverage_dir,
            'ceph-authtool',
            '--create-keyring',
            keyring_path,
        ],
    )
    ctx.cluster.only(firstmon).run(
        args=[
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            coverage_dir,
            'ceph-authtool',
            '--gen-key',
            '--name=mon.',
            keyring_path,
        ],
    )
    ctx.cluster.only(firstmon).run(
        args=[
            'sudo',
            'chmod',
            '0644',
            keyring_path,
        ],
    )
    (mon0_remote,) = ctx.cluster.only(firstmon).remotes.keys()
    monmap_path = '{tdir}/{cluster}.monmap'.format(tdir=testdir,
                                                   cluster=cluster_name)
    fsid = teuthology.create_simple_monmap(
        ctx,
        remote=mon0_remote,
        conf=conf,
        path=monmap_path,
    )
    if not 'global' in conf:
        conf['global'] = {}
    conf['global']['fsid'] = fsid

    default_conf_path = '/etc/ceph/{cluster}.conf'.format(cluster=cluster_name)
    conf_path = config.get('conf_path', default_conf_path)
    log.info('Writing %s for FSID %s...' % (conf_path, fsid))
    write_conf(ctx, conf_path)

    log.info('Creating admin key on %s...' % firstmon)
    ctx.cluster.only(firstmon).run(
        args=[
            'sudo',
            'adjust-ulimits',
            'ceph-coverage',
            coverage_dir,
            'ceph-authtool',
            '--gen-key',
            '--name=client.admin',
            '--set-uid=0',
            '--cap', 'mon', 'allow *',
            '--cap', 'osd', 'allow *',
            '--cap', 'mds', 'allow *',
            keyring_path,
        ],
    )

    log.info('Copying monmap to all nodes...')
    keyring = teuthology.get_file(
        remote=mon0_remote,
        path=keyring_path,
    )
    monmap = teuthology.get_file(
        remote=mon0_remote,
        path=monmap_path,
    )

    for rem in ctx.cluster.remotes.iterkeys():
        # copy mon key and initial monmap
        log.info('Sending monmap to node {remote}'.format(remote=rem))
        teuthology.sudo_write_file(
            remote=rem,
            path=keyring_path,
            data=keyring,
            perms='0644'
        )
        teuthology.write_file(
            remote=rem,
            path=monmap_path,
            data=monmap,
        )

    log.info('Setting up mon nodes...')
    mons = ctx.cluster.only(teuthology.is_type('mon', cluster_name))
    osdmap_path = '{tdir}/{cluster}.osdmap'.format(tdir=testdir,
                                                   cluster=cluster_name)
    run.wait(
        mons.run(
            args=[
                'adjust-ulimits',
                'ceph-coverage',
                coverage_dir,
                'osdmaptool',
                '-c', conf_path,
                '--clobber',
                '--createsimple', '{num:d}'.format(
                    num=teuthology.num_instances_of_type(ctx.cluster, 'osd',
                                                         cluster_name),
                ),
                osdmap_path,
                '--pg_bits', '2',
                '--pgp_bits', '4',
            ],
            wait=False,
        ),
    )

    log.info('Setting up mds nodes...')
    mdss = ctx.cluster.only(teuthology.is_type('mds', cluster_name))
    for remote, roles_for_host in mdss.remotes.iteritems():
        for role in teuthology.cluster_roles_of_type(roles_for_host, 'mds',
                                                     cluster_name):
            _, _, id_ = teuthology.split_role(role)
            mds_dir = '/var/lib/ceph/mds/{cluster}-{id}'.format(
                cluster=cluster_name,
                id=id_,
            )
            remote.run(
                args=[
                    'sudo',
                    'mkdir',
                    '-p',
                    mds_dir,
                    run.Raw('&&'),
                    'sudo',
                    'adjust-ulimits',
                    'ceph-coverage',
                    coverage_dir,
                    'ceph-authtool',
                    '--create-keyring',
                    '--gen-key',
                    '--name=mds.{id}'.format(id=id_),
                    mds_dir + '/keyring',
                ],
            )

    cclient.create_keyring(ctx, cluster_name)
    log.info('Running mkfs on osd nodes...')

    if not hasattr(ctx, 'disk_config'):
        ctx.disk_config = argparse.Namespace()
    if not hasattr(ctx.disk_config, 'remote_to_roles_to_dev'):
        ctx.disk_config.remote_to_roles_to_dev = {}
    if not hasattr(ctx.disk_config, 'remote_to_roles_to_journals'):
        ctx.disk_config.remote_to_roles_to_journals = {}
    if not hasattr(ctx.disk_config, 'remote_to_roles_to_dev_mount_options'):
        ctx.disk_config.remote_to_roles_to_dev_mount_options = {}
    if not hasattr(ctx.disk_config, 'remote_to_roles_to_dev_fstype'):
        ctx.disk_config.remote_to_roles_to_dev_fstype = {}

    teuthology.deep_merge(ctx.disk_config.remote_to_roles_to_dev, remote_to_roles_to_devs)
    teuthology.deep_merge(ctx.disk_config.remote_to_roles_to_journals, remote_to_roles_to_journals)

    log.info("ctx.disk_config.remote_to_roles_to_dev: {r}".format(r=str(ctx.disk_config.remote_to_roles_to_dev)))
    for remote, roles_for_host in osds.remotes.iteritems():
        roles_to_devs = remote_to_roles_to_devs[remote]
        roles_to_journals = remote_to_roles_to_journals[remote]

        for role in teuthology.cluster_roles_of_type(roles_for_host, 'osd', cluster_name):
            _, _, id_ = teuthology.split_role(role)
            mnt_point = '/var/lib/ceph/osd/{cluster}-{id}'.format(cluster=cluster_name, id=id_)
            remote.run(
                args=[
                    'sudo',
                    'mkdir',
                    '-p',
                    mnt_point,
                ])
            log.info(str(roles_to_journals))
            log.info(role)
            if roles_to_devs.get(role):
                dev = roles_to_devs[role]
                fs = config.get('fs')
                package = None
                mkfs_options = config.get('mkfs_options')
                mount_options = config.get('mount_options')
                if fs == 'btrfs':
                    # package = 'btrfs-tools'
                    if mount_options is None:
                        mount_options = ['noatime', 'user_subvol_rm_allowed']
                    if mkfs_options is None:
                        mkfs_options = ['-m', 'single',
                                        '-l', '32768',
                                        '-n', '32768']
                if fs == 'xfs':
                    # package = 'xfsprogs'
                    if mount_options is None:
                        mount_options = ['noatime']
                    if mkfs_options is None:
                        mkfs_options = ['-f', '-i', 'size=2048']
                if fs == 'ext4' or fs == 'ext3':
                    if mount_options is None:
                        mount_options = ['noatime', 'user_xattr']

                if mount_options is None:
                    mount_options = []
                if mkfs_options is None:
                    mkfs_options = []
                mkfs = ['mkfs.%s' % fs] + mkfs_options
                log.info('%s on %s on %s' % (mkfs, dev, remote))
                if package is not None:
                    remote.run(
                        args=[
                            'sudo',
                            'apt-get', 'install', '-y', package
                        ],
                        stdout=StringIO(),
                    )

                try:
                    remote.run(args=['yes', run.Raw('|')] + ['sudo'] + mkfs + [dev])
                except run.CommandFailedError:
                    # Newer btfs-tools doesn't prompt for overwrite, use -f
                    if '-f' not in mount_options:
                        mkfs_options.append('-f')
                        mkfs = ['mkfs.%s' % fs] + mkfs_options
                        log.info('%s on %s on %s' % (mkfs, dev, remote))
                    remote.run(args=['yes', run.Raw('|')] + ['sudo'] + mkfs + [dev])

                log.info('mount %s on %s -o %s' % (dev, remote,
                                                   ','.join(mount_options)))
                remote.run(
                    args=[
                        'sudo',
                        'mount',
                        '-t', fs,
                        '-o', ','.join(mount_options),
                        dev,
                        mnt_point,
                    ]
                )
                if not remote in ctx.disk_config.remote_to_roles_to_dev_mount_options:
                    ctx.disk_config.remote_to_roles_to_dev_mount_options[remote] = {}
                ctx.disk_config.remote_to_roles_to_dev_mount_options[remote][role] = mount_options
                if not remote in ctx.disk_config.remote_to_roles_to_dev_fstype:
                    ctx.disk_config.remote_to_roles_to_dev_fstype[remote] = {}
                ctx.disk_config.remote_to_roles_to_dev_fstype[remote][role] = fs
                devs_to_clean[remote].append(mnt_point)

        for role in teuthology.cluster_roles_of_type(roles_for_host, 'osd', cluster_name):
            _, _, id_ = teuthology.split_role(role)
            remote.run(
                args=[
                    'sudo',
                    'MALLOC_CHECK_=3',
                    'adjust-ulimits',
                    'ceph-coverage',
                    coverage_dir,
                    'ceph-osd',
                    '--cluster',
                    cluster_name,
                    '--mkfs',
                    '--mkkey',
                    '-i', id_,
                    '--monmap', monmap_path,
                ],
            )

    log.info('Reading keys from all nodes...')
    keys_fp = StringIO()
    keys = []
    for remote, roles_for_host in ctx.cluster.remotes.iteritems():
        for type_ in ['mds', 'osd']:
            for role in teuthology.cluster_roles_of_type(roles_for_host, type_, cluster_name):
                _, _, id_ = teuthology.split_role(role)
                data = teuthology.get_file(
                    remote=remote,
                    path='/var/lib/ceph/{type}/{cluster}-{id}/keyring'.format(
                        type=type_,
                        id=id_,
                        cluster=cluster_name,
                    ),
                    sudo=True,
                )
                keys.append((type_, id_, data))
                keys_fp.write(data)
    for remote, roles_for_host in ctx.cluster.remotes.iteritems():
        for role in teuthology.cluster_roles_of_type(roles_for_host, 'client', cluster_name):
            _, _, id_ = teuthology.split_role(role)
            data = teuthology.get_file(
                remote=remote,
                path='/etc/ceph/{cluster}.client.{id}.keyring'.format(id=id_, cluster=cluster_name)
            )
            keys.append(('client', id_, data))
            keys_fp.write(data)

    log.info('Adding keys to all mons...')
    writes = mons.run(
        args=[
            'sudo', 'tee', '-a',
            keyring_path,
        ],
        stdin=run.PIPE,
        wait=False,
        stdout=StringIO(),
    )
    keys_fp.seek(0)
    teuthology.feed_many_stdins_and_close(keys_fp, writes)
    run.wait(writes)
    for type_, id_, data in keys:
        run.wait(
            mons.run(
                args=[
                         'sudo',
                         'adjust-ulimits',
                         'ceph-coverage',
                         coverage_dir,
                         'ceph-authtool',
                         keyring_path,
                         '--name={type}.{id}'.format(
                             type=type_,
                             id=id_,
                         ),
                     ] + list(teuthology.generate_caps(type_)),
                wait=False,
            ),
        )

    log.info('Running mkfs on mon nodes...')
    for remote, roles_for_host in mons.remotes.iteritems():
        for role in teuthology.cluster_roles_of_type(roles_for_host, 'mon', cluster_name):
            _, _, id_ = teuthology.split_role(role)
            remote.run(
                args=[
                    'sudo',
                    'mkdir',
                    '-p',
                    '/var/lib/ceph/mon/{cluster}-{id}'.format(id=id_, cluster=cluster_name),
                ],
            )
            remote.run(
                args=[
                    'sudo',
                    'adjust-ulimits',
                    'ceph-coverage',
                    coverage_dir,
                    'ceph-mon',
                    '--cluster', cluster_name,
                    '--mkfs',
                    '-i', id_,
                    '--monmap', monmap_path,
                    '--osdmap', osdmap_path,
                    '--keyring', keyring_path,
                ],
            )

    run.wait(
        mons.run(
            args=[
                'rm',
                '--',
                monmap_path,
                osdmap_path,
            ],
            wait=False,
        ),
    )

    try:
        yield
    except Exception:
        # we need to know this below
        ctx.summary['success'] = False
        raise
    finally:
        (mon0_remote,) = ctx.cluster.only(firstmon).remotes.keys()

        log.info('Checking cluster log for badness...')

        def first_in_ceph_log(pattern, excludes):
            """
            Find the first occurence of the pattern specified in the Ceph log,
            Returns None if none found.

            :param pattern: Pattern scanned for.
            :param excludes: Patterns to ignore.
            :return: First line of text (or None if not found)
            """
            args = [
                'sudo',
                'egrep', pattern,
                '/var/log/ceph/{cluster}.log'.format(cluster=cluster_name),
            ]
            for exclude in excludes:
                args.extend([run.Raw('|'), 'egrep', '-v', exclude])
            args.extend([
                run.Raw('|'), 'head', '-n', '1',
            ])
            r = mon0_remote.run(
                stdout=StringIO(),
                args=args,
            )
            stdout = r.stdout.getvalue()
            if stdout != '':
                return stdout
            return None

        if first_in_ceph_log('\[ERR\]|\[WRN\]|\[SEC\]',
                             config['log_whitelist']) is not None:
            log.warning('Found errors (ERR|WRN|SEC) in cluster log')
            ctx.summary['success'] = False
            # use the most severe problem as the failure reason
            if 'failure_reason' not in ctx.summary:
                for pattern in ['\[SEC\]', '\[ERR\]', '\[WRN\]']:
                    match = first_in_ceph_log(pattern, config['log_whitelist'])
                    if match is not None:
                        ctx.summary['failure_reason'] = \
                            '"{match}" in cluster log'.format(
                                match=match.rstrip('\n'),
                            )
                        break

        for remote, dirs in devs_to_clean.iteritems():
            for dir_ in dirs:
                log.info('Unmounting %s on %s' % (dir_, remote))
                try:
                    remote.run(
                        args=[
                            'sync',
                            run.Raw('&&'),
                            'sudo',
                            'umount',
                            '-f',
                            dir_
                        ]
                    )
                except Exception as e:
                    remote.run(args=[
                        'sudo',
                        run.Raw('PATH=/usr/sbin:$PATH'),
                        'lsof',
                        run.Raw(';'),
                        'ps', 'auxf',
                    ])
                    raise e

        if config.get('tmpfs_journal'):
            log.info('tmpfs journal enabled - unmounting tmpfs at /mnt')
            for remote, roles_for_host in osds.remotes.iteritems():
                remote.run(
                    args=['sudo', 'umount', '-f', '/mnt'],
                    check_status=False,
                )

        if ctx.archive is not None and \
                not (ctx.config.get('archive-on-error') and ctx.summary['success']):

            # archive mon data, too
            log.info('Archiving mon data...')
            path = os.path.join(ctx.archive, 'data')
            os.makedirs(path)
            for remote, roles in mons.remotes.iteritems():
                for role in roles:
                    is_mon = teuthology.is_type('mon', cluster_name)
                    if is_mon(role):
                        teuthology.pull_directory_tarball(
                            remote,
                            '/var/lib/ceph/mon',
                            path + '/' + role + '.tgz')

        log.info('Cleaning ceph cluster...')
        run.wait(
            ctx.cluster.run(
                args=[
                    'sudo',
                    'rm',
                    '-rf',
                    '--',
                    conf_path,
                    keyring_path,
                    data_dir,
                    monmap_path,
                    osdmap_path,
                    run.Raw('{tdir}/../*.pid'.format(tdir=testdir)),
                ],
                wait=False,
            ),
        )


def get_all_pg_info(rem_site, testdir):
    """
    Get the results of a ceph pg dump
    """
    info = rem_site.run(args=[
        'sudo',
        'adjust-ulimits',
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'ceph', 'pg', 'dump',
        '--format', 'json'], stdout=StringIO())
    all_info = json.loads(info.stdout.getvalue())
    return all_info['pg_stats']


def osd_scrub_pgs(ctx, config):
    """
    Scrub pgs when we exit.

    First make sure all pgs are active and clean.
    Next scrub all osds.
    Then periodically check until all pgs have scrub time stamps that
    indicate the last scrub completed.  Time out if no progess is made
    here after two minutes.
    """
    retries = 12
    delays = 10
    vlist = ctx.cluster.remotes.values()
    testdir = teuthology.get_testdir(ctx)
    rem_site = ctx.cluster.remotes.keys()[0]
    all_clean = False
    for _ in range(0, retries):
        stats = get_all_pg_info(rem_site, testdir)
        states = [stat['state'] for stat in stats]
        if len(set(states)) == 1 and states[0] == 'active+clean':
            all_clean = True
            break
        log.info("Waiting for all osds to be active and clean.")
        time.sleep(delays)
    if not all_clean:
        log.info("Scrubbing terminated -- not all pgs were active and clean.")
        return
    check_time_now = time.localtime()
    time.sleep(1)
    for slists in vlist:
        for role in slists:
            if role.startswith('osd.'):
                log.info("Scrubbing osd {osd}".format(osd=role))
                rem_site.run(args=[
                    'sudo',
                    'adjust-ulimits',
                    'ceph-coverage',
                    '{tdir}/archive/coverage'.format(tdir=testdir),
                    'ceph', 'osd', 'deep-scrub', role])
    prev_good = 0
    gap_cnt = 0
    loop = True
    while loop:
        stats = get_all_pg_info(rem_site, testdir)
        timez = [stat['last_scrub_stamp'] for stat in stats]
        loop = False
        thiscnt = 0
        for tmval in timez:
            pgtm = time.strptime(tmval[0:tmval.find('.')], '%Y-%m-%d %H:%M:%S')
            if pgtm > check_time_now:
                thiscnt += 1
            else:
                loop = True
        if thiscnt > prev_good:
            prev_good = thiscnt
            gap_cnt = 0
        else:
            gap_cnt += 1
            if gap_cnt > retries:
                log.info('Exiting scrub checking -- not all pgs scrubbed.')
                return
        if loop:
            log.info('Still waiting for all pgs to be scrubbed.')
            time.sleep(delays)


@contextlib.contextmanager
def run_daemon(ctx, config, type_):
    """
    Run daemons for a role type.  Handle the startup and termination of a a daemon.
    On startup -- set coverages, cpu_profile, valgrind values for all remotes,
    and a max_mds value for one mds.
    On cleanup -- Stop all existing daemons of this type.

    :param ctx: Context
    :param config: Configuration
    :paran type_: Role type
    """
    log.info('Starting %s daemons...' % type_)
    testdir = teuthology.get_testdir(ctx)
    daemons = ctx.cluster.only(teuthology.is_type(type_))

    # check whether any daemons if this type are configured
    if daemons is None:
        return
    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)

    daemon_signal = 'kill'
    if config.get('coverage') or config.get('valgrind') is not None:
        daemon_signal = 'term'

    for remote, roles_for_host in daemons.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, type_):
            name = '%s.%s' % (type_, id_)

            run_cmd = [
                'sudo',
                'adjust-ulimits',
                'ceph-coverage',
                coverage_dir,
                'daemon-helper',
                daemon_signal,
            ]
            run_cmd_tail = [
                'ceph-%s' % (type_),
                '-f',
                '-i', id_]

            if type_ in config.get('cpu_profile', []):
                profile_path = '/var/log/ceph/profiling-logger/%s.%s.prof' % (type_, id_)
                run_cmd.extend(['env', 'CPUPROFILE=%s' % profile_path])

            if config.get('valgrind') is not None:
                valgrind_args = None
                if type_ in config['valgrind']:
                    valgrind_args = config['valgrind'][type_]
                if name in config['valgrind']:
                    valgrind_args = config['valgrind'][name]
                run_cmd = teuthology.get_valgrind_args(testdir, name,
                                                       run_cmd,
                                                       valgrind_args)

            run_cmd.extend(run_cmd_tail)

            ctx.daemons.add_daemon(remote, type_, id_,
                                   args=run_cmd,
                                   logger=log.getChild(name),
                                   stdin=run.PIPE,
                                   wait=False,
                                   )

    try:
        yield
    finally:
        teuthology.stop_daemons_of_type(ctx, type_)


def healthy(ctx, config):
    """
    Wait for all osd's to be up, and for the ceph health monitor to return HEALTH_OK.

    :param ctx: Context
    :param config: Configuration
    """
    log.info('Waiting until ceph is healthy...')
    firstmon = teuthology.get_first_mon(ctx, config)
    (mon0_remote,) = ctx.cluster.only(firstmon).remotes.keys()
    teuthology.wait_until_osds_up(
        ctx,
        cluster=ctx.cluster,
        remote=mon0_remote
    )
    teuthology.wait_until_healthy(
        ctx,
        remote=mon0_remote,
    )

    if ctx.cluster.only(teuthology.is_type('mds')).remotes:
        # Some MDSs exist, wait for them to be healthy
        ceph_fs = Filesystem(ctx)
        ceph_fs.wait_for_daemons(timeout=300)


def wait_for_osds_up(ctx, config):
    """
    Wait for all osd's to come up.

    :param ctx: Context
    :param config: Configuration
    """
    log.info('Waiting until ceph osds are all up...')
    firstmon = teuthology.get_first_mon(ctx, config)
    (mon0_remote,) = ctx.cluster.only(firstmon).remotes.keys()
    teuthology.wait_until_osds_up(
        ctx,
        cluster=ctx.cluster,
        remote=mon0_remote
    )


def wait_for_mon_quorum(ctx, config):
    """
    Check renote ceph status until all monitors are up.

    :param ctx: Context
    :param config: Configuration
    """

    assert isinstance(config, list)
    firstmon = teuthology.get_first_mon(ctx, config)
    (remote,) = ctx.cluster.only(firstmon).remotes.keys()
    while True:
        r = remote.run(
            args=[
                'sudo',
                'ceph',
                'quorum_status',
            ],
            stdout=StringIO(),
            logger=log.getChild('quorum_status'),
        )
        j = json.loads(r.stdout.getvalue())
        q = j.get('quorum_names', [])
        log.debug('Quorum: %s', q)
        if sorted(q) == sorted(config):
            break
        time.sleep(1)


def created_pool(ctx, config):
    """
    Add new pools to the dictionary of pools that the ceph-manager
    knows about.
    """
    for new_pool in config:
        if new_pool not in ctx.manager.pools:
            ctx.manager.pools[new_pool] = ctx.manager.get_pool_property(
                new_pool, 'pg_num')


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

    daemons = ctx.daemons.resolve_role_list(config.get('daemons', None), CEPH_ROLE_TYPES)
    for i in daemons:
        type_ = i.split('.', 1)[0]
        id_ = i.split('.', 1)[1]
        ctx.daemons.get_daemon(type_, id_).restart()

    if config.get('wait-for-healthy', True):
        healthy(ctx=ctx, config=None)
    if config.get('wait-for-osds-up', False):
        wait_for_osds_up(ctx=ctx, config=None)
    yield


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

    daemons = ctx.daemons.resolve_role_list(config.get('daemons', None), CEPH_ROLE_TYPES)
    for i in daemons:
        type_ = i.split('.', 1)[0]
        id_ = i.split('.', 1)[1]
        ctx.daemons.get_daemon(type_, id_).stop()

    yield


@contextlib.contextmanager
def wait_for_failure(ctx, config):
    """
    Wait for a failure of a ceph daemon

    For example::
      tasks:
      - ceph.wait_for_failure: [mds.*]

      tasks:
      - ceph.wait_for_failure: [osd.0, osd.2]

      tasks:
      - ceph.wait_for_failure:
          daemons: [osd.0, osd.2]

    """
    if config is None:
        config = {}
    elif isinstance(config, list):
        config = {'daemons': config}

    daemons = ctx.daemons.resolve_role_list(config.get('daemons', None), CEPH_ROLE_TYPES)
    for i in daemons:
        type_ = i.split('.', 1)[0]
        id_ = i.split('.', 1)[1]
        try:
            ctx.daemons.get_daemon(type_, id_).wait()
        except:
            log.info('Saw expected daemon failure.  Continuing.')
            pass
        else:
            raise RuntimeError('daemon %s did not fail' % i)

    yield


def validate_config(ctx, config):
    """
    Perform some simple validation on task configuration.
    Raises exceptions.ConfigError if an error is found.
    """
    # check for osds from multiple clusters on the same host
    for remote, roles_for_host in ctx.cluster.remotes.items():
        last_cluster = None
        last_role = None
        for role in roles_for_host:
            role_cluster, role_type, _ = teuthology.split_role(role)
            if role_type != 'osd':
                continue
            if last_cluster and last_cluster != role_cluster:
                msg = "Host should not have osds (%s and %s) from multiple clusters" % (
                    last_role, role)
                raise exceptions.ConfigError(msg)
            last_cluster = role_cluster
            last_role = role


@contextlib.contextmanager
def task(ctx, config):
    """
    Set up and tear down a Ceph cluster.

    For example::

        tasks:
        - ceph:
        - interactive:

    You can also specify what branch to run::

        tasks:
        - ceph:
            branch: foo

    Or a tag::

        tasks:
        - ceph:
            tag: v0.42.13

    Or a sha1::

        tasks:
        - ceph:
            sha1: 1376a5ab0c89780eab39ffbbe436f6a6092314ed

    Or a local source dir::

        tasks:
        - ceph:
            path: /home/sage/ceph

    To capture code coverage data, use::

        tasks:
        - ceph:
            coverage: true

    To use btrfs, ext4, or xfs on the target's scratch disks, use::

        tasks:
        - ceph:
            fs: xfs
            mkfs_options: [-b,size=65536,-l,logdev=/dev/sdc1]
            mount_options: [nobarrier, inode64]

    Note, this will cause the task to check the /scratch_devs file on each node
    for available devices.  If no such file is found, /dev/sdb will be used.

    To run some daemons under valgrind, include their names
    and the tool/args to use in a valgrind section::

        tasks:
        - ceph:
          valgrind:
            mds.1: --tool=memcheck
            osd.1: [--tool=memcheck, --leak-check=no]

    Those nodes which are using memcheck or valgrind will get
    checked for bad results.

    To adjust or modify config options, use::

        tasks:
        - ceph:
            conf:
              section:
                key: value

    For example::

        tasks:
        - ceph:
            conf:
              mds.0:
                some option: value
                other key: other value
              client.0:
                debug client: 10
                debug ms: 1

    By default, the cluster log is checked for errors and warnings,
    and the run marked failed if any appear. You can ignore log
    entries by giving a list of egrep compatible regexes, i.e.:

        tasks:
        - ceph:
            log-whitelist: ['foo.*bar', 'bad message']

    To run multiple ceph clusters, use multiple ceph tasks, and roles
    with a cluster name prefix, e.g. cluster1.client.0. Roles with no
    cluster use the default cluster name, 'ceph'. OSDs from separate
    clusters must be on separate hosts. Clients and non-osd daemons
    from multiple clusters may be colocated. For each cluster, add an
    instance of the ceph task with the cluster name specified, e.g.::

        roles:
        - [mon.a, osd.0, osd.1]
        - [backup.mon.a, backup.osd.0, backup.osd.1]
        - [client.0, backup.client.0]
        tasks:
        - ceph:
            cluster: ceph
        - ceph:
            cluster: backup

    :param ctx: Context
    :param config: Configuration

    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        "task ceph only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('ceph', {}))

    ctx.daemons = DaemonGroup()

    testdir = teuthology.get_testdir(ctx)
    if config.get('coverage'):
        coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)
        log.info('Creating coverage directory...')
        run.wait(
            ctx.cluster.run(
                args=[
                    'install', '-d', '-m0755', '--',
                    coverage_dir,
                ],
                wait=False,
            )
        )

    if 'cluster' not in config:
        config['cluster'] = 'ceph'

    validate_config(ctx, config)

    with contextutil.nested(
            lambda: ceph_log(ctx=ctx, config=None),
            lambda: valgrind_post(ctx=ctx, config=config),
            lambda: cluster(ctx=ctx, config=dict(
                conf=config.get('conf', {}),
                fs=config.get('fs', None),
                mkfs_options=config.get('mkfs_options', None),
                mount_options=config.get('mount_options', None),
                block_journal=config.get('block_journal', None),
                tmpfs_journal=config.get('tmpfs_journal', None),
                log_whitelist=config.get('log-whitelist', []),
                cpu_profile=set(config.get('cpu_profile', []),),
                cluster=config['cluster'],
            )),
            lambda: run_daemon(ctx=ctx, config=config, type_='mon'),
            lambda: crush_setup(ctx=ctx, config=config),
            lambda: run_daemon(ctx=ctx, config=config, type_='osd'),
            lambda: cephfs_setup(ctx=ctx, config=config),
            lambda: run_daemon(ctx=ctx, config=config, type_='mds'),
    ):
        try:
            if config.get('wait-for-healthy', True):
                healthy(ctx=ctx, config=None)
            first_mon = teuthology.get_first_mon(ctx, config)
            (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
            ctx.manager = CephManager(
                mon,
                ctx=ctx,
                logger=log.getChild('ceph_manager'),
            )
            yield
        finally:
            if config.get('wait-for-scrub', True):
                osd_scrub_pgs(ctx, config)
