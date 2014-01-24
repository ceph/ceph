"""
Execute ceph-deploy as a task
"""
from cStringIO import StringIO

import contextlib
import os
import time
import logging

from teuthology import misc as teuthology
from teuthology import contextutil
from ..config import config as teuth_config
import ceph as ceph_fn
from ..orchestra import run

log = logging.getLogger(__name__)


@contextlib.contextmanager
def download_ceph_deploy(ctx, config):
    """
    Downloads ceph-deploy from the ceph.com git mirror and (by default)
    switches to the master branch. If the `ceph-deploy-branch` is specified, it
    will use that instead.
    """
    log.info('Downloading ceph-deploy...')
    testdir = teuthology.get_testdir(ctx)
    ceph_admin = teuthology.get_first_mon(ctx, config)
    default_cd_branch = {'ceph-deploy-branch': 'master'}
    ceph_deploy_branch = config.get(
        'ceph-deploy',
        default_cd_branch).get('ceph-deploy-branch')

    ctx.cluster.only(ceph_admin).run(
        args=[
            'git', 'clone', '-b', ceph_deploy_branch,
            teuth_config.ceph_git_base_url + 'ceph-deploy.git',
            '{tdir}/ceph-deploy'.format(tdir=testdir),
            ],
        )
    ctx.cluster.only(ceph_admin).run(
        args=[
            'cd',
            '{tdir}/ceph-deploy'.format(tdir=testdir),
            run.Raw('&&'),
            './bootstrap',
            ],
        )

    try:
        yield
    finally:
        log.info('Removing ceph-deploy ...')
        ctx.cluster.only(ceph_admin).run(
            args=[
                'rm',
                '-rf',
                '{tdir}/ceph-deploy'.format(tdir=testdir),
                ],
            )


def is_healthy(ctx, config):
    """Wait until a Ceph cluster is healthy."""
    testdir = teuthology.get_testdir(ctx)
    ceph_admin = teuthology.get_first_mon(ctx, config)
    (remote,) = ctx.cluster.only(ceph_admin).remotes.keys()
    max_tries = 90  # 90 tries * 10 secs --> 15 minutes
    tries = 0
    while True:
        tries += 1
        if tries >= max_tries:
            msg = "ceph health was unable to get 'HEALTH_OK' after waiting 15 minutes"
            raise RuntimeError(msg)

        r = remote.run(
            args=[
                'cd',
                '{tdir}'.format(tdir=testdir),
                run.Raw('&&'),
                'sudo', 'ceph',
                'health',
                ],
            stdout=StringIO(),
            logger=log.getChild('health'),
            )
        out = r.stdout.getvalue()
        log.debug('Ceph health: %s', out.rstrip('\n'))
        if out.split(None, 1)[0] == 'HEALTH_OK':
            break
        time.sleep(10)

def get_nodes_using_roles(ctx, config, role):
    """Extract the names of nodes that match a given role from a cluster"""
    newl = []
    for _remote, roles_for_host in ctx.cluster.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, role):
            rem = _remote
            if role == 'mon':
                req1 = str(rem).split('@')[-1]
            else:
                req = str(rem).split('.')[0]
                req1 = str(req).split('@')[1]
            newl.append(req1)
    return newl

def get_dev_for_osd(ctx, config):
    """Get a list of all osd device names."""
    osd_devs = []
    for remote, roles_for_host in ctx.cluster.remotes.iteritems():
        host = remote.name.split('@')[-1]
        shortname = host.split('.')[0]
        devs = teuthology.get_scratch_devices(remote)
        num_osd_per_host = list(teuthology.roles_of_type(roles_for_host, 'osd'))
        num_osds = len(num_osd_per_host)
        assert num_osds <= len(devs), 'fewer disks than osds on ' + shortname
        for dev in devs[:num_osds]:
            dev_short = dev.split('/')[-1]
            osd_devs.append('{host}:{dev}'.format(host=shortname, dev=dev_short))
    return osd_devs

def get_all_nodes(ctx, config):
    """Return a string of node names separated by blanks"""
    nodelist = []
    for t, k in ctx.config['targets'].iteritems():
        host = t.split('@')[-1]
        simple_host = host.split('.')[0]
        nodelist.append(simple_host)
    nodelist = " ".join(nodelist)
    return nodelist

def execute_ceph_deploy(ctx, config, cmd):
    """Remotely execute a ceph_deploy command"""
    testdir = teuthology.get_testdir(ctx)
    ceph_admin = teuthology.get_first_mon(ctx, config)
    exec_cmd = cmd
    (remote,) = ctx.cluster.only(ceph_admin).remotes.iterkeys()
    proc = remote.run(
        args = [
            'cd',
            '{tdir}/ceph-deploy'.format(tdir=testdir),
            run.Raw('&&'),
            run.Raw(exec_cmd),
            ],
            check_status=False,
        )
    exitstatus = proc.exitstatus
    return exitstatus

@contextlib.contextmanager
def build_ceph_cluster(ctx, config):
    """Build a ceph cluster"""
    log.info('Building ceph cluster using ceph-deploy...')
    testdir = teuthology.get_testdir(ctx)
    ceph_branch = None
    if config.get('branch') is not None:
        cbranch = config.get('branch')
        for var, val in cbranch.iteritems():
            if var == 'testing':
                ceph_branch = '--{var}'.format(var=var)
            ceph_branch = '--{var}={val}'.format(var=var, val=val)
    node_dev_list = []
    all_nodes = get_all_nodes(ctx, config)
    mds_nodes = get_nodes_using_roles(ctx, config, 'mds')
    mds_nodes = " ".join(mds_nodes)
    mon_node = get_nodes_using_roles(ctx, config, 'mon')
    mon_nodes = " ".join(mon_node)
    new_mon = './ceph-deploy new'+" "+mon_nodes
    install_nodes = './ceph-deploy install '+ceph_branch+" "+all_nodes
    purge_nodes = './ceph-deploy purge'+" "+all_nodes
    purgedata_nodes = './ceph-deploy purgedata'+" "+all_nodes
    mon_hostname = mon_nodes.split(' ')[0]
    mon_hostname = str(mon_hostname)
    gather_keys = './ceph-deploy gatherkeys'+" "+mon_hostname
    deploy_mds = './ceph-deploy mds create'+" "+mds_nodes
    no_of_osds = 0

    if mon_nodes is None:
        raise RuntimeError("no monitor nodes in the config file")

    estatus_new = execute_ceph_deploy(ctx, config, new_mon)
    if estatus_new != 0:
        raise RuntimeError("ceph-deploy: new command failed")

    log.info('adding config inputs...')
    testdir = teuthology.get_testdir(ctx)
    conf_path = '{tdir}/ceph-deploy/ceph.conf'.format(tdir=testdir)
    first_mon = teuthology.get_first_mon(ctx, config)
    (remote,) = ctx.cluster.only(first_mon).remotes.keys()

    lines = None
    if config.get('conf') is not None:
        confp = config.get('conf')
        for section, keys in confp.iteritems():
            lines = '[{section}]\n'.format(section=section)
            teuthology.append_lines_to_file(remote, conf_path, lines, sudo=True)
            for key, value in keys.iteritems():
                log.info("[%s] %s = %s" % (section, key, value))
                lines = '{key} = {value}\n'.format(key=key, value=value)
                teuthology.append_lines_to_file(remote, conf_path, lines, sudo=True)

    estatus_install = execute_ceph_deploy(ctx, config, install_nodes)
    if estatus_install != 0:
        raise RuntimeError("ceph-deploy: Failed to install ceph")

    mon_no = None
    mon_no = config.get('mon_initial_members')
    if mon_no is not None:
        i = 0
        mon1 = []
        while(i < mon_no):
            mon1.append(mon_node[i])
            i = i + 1
        initial_mons = " ".join(mon1)
        for k in range(mon_no, len(mon_node)):
            mon_create_nodes = './ceph-deploy mon create'+" "+initial_mons+" "+mon_node[k]
            estatus_mon = execute_ceph_deploy(ctx, config, mon_create_nodes)
            if estatus_mon != 0:
                raise RuntimeError("ceph-deploy: Failed to create monitor")
    else:
        mon_create_nodes = './ceph-deploy mon create-initial'
        estatus_mon = execute_ceph_deploy(ctx, config, mon_create_nodes)
        if estatus_mon != 0:
            raise RuntimeError("ceph-deploy: Failed to create monitors")

    estatus_gather = execute_ceph_deploy(ctx, config, gather_keys)
    max_gather_tries = 90
    gather_tries = 0
    while (estatus_gather != 0):
        gather_tries += 1
        if gather_tries >= max_gather_tries:
            msg = 'ceph-deploy was not able to gatherkeys after 15 minutes'
            raise RuntimeError(msg)
        estatus_gather = execute_ceph_deploy(ctx, config, gather_keys)
        time.sleep(10)

    if mds_nodes:
        estatus_mds = execute_ceph_deploy(ctx, config, deploy_mds)
        if estatus_mds != 0:
            raise RuntimeError("ceph-deploy: Failed to deploy mds")

    if config.get('test_mon_destroy') is not None:
        for d in range(1, len(mon_node)):
            mon_destroy_nodes = './ceph-deploy mon destroy'+" "+mon_node[d]
            estatus_mon_d = execute_ceph_deploy(ctx, config, mon_destroy_nodes)
            if estatus_mon_d != 0:
                raise RuntimeError("ceph-deploy: Failed to delete monitor")

    node_dev_list = get_dev_for_osd(ctx, config)
    for d in node_dev_list:
        osd_create_cmds = './ceph-deploy osd create --zap-disk'+" "+d
        estatus_osd = execute_ceph_deploy(ctx, config, osd_create_cmds)
        if estatus_osd == 0:
            log.info('successfully created osd')
            no_of_osds += 1
        else:
            zap_disk = './ceph-deploy disk zap'+" "+d
            execute_ceph_deploy(ctx, config, zap_disk)
            estatus_osd = execute_ceph_deploy(ctx, config, osd_create_cmds)
            if estatus_osd == 0:
                log.info('successfully created osd')
                no_of_osds += 1
            else:
                raise RuntimeError("ceph-deploy: Failed to create osds")

    if config.get('wait-for-healthy', True) and no_of_osds >= 2:
        is_healthy(ctx=ctx, config=None)

        log.info('Setting up client nodes...')
        conf_path = '/etc/ceph/ceph.conf'
        admin_keyring_path = '/etc/ceph/ceph.client.admin.keyring'
        first_mon = teuthology.get_first_mon(ctx, config)
        (mon0_remote,) = ctx.cluster.only(first_mon).remotes.keys()
        conf_data = teuthology.get_file(
            remote=mon0_remote,
            path=conf_path,
            sudo=True,
            )
        admin_keyring = teuthology.get_file(
            remote=mon0_remote,
            path=admin_keyring_path,
            sudo=True,
            )

        clients = ctx.cluster.only(teuthology.is_type('client'))
        for remot, roles_for_host in clients.remotes.iteritems():
            for id_ in teuthology.roles_of_type(roles_for_host, 'client'):
                client_keyring = '/etc/ceph/ceph.client.{id}.keyring'.format(id=id_)
                mon0_remote.run(
                    args=[
                        'cd',
                        '{tdir}'.format(tdir=testdir),
                        run.Raw('&&'),
                        'sudo','bash','-c',
                        run.Raw('"'),'ceph',
                        'auth',
                        'get-or-create',
                        'client.{id}'.format(id=id_),
                        'mds', 'allow',
                        'mon', 'allow *',
                        'osd', 'allow *',
                        run.Raw('>'),
                        client_keyring,
                        run.Raw('"'),
                        ],
                    )
                key_data = teuthology.get_file(
                    remote=mon0_remote,
                    path=client_keyring,
                    sudo=True,
                    )
                teuthology.sudo_write_file(
                    remote=remot,
                    path=client_keyring,
                    data=key_data,
                    perms='0644'
                )
                teuthology.sudo_write_file(
                    remote=remot,
                    path=admin_keyring_path,
                    data=admin_keyring,
                    perms='0644'
                )
                teuthology.sudo_write_file(
                    remote=remot,
                    path=conf_path,
                    data=conf_data,
                    perms='0644'
                )
    else:
        raise RuntimeError("The cluster is NOT operational due to insufficient OSDs")

    try:
        yield

    finally:
        log.info('Stopping ceph...')
        ctx.cluster.run(args=[
                'sudo', 'stop', 'ceph-all',
                run.Raw('||'),
                'sudo', 'service', 'ceph', 'stop'
                ])

        if ctx.archive is not None:
            # archive mon data, too
            log.info('Archiving mon data...')
            path = os.path.join(ctx.archive, 'data')
            os.makedirs(path)
            mons = ctx.cluster.only(teuthology.is_type('mon'))
            for remote, roles in mons.remotes.iteritems():
                for role in roles:
                    if role.startswith('mon.'):
                        teuthology.pull_directory_tarball(
                            remote,
                            '/var/lib/ceph/mon',
                            path + '/' + role + '.tgz')

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

        log.info('Purging package...')
        execute_ceph_deploy(ctx, config, purge_nodes)
        log.info('Purging data...')
        execute_ceph_deploy(ctx, config, purgedata_nodes)

@contextlib.contextmanager
def task(ctx, config):
    """
    Set up and tear down a Ceph cluster.

    For example::

        tasks:
        - install:
             extras: yes
        - ssh_keys:
        - ceph-deploy:
             branch:
                stable: bobtail
             mon_initial_members: 1

        tasks:
        - install:
             extras: yes
        - ssh_keys:
        - ceph-deploy:
             branch:
                dev: master
             conf:
                mon:
                   debug mon = 20

        tasks:
        - install:
             extras: yes
        - ssh_keys:
        - ceph-deploy:
             branch:
                testing:
    """
    if config is None:
        config = {}

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('ceph-deploy', {}))

    assert isinstance(config, dict), \
        "task ceph-deploy only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('ceph-deploy', {}))

    if config.get('branch') is not None:
        assert isinstance(config['branch'], dict), 'branch must be a dictionary'

    with contextutil.nested(
         lambda: ceph_fn.ship_utilities(ctx=ctx, config=None),
         lambda: download_ceph_deploy(ctx=ctx, config=config),
         lambda: build_ceph_cluster(ctx=ctx, config=dict(
                 conf=config.get('conf', {}),
                 branch=config.get('branch',{}),
                 mon_initial_members=config.get('mon_initial_members', None),
                 test_mon_destroy=config.get('test_mon_destroy', None),
                 )),
        ):
        yield
