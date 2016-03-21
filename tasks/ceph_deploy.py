"""
Execute ceph-deploy as a task
"""
from cStringIO import StringIO

import contextlib
import os
import time
import logging
import traceback

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.config import config as teuth_config
from teuthology.task import install as install_fn
from teuthology.orchestra import run
from tasks.cephfs.filesystem import Filesystem

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
    ceph_admin = ctx.cluster.only(teuthology.get_first_mon(ctx, config))
    ceph_deploy_branch = config.get('ceph-deploy-branch', 'master')

    ceph_admin.run(
        args=[
            'git', 'clone', '-b', ceph_deploy_branch,
            teuth_config.ceph_git_base_url + 'ceph-deploy.git',
            '{tdir}/ceph-deploy'.format(tdir=testdir),
        ],
    )
    ceph_admin.run(
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
        ceph_admin.run(
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
            remote.run(
                args=[
                    'cd',
                    '{tdir}'.format(tdir=testdir),
                    run.Raw('&&'),
                    'sudo', 'ceph',
                    'report',
                ],
            )
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
        log.info('Ceph health: %s', out.rstrip('\n'))
        if out.split(None, 1)[0] == 'HEALTH_OK':
            break
        time.sleep(10)


def get_nodes_using_role(ctx, target_role):
    """
    Extract the names of nodes that match a given role from a cluster, and modify the
    cluster's service IDs to match the resulting node-based naming scheme that ceph-deploy
    uses, such that if "mon.a" is on host "foo23", it'll be renamed to "mon.foo23".
    """

    # Nodes containing a service of the specified role
    nodes_of_interest = []

    # Prepare a modified version of cluster.remotes with ceph-deploy-ized names
    modified_remotes = {}

    for _remote, roles_for_host in ctx.cluster.remotes.iteritems():
        modified_remotes[_remote] = []
        for svc_id in roles_for_host:
            if svc_id.startswith("{0}.".format(target_role)):
                fqdn = str(_remote).split('@')[-1]
                nodename = str(str(_remote).split('.')[0]).split('@')[1]
                if target_role == 'mon':
                    nodes_of_interest.append(fqdn)
                else:
                    nodes_of_interest.append(nodename)

                modified_remotes[_remote].append(
                    "{0}.{1}".format(target_role, nodename))
            else:
                modified_remotes[_remote].append(svc_id)

    ctx.cluster.remotes = modified_remotes

    return nodes_of_interest


def get_dev_for_osd(ctx, config):
    """Get a list of all osd device names."""
    osd_devs = []
    for remote, roles_for_host in ctx.cluster.remotes.iteritems():
        host = remote.name.split('@')[-1]
        shortname = host.split('.')[0]
        devs = teuthology.get_scratch_devices(remote)
        num_osd_per_host = list(
            teuthology.roles_of_type(
                roles_for_host, 'osd'))
        num_osds = len(num_osd_per_host)
        if config.get('separate_journal_disk') is not None:
            num_devs_reqd = 2 * num_osds
            assert num_devs_reqd <= len(
                devs), 'fewer data and journal disks than required ' + shortname
            for dindex in range(0, num_devs_reqd, 2):
                jd_index = dindex + 1
                dev_short = devs[dindex].split('/')[-1]
                jdev_short = devs[jd_index].split('/')[-1]
                osd_devs.append((shortname, dev_short, jdev_short))
        else:
            assert num_osds <= len(devs), 'fewer disks than osds ' + shortname
            for dev in devs[:num_osds]:
                dev_short = dev.split('/')[-1]
                osd_devs.append((shortname, dev_short))
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


@contextlib.contextmanager
def build_ceph_cluster(ctx, config):
    """Build a ceph cluster"""

    # Expect to find ceph_admin on the first mon by ID, same place that the download task
    # puts it.  Remember this here, because subsequently IDs will change from those in
    # the test config to those that ceph-deploy invents.
    (ceph_admin,) = ctx.cluster.only(
        teuthology.get_first_mon(ctx, config)).remotes.iterkeys()

    def execute_ceph_deploy(cmd):
        """Remotely execute a ceph_deploy command"""
        return ceph_admin.run(
            args=[
                'cd',
                '{tdir}/ceph-deploy'.format(tdir=testdir),
                run.Raw('&&'),
                run.Raw(cmd),
            ],
            check_status=False,
        ).exitstatus

    try:
        log.info('Building ceph cluster using ceph-deploy...')
        testdir = teuthology.get_testdir(ctx)
        ceph_branch = None
        if config.get('branch') is not None:
            cbranch = config.get('branch')
            for var, val in cbranch.iteritems():
                ceph_branch = '--{var}={val}'.format(var=var, val=val)
        all_nodes = get_all_nodes(ctx, config)
        mds_nodes = get_nodes_using_role(ctx, 'mds')
        mds_nodes = " ".join(mds_nodes)
        mon_node = get_nodes_using_role(ctx, 'mon')
        mon_nodes = " ".join(mon_node)
        new_mon = './ceph-deploy new' + " " + mon_nodes
        mon_hostname = mon_nodes.split(' ')[0]
        mon_hostname = str(mon_hostname)
        gather_keys = './ceph-deploy gatherkeys' + " " + mon_hostname
        deploy_mds = './ceph-deploy mds create' + " " + mds_nodes
        no_of_osds = 0

        if mon_nodes is None:
            raise RuntimeError("no monitor nodes in the config file")

        estatus_new = execute_ceph_deploy(new_mon)
        if estatus_new != 0:
            raise RuntimeError("ceph-deploy: new command failed")

        log.info('adding config inputs...')
        testdir = teuthology.get_testdir(ctx)
        conf_path = '{tdir}/ceph-deploy/ceph.conf'.format(tdir=testdir)

        if config.get('conf') is not None:
            confp = config.get('conf')
            for section, keys in confp.iteritems():
                lines = '[{section}]\n'.format(section=section)
                teuthology.append_lines_to_file(ceph_admin, conf_path, lines,
                                                sudo=True)
                for key, value in keys.iteritems():
                    log.info("[%s] %s = %s" % (section, key, value))
                    lines = '{key} = {value}\n'.format(key=key, value=value)
                    teuthology.append_lines_to_file(
                        ceph_admin, conf_path, lines, sudo=True)

        # install ceph
        install_nodes = './ceph-deploy install ' + \
            (ceph_branch if ceph_branch else "--dev=master") + " " + all_nodes
        estatus_install = execute_ceph_deploy(install_nodes)
        if estatus_install != 0:
            raise RuntimeError("ceph-deploy: Failed to install ceph")
        # install ceph-test package too
        install_nodes2 = './ceph-deploy install --tests ' + \
            (ceph_branch if ceph_branch else "--dev=master") + " " + all_nodes
        estatus_install = execute_ceph_deploy(install_nodes2)
        if estatus_install != 0:
            raise RuntimeError("ceph-deploy: Failed to install ceph-test")

        mon_create_nodes = './ceph-deploy mon create-initial'
        # If the following fails, it is OK, it might just be that the monitors
        # are taking way more than a minute/monitor to form quorum, so lets
        # try the next block which will wait up to 15 minutes to gatherkeys.
        execute_ceph_deploy(mon_create_nodes)

        estatus_gather = execute_ceph_deploy(gather_keys)
        max_gather_tries = 90
        gather_tries = 0
        while (estatus_gather != 0):
            gather_tries += 1
            if gather_tries >= max_gather_tries:
                msg = 'ceph-deploy was not able to gatherkeys after 15 minutes'
                raise RuntimeError(msg)
            estatus_gather = execute_ceph_deploy(gather_keys)
            time.sleep(10)

        if mds_nodes:
            estatus_mds = execute_ceph_deploy(deploy_mds)
            if estatus_mds != 0:
                raise RuntimeError("ceph-deploy: Failed to deploy mds")

        if config.get('test_mon_destroy') is not None:
            for d in range(1, len(mon_node)):
                mon_destroy_nodes = './ceph-deploy mon destroy' + \
                    " " + mon_node[d]
                estatus_mon_d = execute_ceph_deploy(mon_destroy_nodes)
                if estatus_mon_d != 0:
                    raise RuntimeError("ceph-deploy: Failed to delete monitor")

        node_dev_list = get_dev_for_osd(ctx, config)
        for d in node_dev_list:
            node = d[0]
            for disk in d[1:]:
                zap = './ceph-deploy disk zap ' + node + ':' + disk
                estatus = execute_ceph_deploy(zap)
                if estatus != 0:
                    raise RuntimeError("ceph-deploy: Failed to zap osds")
            osd_create_cmd = './ceph-deploy osd create '
            if config.get('dmcrypt') is not None:
                osd_create_cmd += '--dmcrypt '
            osd_create_cmd += ":".join(d)
            estatus_osd = execute_ceph_deploy(osd_create_cmd)
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
                    client_keyring = \
                        '/etc/ceph/ceph.client.{id}.keyring'.format(id=id_)
                    mon0_remote.run(
                        args=[
                            'cd',
                            '{tdir}'.format(tdir=testdir),
                            run.Raw('&&'),
                            'sudo', 'bash', '-c',
                            run.Raw('"'), 'ceph',
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

            if mds_nodes:
                log.info('Configuring CephFS...')
                ceph_fs = Filesystem(ctx)
                if not ceph_fs.legacy_configured():
                    ceph_fs.create()
        elif not config.get('only_mon'):
            raise RuntimeError(
                "The cluster is NOT operational due to insufficient OSDs")
        yield

    except Exception:
        log.info(
            "Error encountered, logging exception before tearing down ceph-deploy")
        log.info(traceback.format_exc())
        raise
    finally:
        if config.get('keep_running'):
            return
        log.info('Stopping ceph...')
        ctx.cluster.run(args=['sudo', 'stop', 'ceph-all', run.Raw('||'),
                              'sudo', 'service', 'ceph', 'stop', run.Raw('||'),
                              'sudo', 'systemctl', 'stop', 'ceph.target'])

        # Are you really not running anymore?
        # try first with the init tooling
        # ignoring the status so this becomes informational only
        ctx.cluster.run(
            args=[
                'sudo', 'status', 'ceph-all', run.Raw('||'),
                'sudo', 'service', 'ceph', 'status', run.Raw('||'),
                'sudo', 'systemctl', 'status', 'ceph.target'],
            check_status=False)

        # and now just check for the processes themselves, as if upstart/sysvinit
        # is lying to us. Ignore errors if the grep fails
        ctx.cluster.run(args=['sudo', 'ps', 'aux', run.Raw('|'),
                              'grep', '-v', 'grep', run.Raw('|'),
                              'grep', 'ceph'], check_status=False)

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

        # Prevent these from being undefined if the try block fails
        all_nodes = get_all_nodes(ctx, config)
        purge_nodes = './ceph-deploy purge' + " " + all_nodes
        purgedata_nodes = './ceph-deploy purgedata' + " " + all_nodes

        log.info('Purging package...')
        execute_ceph_deploy(purge_nodes)
        log.info('Purging data...')
        execute_ceph_deploy(purgedata_nodes)


@contextlib.contextmanager
def cli_test(ctx, config):
    """
     ceph-deploy cli to exercise most commonly use cli's and ensure
     all commands works and also startup the init system.

    """
    log.info('Ceph-deploy Test')
    if config is None:
        config = {}
    test_branch = ''
    conf_dir = teuthology.get_testdir(ctx) + "/cdtest"

    def execute_cdeploy(admin, cmd, path):
        """Execute ceph-deploy commands """
        """Either use git path or repo path """
        args = ['cd', conf_dir, run.Raw(';')]
        if path:
            args.append('{path}/ceph-deploy/ceph-deploy'.format(path=path));
        else:
            args.append('ceph-deploy')
        args.append(run.Raw(cmd))
        ec = admin.run(args=args, check_status=False).exitstatus
        if ec != 0:
            raise RuntimeError(
                "failed during ceph-deploy cmd: {cmd} , ec={ec}".format(cmd=cmd, ec=ec))

    if config.get('rhbuild'):
        path = None
    else:
        path = teuthology.get_testdir(ctx)
        # test on branch from config eg: wip-* , master or next etc
        # packages for all distro's should exist for wip*
        if ctx.config.get('branch'):
            branch = ctx.config.get('branch')
            test_branch = ' --dev={branch} '.format(branch=branch)
    mons = ctx.cluster.only(teuthology.is_type('mon'))
    for node, role in mons.remotes.iteritems():
        admin = node
        admin.run(args=['mkdir', conf_dir], check_status=False)
        nodename = admin.shortname
    system_type = teuthology.get_system_type(admin)
    if config.get('rhbuild'):
        admin.run(args=['sudo', 'yum', 'install', 'ceph-deploy', '-y'])
    log.info('system type is %s', system_type)
    osds = ctx.cluster.only(teuthology.is_type('osd'))

    for remote, roles in osds.remotes.iteritems():
        devs = teuthology.get_scratch_devices(remote)
        log.info("roles %s", roles)
        if (len(devs) < 3):
            log.error(
                'Test needs minimum of 3 devices, only found %s',
                str(devs))
            raise RuntimeError("Needs minimum of 3 devices ")

    conf_path = '{conf_dir}/ceph.conf'.format(conf_dir=conf_dir)
    new_cmd = 'new ' + nodename
    execute_cdeploy(admin, new_cmd, path)
    if config.get('conf') is not None:
        confp = config.get('conf')
        for section, keys in confp.iteritems():
            lines = '[{section}]\n'.format(section=section)
            teuthology.append_lines_to_file(admin, conf_path, lines,
                                            sudo=True)
            for key, value in keys.iteritems():
                log.info("[%s] %s = %s" % (section, key, value))
                lines = '{key} = {value}\n'.format(key=key, value=value)
                teuthology.append_lines_to_file(admin, conf_path, lines,
                                                sudo=True)
    new_mon_install = 'install {branch} --mon '.format(
        branch=test_branch) + nodename
    new_osd_install = 'install {branch} --osd '.format(
        branch=test_branch) + nodename
    new_admin = 'install {branch} --cli '.format(branch=test_branch) + nodename
    create_initial = 'mon create-initial '
    execute_cdeploy(admin, new_mon_install, path)
    execute_cdeploy(admin, new_osd_install, path)
    execute_cdeploy(admin, new_admin, path)
    execute_cdeploy(admin, create_initial, path)

    for i in range(3):
        zap_disk = 'disk zap ' + "{n}:{d}".format(n=nodename, d=devs[i])
        prepare = 'osd prepare ' + "{n}:{d}".format(n=nodename, d=devs[i])
        execute_cdeploy(admin, zap_disk, path)
        execute_cdeploy(admin, prepare, path)

    log.info("list files for debugging purpose to check file permissions")
    admin.run(args=['ls', run.Raw('-lt'), conf_dir])
    remote.run(args=['sudo', 'ceph', '-s'], check_status=False)
    r = remote.run(args=['sudo', 'ceph', 'health'], stdout=StringIO())
    out = r.stdout.getvalue()
    log.info('Ceph health: %s', out.rstrip('\n'))
    log.info("Waiting for cluster to become healthy")
    with contextutil.safe_while(sleep=10, tries=6,
                                action='check health') as proceed:
       while proceed():
           r = remote.run(args=['sudo', 'ceph', 'health'], stdout=StringIO())
           if (out.split(None,1)[0] == 'HEALTH_OK'):
               break
    rgw_install = 'install {branch} --rgw {node}'.format(
        branch=test_branch,
        node=nodename,
    )
    rgw_create = 'rgw create ' + nodename
    execute_cdeploy(admin, rgw_install, path)
    execute_cdeploy(admin, rgw_create, path)
    log.info('All ceph-deploy cli tests passed')
    try:
        yield
    finally:
        log.info("cleaning up")
        ctx.cluster.run(args=['sudo', 'stop', 'ceph-all', run.Raw('||'),
                              'sudo', 'service', 'ceph', 'stop', run.Raw('||'),
                              'sudo', 'systemctl', 'stop', 'ceph.target'],
                        check_status=False)
        time.sleep(4)
        for i in range(3):
            umount_dev = "{d}1".format(d=devs[i])
            r = remote.run(args=['sudo', 'umount', run.Raw(umount_dev)])
        cmd = 'purge ' + nodename
        execute_cdeploy(admin, cmd, path)
        cmd = 'purgedata ' + nodename
        execute_cdeploy(admin, cmd, path)
        log.info("Removing temporary dir")
        admin.run(
            args=[
                'rm',
                run.Raw('-rf'),
                run.Raw(conf_dir)],
            check_status=False)
        if config.get('rhbuild'):
            admin.run(args=['sudo', 'yum', 'remove', 'ceph-deploy', '-y'])


@contextlib.contextmanager
def single_node_test(ctx, config):
    """
    - ceph-deploy.single_node_test: null

    #rhbuild testing
    - ceph-deploy.single_node_test:
        rhbuild: 1.2.3

    """
    log.info("Testing ceph-deploy on single node")
    if config is None:
        config = {}
    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('ceph-deploy', {}))

    if config.get('rhbuild'):
        log.info("RH Build, Skip Download")
        with contextutil.nested(
            lambda: cli_test(ctx=ctx, config=config),
        ):
            yield
    else:
        with contextutil.nested(
            lambda: install_fn.ship_utilities(ctx=ctx, config=None),
            lambda: download_ceph_deploy(ctx=ctx, config=config),
            lambda: cli_test(ctx=ctx, config=config),
        ):
            yield


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
             only_mon: true
             keep_running: true

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
             dmcrypt: yes
             separate_journal_disk: yes

    """
    if config is None:
        config = {}

    assert isinstance(config, dict), \
        "task ceph-deploy only supports a dictionary for configuration"

    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('ceph-deploy', {}))

    if config.get('branch') is not None:
        assert isinstance(
            config['branch'], dict), 'branch must be a dictionary'

    log.info('task ceph-deploy with config ' + str(config))

    with contextutil.nested(
        lambda: install_fn.ship_utilities(ctx=ctx, config=None),
        lambda: download_ceph_deploy(ctx=ctx, config=config),
        lambda: build_ceph_cluster(ctx=ctx, config=config),
    ):
        yield
