from cStringIO import StringIO

import contextlib
import logging
import random

from teuthology import misc as teuthology
from teuthology.orchestra import run

from ceph_manager import CephManager, write_conf


log = logging.getLogger(__name__)


def _get_mons(ctx):
    return [name[len('mon.'):] for name in teuthology.get_mon_names(ctx)]


# teuthology prepares the monitor IPs (and ports) in get_mons(), we can
# enumerate all monitor ports ([6789..]), and find the next available one.
def _get_next_port(ctx, ip, cluster):
    # assuming we have only one cluster here.
    used = []
    for name in teuthology.get_mon_names(ctx, cluster):
        addr = ctx.ceph[cluster].mons[name]
        mon_ip, mon_port = addr.split(':')
        if mon_ip != ip:
            continue
        used.append(int(mon_port))
    port = 6789
    used.sort()
    for p in used:
        if p != port:
            break
        port += 1
    return port


def _setup_mon(ctx, manager, remote, mon, name, data_path, conf_path):
    # co-locate a new monitor on remote where an existing monitor is hosted
    cluster = manager.cluster
    remote.run(args=['sudo', 'mkdir', '-p', data_path])
    keyring_path = '/etc/ceph/{cluster}.keyring'.format(
        cluster=manager.cluster)
    testdir = teuthology.get_testdir(ctx)
    monmap_path = '{tdir}/{cluster}.monmap'.format(tdir=testdir,
                                                   cluster=cluster)
    manager.raw_cluster_cmd('mon', 'getmap', '-o', monmap_path)
    if manager.controller != remote:
        monmap = teuthology.get_file(manager.controller, monmap_path)
        teuthology.write_file(remote, monmap_path, StringIO(monmap))
    remote.run(
        args=[
            'sudo',
            'ceph-mon',
            '--cluster', cluster,
            '--mkfs',
            '-i', mon,
            '--monmap', monmap_path,
            '--keyring', keyring_path])
    if manager.controller != remote:
        teuthology.delete_file(remote, monmap_path)
    # raw_cluster_cmd() is performed using sudo, so sudo here also.
    teuthology.delete_file(manager.controller, monmap_path, sudo=True)
    # update ceph.conf so that the ceph CLI is able to connect to the cluster
    if conf_path:
        ip = remote.ip_address
        port = _get_next_port(ctx, ip, cluster)
        mon_addr = '{ip}:{port}'.format(ip=ip, port=port)
        ctx.ceph[cluster].conf[name] = {'mon addr': mon_addr}
        write_conf(ctx, conf_path, cluster)


def _teardown_mon(ctx, manager, remote, name, data_path, conf_path):
    cluster = manager.cluster
    del ctx.ceph[cluster].conf[name]
    write_conf(ctx, conf_path, cluster)
    remote.run(args=['sudo', 'rm', '-rf', data_path])


@contextlib.contextmanager
def _prepare_mon(ctx, manager, remote, mon):
    cluster = manager.cluster
    data_path = '/var/lib/ceph/mon/{cluster}-{id}'.format(
        cluster=cluster, id=mon)
    conf_path = '/etc/ceph/{cluster}.conf'.format(cluster=cluster)
    name = 'mon.{0}'.format(mon)
    _setup_mon(ctx, manager, remote, mon, name, data_path, conf_path)
    yield
    _teardown_mon(ctx, manager, remote, name,
                  data_path, conf_path)


# run_daemon() in ceph.py starts a herd of daemons of the same type, but
# _run_daemon() starts only one instance.
@contextlib.contextmanager
def _run_daemon(ctx, remote, cluster, type_, id_):
    testdir = teuthology.get_testdir(ctx)
    coverage_dir = '{tdir}/archive/coverage'.format(tdir=testdir)
    daemon_signal = 'kill'
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
        '--cluster', cluster,
        '-i', id_]
    run_cmd.extend(run_cmd_tail)
    ctx.daemons.add_daemon(remote, type_, id_,
                           cluster=cluster,
                           args=run_cmd,
                           logger=log.getChild(type_),
                           stdin=run.PIPE,
                           wait=False)
    daemon = ctx.daemons.get_daemon(type_, id_, cluster)
    yield daemon
    daemon.stop()


@contextlib.contextmanager
def task(ctx, config):
    """
    replace a monitor with a newly added one, and then revert this change

    How it works::
    1. add a mon with specified id (mon.victim_prime)
    2. wait for quorum
    3. remove a monitor with specified id (mon.victim), mon.victim will commit
       suicide
    4. wait for quorum
    5. <yield>
    5. add mon.a back, and start it
    6. wait for quorum
    7. remove mon.a_prime

    Options::
    victim       the id of the mon to be removed (pick a random mon by default)
    replacer     the id of the new mon (use "${victim}_prime" if not specified)
    """
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()
    manager = CephManager(mon, ctx=ctx, logger=log.getChild('ceph_manager'))

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        "task ceph only supports a dictionary for configuration"
    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('mon_seesaw', {}))
    victim = config.get('victim', random.choice(_get_mons(ctx)))
    replacer = config.get('replacer', '{0}_prime'.format(victim))
    remote = manager.find_remote('mon', victim)
    quorum = manager.get_mon_quorum()
    cluster = manager.cluster
    log.info('replacing {victim} with {replacer}'.format(victim=victim,
                                                         replacer=replacer))
    with _prepare_mon(ctx, manager, remote, replacer):
        with _run_daemon(ctx, remote, cluster, 'mon', replacer):
            # replacer will join the quorum automatically
            manager.wait_for_mon_quorum_size(len(quorum) + 1, 10)
            # if we don't remove the victim from monmap, there is chance that
            # we are leaving the new joiner with a monmap of 2 mon, and it will
            # not able to reach the other one, it will be keeping probing for
            # ever.
            log.info('removing {mon}'.format(mon=victim))
            manager.raw_cluster_cmd('mon', 'remove', victim)
            manager.wait_for_mon_quorum_size(len(quorum), 10)
            # the victim will commit suicide after being removed from
            # monmap, let's wait until it stops.
            ctx.daemons.get_daemon('mon', victim, cluster).wait(10)
            try:
                # perform other tasks
                yield
            finally:
                # bring the victim back online
                # nuke the monstore of victim, otherwise it will refuse to boot
                # with following message:
                #
                # not in monmap and have been in a quorum before; must have
                # been removed
                log.info('re-adding {mon}'.format(mon=victim))
                data_path = '/var/lib/ceph/mon/{cluster}-{id}'.format(
                    cluster=cluster, id=victim)
                remote.run(args=['sudo', 'rm', '-rf', data_path])
                name = 'mon.{0}'.format(victim)
                _setup_mon(ctx, manager, remote, victim, name, data_path, None)
                log.info('reviving {mon}'.format(mon=victim))
                manager.revive_mon(victim)
                manager.wait_for_mon_quorum_size(len(quorum) + 1, 10)
                manager.raw_cluster_cmd('mon', 'remove', replacer)
                manager.wait_for_mon_quorum_size(len(quorum), 10)
