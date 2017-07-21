"""
Systemd test
"""
import contextlib
import logging
import re
import time

from cStringIO import StringIO
from teuthology.orchestra import run
from teuthology.misc import reconnect, get_first_mon, wait_until_healthy

log = logging.getLogger(__name__)


@contextlib.contextmanager
def task(ctx, config):
    """
      - tasks:
          ceph-deploy:
          systemd:

    Test ceph systemd services can start, stop and restart and
    check for any failed services and report back errors
    """
    for remote, roles in ctx.cluster.remotes.iteritems():
        remote.run(args=['sudo', 'ps', '-eaf', run.Raw('|'),
                         'grep', 'ceph'])
        r = remote.run(args=['sudo', 'systemctl', 'list-units', run.Raw('|'),
                             'grep', 'ceph'], stdout=StringIO(),
                       check_status=False)
        log.info(r.stdout.getvalue())
        if r.stdout.getvalue().find('failed'):
            log.info("Ceph services in failed state")

        # test overall service stop and start using ceph.target
        # ceph.target tests are meant for ceph systemd tests
        # and not actual process testing using 'ps'
        log.info("Stopping all Ceph services")
        remote.run(args=['sudo', 'systemctl', 'stop', 'ceph.target'])
        r = remote.run(args=['sudo', 'systemctl', 'status', 'ceph.target'],
                       stdout=StringIO(), check_status=False)
        log.info(r.stdout.getvalue())
        log.info("Checking process status")
        r = remote.run(args=['sudo', 'ps', '-eaf', run.Raw('|'),
                             'grep', 'ceph'], stdout=StringIO())
        if r.stdout.getvalue().find('Active: inactive'):
            log.info("Sucessfully stopped all ceph services")
        else:
            log.info("Failed to stop ceph services")

        log.info("Starting all Ceph services")
        remote.run(args=['sudo', 'systemctl', 'start', 'ceph.target'])
        r = remote.run(args=['sudo', 'systemctl', 'status', 'ceph.target'],
                       stdout=StringIO())
        log.info(r.stdout.getvalue())
        if r.stdout.getvalue().find('Active: active'):
            log.info("Sucessfully started all Ceph services")
        else:
            log.info("info", "Failed to start Ceph services")
        r = remote.run(args=['sudo', 'ps', '-eaf', run.Raw('|'),
                             'grep', 'ceph'], stdout=StringIO())
        log.info(r.stdout.getvalue())
        time.sleep(4)

        # test individual services start stop
        name = remote.shortname
        mon_name = 'ceph-mon@' + name + '.service'
        mds_name = 'ceph-mds@' + name + '.service'
        mgr_name = 'ceph-mgr@' + name + '.service'
        mon_role_name = 'mon.' + name
        mds_role_name = 'mds.' + name
        mgr_role_name = 'mgr.' + name
        m_osd = re.search('--id (\d+) --setuser ceph', r.stdout.getvalue())
        if m_osd:
            osd_service = 'ceph-osd@{m}.service'.format(m=m_osd.group(1))
            remote.run(args=['sudo', 'systemctl', 'status',
                             osd_service])
            remote.run(args=['sudo', 'systemctl', 'stop',
                             osd_service])
            time.sleep(4)  # immediate check will result in deactivating state
            r = remote.run(args=['sudo', 'systemctl', 'status', osd_service],
                           stdout=StringIO(), check_status=False)
            log.info(r.stdout.getvalue())
            if r.stdout.getvalue().find('Active: inactive'):
                log.info("Sucessfully stopped single osd ceph service")
            else:
                log.info("Failed to stop ceph osd services")
            remote.run(args=['sudo', 'systemctl', 'start',
                             osd_service])
            time.sleep(4)
        if mon_role_name in roles:
            remote.run(args=['sudo', 'systemctl', 'status', mon_name])
            remote.run(args=['sudo', 'systemctl', 'stop', mon_name])
            time.sleep(4)  # immediate check will result in deactivating state
            r = remote.run(args=['sudo', 'systemctl', 'status', mon_name],
                           stdout=StringIO(), check_status=False)
            if r.stdout.getvalue().find('Active: inactive'):
                log.info("Sucessfully stopped single mon ceph service")
            else:
                log.info("Failed to stop ceph mon service")
            remote.run(args=['sudo', 'systemctl', 'start', mon_name])
            time.sleep(4)
        if mgr_role_name in roles:
            remote.run(args=['sudo', 'systemctl', 'status', mgr_name])
            remote.run(args=['sudo', 'systemctl', 'stop', mgr_name])
            time.sleep(4)  # immediate check will result in deactivating state
            r = remote.run(args=['sudo', 'systemctl', 'status', mgr_name],
                           stdout=StringIO(), check_status=False)
            if r.stdout.getvalue().find('Active: inactive'):
                log.info("Sucessfully stopped single ceph mgr service")
            else:
                log.info("Failed to stop ceph mgr service")
            remote.run(args=['sudo', 'systemctl', 'start', mgr_name])
            time.sleep(4)
        if mds_role_name in roles:
            remote.run(args=['sudo', 'systemctl', 'status', mds_name])
            remote.run(args=['sudo', 'systemctl', 'stop', mds_name])
            time.sleep(4)  # immediate check will result in deactivating state
            r = remote.run(args=['sudo', 'systemctl', 'status', mds_name],
                           stdout=StringIO(), check_status=False)
            if r.stdout.getvalue().find('Active: inactive'):
                log.info("Sucessfully stopped single ceph mds service")
            else:
                log.info("Failed to stop ceph mds service")
            remote.run(args=['sudo', 'systemctl', 'start', mds_name])
            time.sleep(4)

    # reboot all nodes and verify the systemd units restart
    # workunit that runs would fail if any of the systemd unit doesnt start
    ctx.cluster.run(args='sudo reboot', wait=False, check_status=False)
    # avoid immediate reconnect
    time.sleep(120)
    reconnect(ctx, 480)  # reconnect all nodes
    # for debug info
    ctx.cluster.run(args=['sudo', 'ps', '-eaf', run.Raw('|'),
                          'grep', 'ceph'])
    # wait for HEALTH_OK
    mon = get_first_mon(ctx, config)
    (mon_remote,) = ctx.cluster.only(mon).remotes.iterkeys()
    wait_until_healthy(ctx, mon_remote, use_sudo=True)
    yield
