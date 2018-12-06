"""
Migrate from filestore to bluestore
"""
import logging
import json
import time
from StringIO import StringIO

from teuthology import misc as teuthology
# from util.rados import rados

LOG = logging.getLogger(__name__)


def is_active_osd_systemd(remote, osdid):
    ''' check status through systemd'''
    is_active = [
        'sudo',
        'systemctl',
        'is-active',
        'ceph-osd@{}'.format(osdid),
    ]
    proc = remote.run(
        args=is_active,
        stdout=StringIO(),
        stderr=StringIO(),
        check_status=False,
    )
    if proc.stdout is not None:
        status = proc.stdout.getvalue()
    else:
        status = proc.stderr.getvalue()
    LOG.info(status)
    return "active" == status.strip('\n')


def is_active_osd(remote, osdid):
    return is_active_osd_systemd(remote, osdid)


def start_osd_systemd(remote, osdid, timeout=600):
    '''start an osd using systemd command'''
    start = time.time()
    status = is_active_osd(remote, osdid)
    if status:
        return

    start_cmd = [
        'sudo',
        'systemctl',
        'start',
        'ceph-osd@{}'.format(osdid),
    ]
    remote.run(
        args=start_cmd,
        wait=False,
    )

    while timeout > (time.time() - start):
        active = is_active_osd(remote, osdid)
        if active:
            return
        else:
            LOG.info("osd status is not UP, looping again")
            continue
    LOG.error("Failed to start osd.{}".format(osdid))
    assert False


def start_osd(remote, osd):
    start_osd_systemd(remote, osd)


def stop_osd_systemd(remote, osdid):
    ''' stop osd through systemd command '''
    active = is_active_osd(remote, osdid)
    if active:
        stop_cmd = [
            'sudo',
            'systemctl',
            'stop',
            'ceph-osd@{}'.format(osdid),
        ]
        remote.run(
            args=stop_cmd,
            wait=False,
        )
    else:
        LOG.info("osd.{} is not running".format(osdid))
        return 1

    time.sleep(5)
    active = is_active_osd(remote, osdid)
    return active


def stop_osd(remote, osdid):
    ''' stop osd : may be daemon-helper or systemd'''
    return stop_osd_systemd(remote, osdid)


def unmount_osd(remote, disk):
    '''
    TODO: to be handled for different journal partition, encrypted etc..
    as of now just unmount it
    '''
    if 'encrypted' in disk:
        '''TODO: handle if disk is encrypted'''
        pass
    if 'lvm' in disk:
        '''TODO: may need to handle this'''
        pass
    target = disk['data']['path']
    umnt = [
        'sudo',
        'umount',
        '{}'.format(target),
    ]
    proc = remote.run(
        args=umnt,
        check_status=False,
    )
    time.sleep(5)
    proc.wait()
    return proc.exitstatus == 0


def prepare_to_migrate(remote, osdid, disk):
    ''' Stops the osd and unmount the disk '''
    ret = 0

    ret = stop_osd(remote, osdid)
    if ret:
        LOG.error("Could not stop osd.{} OR its already stopped".format(osdid))
        assert False
    LOG.info("Stopped osd.{}".format(osdid))

    ret = unmount_osd(remote, disk)
    if not ret:
        LOG.error("Couldn't unmount disk {} for osd {}".format(disk, osdid))
        assert False


def is_worth_migrating(osd, mon):
    ''' If its not filestore osd then its not worth trying migration'''
    osd_meta = [
        'sudo',
        'ceph',
        'osd',
        'metadata',
        '{}'.format(osd),
    ]
    proc = mon.run(
        args=osd_meta,
        stdout=StringIO(),
        stderr=StringIO(),
    )
    if proc.stdout is not None:
        status = proc.stdout.getvalue()
    else:
        status = proc.stderr.getvalue()
    LOG.info(status)
    dstatus = json.loads(status)
    return dstatus['osd_objectstore'] == 'filestore'


def check_objectstore(osd, mon, store):
    ''' check what objectstore it has '''
    osd_meta = [
        'sudo',
        'ceph',
        'osd',
        'metadata',
        '{}'.format(osd),
    ]
    proc = mon.run(
        args=osd_meta,
        stdout=StringIO(),
        stderr=StringIO(),
    )
    if proc.stdout is not None:
        status = proc.stdout.getvalue()
    else:
        status = proc.stderr.getvalue()
    LOG.info(status)

    dstatus = json.loads(status)
    return dstatus['osd_objectstore'] == store


def check_safe_to_destroy(mon, osdid, timeout=600):
    ''' check whether its safe to destroy osd'''
    start = time.time()
    is_safe = [
        'sudo',
        'ceph',
        'osd',
        'safe-to-destroy',
        str(osdid),
    ]

    while timeout > (time.time() - start):
        proc = mon.run(
            args=is_safe,
            stdout=StringIO(),
            stderr=StringIO(),
            check_status=False,
        )
        out = proc.stdout.getvalue()
        err = proc.stderr.getvalue()
        LOG.info(out)
        LOG.info(err)

        if "EBUSY" in err:
            # looks like recovery is still in progress try for sometime
            continue
        if "safe to destroy" in out or "safe to destroy" in err:
            return
    LOG.error("OSD never reached safe-to-destroy condition")
    assert False


def zap_volume(remote, disk):
    ''' zap the disk '''
    if 'encrypted' in disk:
        '''TODO: handle this'''
        pass
    if 'lvm' in disk:
        '''TODO: handle this'''
        pass
    target = disk['data']['path']
    zap_cmd = [
        'sudo',
        'ceph-volume',
        'lvm',
        'zap',
        target,
    ]
    proc = remote.run(
        args=zap_cmd,
        stdout=StringIO(),
        stderr=StringIO(),
    )
    if proc.stdout is not None:
        status = proc.stdout.getvalue()
    else:
        status = proc.stderr.getvalue()
    LOG.info(status)
    if 'success' not in status:
        LOG.error("Failed to zap disk {}".format(target))
        assert False


def destroy_osd(mon, osd_num):
    ''' destroy the osd'''
    d_cmd = [
        'sudo',
        'ceph',
        'osd',
        'destroy',
        str(osd_num),
        '--yes-i-really-mean-it',
    ]
    mon.run(
        args=d_cmd,
        wait=False
    )


def create_bluestore(remote, disk, osd_num, mon):
    ''' create bluestore on the disk '''
    time.sleep(10)
    osd_tree = [
        'sudo',
        'ceph',
        'osd',
        'tree',
    ]
    proc = mon.run(
        args=osd_tree,
        stdout=StringIO(),
        stderr=StringIO(),
    )
    if proc.stdout is not None:
        status = proc.stdout.getvalue()
    else:
        status = proc.stdout.getvalue()

    LOG.info(status)

    target = disk['data']['path']
    create_blue = [
        'sudo',
        'ceph-volume',
        'lvm',
        'create',
        '--bluestore',
        '--data',
        target,
        '--osd-id',
        str(osd_num),
    ]
    proc = remote.run(
        args=create_blue,
        stdout=StringIO(),
        stderr=StringIO(),
    )
    if proc.stdout is not None:
        status = proc.stdout.getvalue()
    else:
        status = proc.stderr.getvalue()
    LOG.info(status)

    if "lvm create successful" not in status:
        LOG.error("lvm create failed for osd.{}".format(osd_num))
        assert False
    LOG.info("lvm creation successful for osd.{}".format(osd_num))


def build_remote_osds(osd_disk_info):
    '''
    osd_disk_info is a dict with osd number as key,
    for easy of iteration purpose we will build a new dict with
    remote as key so that migration loop will be looping around
    all the osds on a single remote
    O/P: {'remote':[{osd1:{'data':disk1}, {'journal':disk2}}, {osd2:disk2}]}
    '''
    remotes_osds_disks = dict()
    for osd, disks_remote in osd_disk_info.iteritems():
        if disks_remote[1] not in remotes_osds_disks:
            remotes_osds_disks[disks_remote[1]] = []
        remotes_osds_disks[disks_remote[1]].append({osd: disks_remote[0]})
    return remotes_osds_disks


def migrate(manager, osd_disk_info, mon):
    ''' Prepare and migrate to bluestore'''
    remotes_osds_disks = build_remote_osds(osd_disk_info)
    for remote, osd_disks in remotes_osds_disks.iteritems():
        for ele in osd_disks:
            for osd_id, disk in ele.iteritems():
                if not is_worth_migrating(osd_id, mon):
                    LOG.warn("Skipping migration of osd %s" % osd_id)
                else:
                    manager.mark_out_osd(osd_id)
                    manager.wait_for_clean()
                    check_safe_to_destroy(mon, osd_id)
                    prepare_to_migrate(remote, osd_id, disk)
                    zap_volume(remote, disk)
                    destroy_osd(mon, osd_id)
                    create_bluestore(remote, disk, osd_id, mon)
                    start_osd(remote, osd_id)
                    manager.mark_in_osd(osd_id)
                    manager.wait_for_clean()
                    # check whether it has bluestore now
                    if not check_objectstore(osd_id, mon, "bluestore"):
                        LOG.error("Failed conversion from fs to bs in osd.{}".
                                  format(osd_id))
                        assert False
                    LOG.info("Successfully migrated from fs to bs for osd.{}".
                             format(osd_id))


def check_all_filestore(mon, num_osds):
    ''' Check whether all the osds are currently filestore or not '''
    fcount = 0
    objstore = [
        'sudo',
        'ceph',
        'osd',
        'count-metadata',
        'osd_objectstore',
    ]

    proc = mon.run(
        args=objstore,
        stdout=StringIO(),
    )
    if proc.stdout is not None:
        out = proc.stdout.getvalue()
    else:
        out = proc.stderr.getvalue()
    LOG.info(out)

    dout = json.loads(out)
    if 'filestore' in dout:
        fcount = dout['filestore']

    if fcount != num_osds:
        LOG.warning("Looks like not all osds are on filestore \
                    before migration")
        if 'bluestore' in out:
            LOG.warning("some of the osds are already on Bluestore")


def task(ctx, config):
    """
    task for migrating from filestore to bluestore
    """
    if config is None:
        config = {}
        assert isinstance(config, dict), \
            'Migrate task accepts only dict'
    manager = ctx.managers['ceph']
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()

    num_osds = teuthology.num_instances_of_type(ctx.cluster, 'osd')
    LOG.info('number of osds = {}'.format(num_osds))

    check_all_filestore(mon, num_osds)
    migrate(manager, ctx.osd_disk_info, mon)
