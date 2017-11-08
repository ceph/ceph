"""
Utilities to control systemd units
"""
from ceph_volume import process


def start(unit):
    process.run(['sudo', 'systemctl', 'start', unit])


def stop(unit):
    process.run(['sudo', 'systemctl', 'stop', unit])


def enable(unit):
    process.run(['sudo', 'systemctl', 'enable', unit])


def disable(unit):
    process.run(['sudo', 'systemctl', 'disable', unit])


def mask(unit):
    process.run(['sudo', 'systemctl', 'mask', unit])


def start_osd(id_):
    return start(osd_unit % id_)


def stop_osd(id_):
    return stop(osd_unit % id_)


def enable_osd(id_):
    return enable(osd_unit % id_)


def disable_osd(id_):
    return disable(osd_unit % id_)


def enable_volume(id_, fsid, device_type='lvm'):
    return enable(volume_unit % (device_type, id_, fsid))


def mask_ceph_disk(instance='*'):
    # ``instance`` will probably be '*' all the time, because ceph-volume will
    # want all instances disabled at once, not just one
    return mask(ceph_disk_unit % instance)


#
# templates
#

osd_unit = "ceph-osd@%s"
ceph_disk_unit = "ceph-disk@%s"
volume_unit = "ceph-volume@%s-%s-%s"
