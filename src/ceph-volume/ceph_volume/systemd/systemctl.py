"""
Utilities to control systemd units
"""
from ceph_volume import process


def start(unit):
    process.run(['systemctl', 'start', unit])


def stop(unit):
    process.run(['systemctl', 'stop', unit])


def enable(unit):
    process.run(['systemctl', 'enable', unit])


def disable(unit):
    process.run(['systemctl', 'disable', unit])


def mask(unit):
    process.run(['systemctl', 'mask', unit])


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


def mask_ceph_disk():
    # systemctl allows using a glob like '*' for masking, but there was a bug
    # in that it wouldn't allow this for service templates. This means that
    # masking ceph-disk@* will not work, so we must link the service directly.
    # /etc/systemd takes precendence regardless of the location of the unit
    process.run(
        ['ln', '-sf', '/dev/null', '/etc/systemd/system/ceph-disk@.service']
    )


#
# templates
#

osd_unit = "ceph-osd@%s"
ceph_disk_unit = "ceph-disk@%s"
volume_unit = "ceph-volume@%s-%s-%s"
