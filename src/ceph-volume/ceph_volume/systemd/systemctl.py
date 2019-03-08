"""
Utilities to control systemd units
"""
import logging

from ceph_volume import process

logger = logging.getLogger(__name__)

def start(unit):
    process.run(['systemctl', 'start', unit])


def stop(unit):
    process.run(['systemctl', 'stop', unit])


def enable(unit, runtime=False):
    if runtime:
        process.run(['systemctl', 'enable', '--runtime', unit])
    else:
        process.run(['systemctl', 'enable', unit])


def disable(unit):
    process.run(['systemctl', 'disable', unit])


def mask(unit):
    process.run(['systemctl', 'mask', unit])


def is_active(unit):
    out, err, rc = process.call(
        ['systemctl', 'is-active', unit],
        verbose_on_failure=False
    )
    return rc == 0

def get_running_osd_ids():
    out, err, rc = process.call([
        'systemctl',
        'show',
        '--no-pager',
        '--property=Id',
        '--state=running',
        'ceph-osd@*',
    ])
    osd_ids = []
    if rc == 0:
        for line in out:
            if line:
                # example line looks like: Id=ceph-osd@1.service
                try:
                    osd_id = line.split("@")[1].split(".service")[0]
                    osd_ids.append(osd_id)
                except (IndexError, TypeError):
                    logger.warning("Failed to parse output from systemctl: %s", line)
    return osd_ids

def start_osd(id_):
    return start(osd_unit % id_)


def stop_osd(id_):
    return stop(osd_unit % id_)


def enable_osd(id_):
    return enable(osd_unit % id_, runtime=True)


def disable_osd(id_):
    return disable(osd_unit % id_)


def osd_is_active(id_):
    return is_active(osd_unit % id_)


def enable_volume(id_, fsid, device_type='lvm'):
    return enable(volume_unit % (device_type, id_, fsid))


def mask_ceph_disk():
    # systemctl allows using a glob like '*' for masking, but there was a bug
    # in that it wouldn't allow this for service templates. This means that
    # masking ceph-disk@* will not work, so we must link the service directly.
    # /etc/systemd takes precedence regardless of the location of the unit
    process.run(
        ['ln', '-sf', '/dev/null', '/etc/systemd/system/ceph-disk@.service']
    )


#
# templates
#

osd_unit = "ceph-osd@%s"
ceph_disk_unit = "ceph-disk@%s"
volume_unit = "ceph-volume@%s-%s-%s"
