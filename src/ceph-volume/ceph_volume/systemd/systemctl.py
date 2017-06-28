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


def start_osd(id_):
    return start(osd_unit % id_)


def stop_osd(id_):
    return stop(osd_unit % id_)


def enable_osd(id_):
    return enable(osd_unit % id_)


def disable_osd(id_):
    return disable(osd_unit % id_)


#
# templates
#

osd_unit = "ceph-osd@%s"
