from ceph_volume.util import disk
from ceph_volume.api import lvm


def minimum_device_size(devices):
    """
    Ensure that the minimum requirements for this type of scenario is
    met, raise an error if the provided devices would not work
    """
    msg = 'Unable to use device smaller than 5GB: %s (%s)'
    for device in devices:
        device_size = disk.Size(b=device.sys_api['size'])
        if device_size < disk.Size(gb=5):
            raise RuntimeError(msg % (device, device_size))


def no_lvm_membership(devices):
    """
    Do not allow devices that are part of LVM
    """
    msg = 'Unable to use device, already a member of LVM: %s'
    for device in devices:
        if device.is_lvm_member:
            raise RuntimeError(msg % device.abspath)


def has_common_vg(ssd_devices):
    """
    Ensure that devices have a common VG between them
    """
    msg = 'Could not find a common VG between devices: %s'
    system_vgs = lvm.VolumeGroups()
    ssd_vgs = {}

    for ssd_device in ssd_devices:
        for pv in ssd_device.pvs_api:
            vg = system_vgs.get(vg_name=pv.vg_name)
            if not vg:
                continue
            try:
                ssd_vgs[vg.name].append(ssd_device.abspath)
            except KeyError:
                ssd_vgs[vg.name] = [ssd_device.abspath]
    # len of 1 means they all have a common vg, and len of 0 means that these
    # are blank
    if len(ssd_vgs) <= 1:
        return
    raise RuntimeError(msg % ', '.join(ssd_vgs.keys()))
