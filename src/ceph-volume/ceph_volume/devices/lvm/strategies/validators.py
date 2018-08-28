from ceph_volume.util import disk


def minimum_device_size(devices):
    """
    Ensure that the minimum requirements for this type of scenario is
    met, raise an error if the provided devices would not work
    """
    msg = 'Unable to use device smaller than 5GB: %s (%s)'
    for device in devices:
        device_size = disk.Size(b=device['size'])
        if device_size < disk.Size(gb=5):
            raise RuntimeError(msg % (device, device_size))
