from ceph_volume import process


def get_partuuid(device):
    """
    If a device is a partition, it will probably have a PARTUUID on it that
    will persist and can be queried against `blkid` later to detect the actual
    device
    """
    out, err, rc = process.call(
        ['sudo', 'blkid', '-s', 'PARTUUID', '-o', 'value', device]
    )
    return ' '.join(out).strip()


def get_device_from_partuuid(partuuid):
    """
    If a device has a partuuid, query blkid so that it can tell us what that
    device is
    """
    out, err, rc = process.call(
        ['sudo', 'blkid', '-t', 'PARTUUID="%s"' % partuuid, '-o', 'device']
    )
    return ' '.join(out).strip()
