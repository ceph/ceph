from ceph.deployment.inventory import Devices, Device

try:
    from typing import Any, List
except ImportError:
    pass  # for type checking


def _mk_device(rotational=True,
               locked=False,
               size="394.27 GB"):
    return [Device(
        path='??',
        sys_api={
            "rotational": '1' if rotational else '0',
            "vendor": "Vendor",
            "human_readable_size": size,
            "partitions": {},
            "locked": int(locked),
            "sectorsize": "512",
            "removable": "0",
            "path": "??",
            "support_discard": "",
            "model": "Model",
            "ro": "0",
            "nr_requests": "128",
            "size": 423347879936  # ignore coversion from human_readable_size
        },
        available=not locked,
        rejected_reasons=['locked'] if locked else [],
        lvs=[],
        device_id="Model-Vendor-foobar"
    )]


def _mk_inventory(devices):
    # type: (Any) -> List[Device]
    devs = []
    for dev_, name in zip(devices, map(chr, range(ord('a'), ord('z')))):
        dev = Device.from_json(dev_.to_json())
        dev.path = '/dev/sd' + name
        dev.sys_api = dict(dev_.sys_api, path='/dev/sd' + name)
        devs.append(dev)
    return Devices(devices=devs).devices
