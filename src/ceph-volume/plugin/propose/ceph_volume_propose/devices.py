# -*- coding: utf-8 -*-

from ceph_volume.util import disk
from ceph_volume.util.system import get_mounts

block_device_template = """
{dev:<25} {size:<12} {rot:<7} {model}"""


class Devices(object):

    def __init__(self, devices=None):
        if not devices:
            devices = disk.get_devices()
        self.devices = [Device(k, v) for k, v in
                        self._annotate_devices(devices).items()]

    def items(self):
        for device in self.devices:
            yield device

    def _annotate_devices(self, devices):
        """
        adds some extra info the the devices structure
        """
        mounts = get_mounts()
        mounted_devs = mounts.keys()
        for device in devices:
            devices[device]['mounted'] = '0' if device in mounted_devs else '1'
        return devices

    def pretty_report(self, all=True):
        output = [
            block_device_template.format(
                dev='Device Path',
                size='Size',
                rot='rotates',
                model='Model name',
            )]
        for device in self.devices:
            if device.available or all:
                output.append(
                    block_device_template.format(
                        dev=device.device,
                        size=device.attrs['human_readable_size'],
                        rot='true' if device.attrs['rotational'] == 0
                            else 'false',
                        model=device.attrs['model'],
                    )
                )
        return ''.join(output)


class Device(object):

    def __init__(self, device, attrs):
        self.device = device
        self.attrs = attrs

    def __repr__(self):
        return self.device

    def __str_(self):
        return self.device

    @property
    def available(self):
        return self.attrs['mounted'] != '0' and self.attrs['partitions'] == {}

    @property
    def rotates(self):
        return self.attrs['rotational'] == 1
