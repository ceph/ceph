# -*- coding: utf-8 -*-

import argparse

from . import devices


class DriveGroups(object):

    help = "Generate Drive Groups from the local disc inventory"

    def __init__(self, argv):
        self.argv = argv

    def main(self):
        self.devices = devices.Devices()
        parser = argparse.ArgumentParser(
            prog='ceph-volume-propose',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self.help,
        )
        parser.add_argument(
            '--standalone',
            action='store_true',
            help='Standalone OSDs only',
        )
        self.args = parser.parse_args(self.argv)
        if self.args.standalone:
            self._standalone()
        else:
            self._default()

    def _standalone(self):
        ret = {
            'data_devices': [],
            'shared_devices': [],
        }
        for dev in self.devices.items():
            if dev.available:
                ret['data_devices'].append(dev)
        print(ret)

    def _default(self):
        ret = {
            'data_devices': [],
            'shared_devices': [],
        }
        for dev in self.devices.items():
            if dev.available:
                if dev.rotates:
                    ret['data_devices'].append(dev)
                else:
                    ret['shared_devices'].append(dev)
        if not ret['data_devices']:
            ret['data_devices'] = ret['shared_devices']
            ret['shared_devices'] = []
        print(ret)
