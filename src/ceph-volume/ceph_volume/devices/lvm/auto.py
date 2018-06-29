import argparse
from textwrap import dedent
from ceph_volume import terminal, decorators
from ceph_volume.util import disk
from . import strategies


device_list_template = """
  * {path: <25} {size: <10} {state}"""


def device_formatter(devices):
    lines = []
    for path, details in devices:
        lines.append(device_list_template.format(
            path=path, size=details['human_readable_size'],
            state='solid' if details['rotational'] == '0' else 'rotational')
        )

    return ''.join(lines)


# Scenario filtering/detection
def bluestore_single_type(device_facts):
    """
    Detect devices that are just HDDs or solid state so that a 1:1
    device-to-osd provisioning can be done
    """
    types = [device['rotational'] for device in device_facts]
    if len(set(types)) == 1:
        return strategies.bluestore.SingleType


def bluestore_mixed_type(device_facts):
    """
    Detect if devices are HDDs as well as solid state so that block.db can be
    placed in solid devices while data is kept in the spinning drives.
    """
    types = [device['rotational'] for device in device_facts]
    if len(set(types)) > 1:
        return strategies.bluestore.MixedType


def filestore_single_type(device_facts):
    """
    Detect devices that are just HDDs or solid state so that a 1:1
    device-to-osd provisioning can be done, keeping the journal on the OSD
    """
    types = [device['rotational'] for device in device_facts]
    if len(set(types)) == 1:
        return strategies.filestore.SingleType


def filestore_mixed_type(device_facts):
    """
    Detect if devices are HDDs as well as solid state so that the journal can be
    placed in solid devices while data is kept in the spinning drives.
    """
    types = [device['rotational'] for device in device_facts]
    if len(set(types)) > 1:
        return strategies.filestore.MixedType


def get_strategy(devices, args):
    """
    Given a set of devices as input, go through the different detection
    mechanisms to narrow down on a strategy to use. The strategies are 4 in
    total:

    * Single device type on Bluestore
    * Mixed device types on Bluestore
    * Single device type on Filestore
    * Mixed device types on Filestore

    When the function matches to a scenario it returns the strategy class. This
    allows for dynamic loading of the conditions needed for each scenario, with
    normalized classes
    """
    bluestore_strategies = [bluestore_mixed_type, bluestore_single_type]
    filestore_strategies = [filestore_mixed_type, filestore_single_type]
    if args.bluestore:
        strategies = bluestore_strategies
    else:
        strategies = filestore_strategies

    for strategy in strategies:
        backend = strategy(devices)
        if backend:
            return backend(devices)


class Auto(object):

    help = 'Automatically size devices for multi-OSD provisioning with minimal interaction'

    _help = dedent("""
    Automatically size devices ready for OSD provisioning based on default strategies.

    Detected devices:
    {detected_devices}

    Usage:

        ceph-volume lvm auto [{device}..]

    Optional reporting on possible outcomes is enabled with --report

        ceph-volume lvm auto --report [{device}..]
    """)

    def __init__(self, argv):
        self.argv = argv

    def get_devices(self):
        all_devices = disk.get_devices()
        # remove devices with partitions
        # XXX Should be optional when getting device info
        for device, detail in all_devices.items():
            if detail.get('partitions') != {}:
                del all_devices[device]
        devices = sorted(all_devices.items(), key=lambda x: (x[0], x[1]['size']))
        return device_formatter(devices)

    def print_help(self, sub_help):
        return self._help.format(
            detected_devices=self.get_devices(),
        )

    def get_filtered_devices(self, devices):
        """
        Parse all devices in the current system and keep only the ones that are
        being explicity passed in
        """
        system_devices = disk.get_devices()
        if not devices:
            return system_devices
        parsed_devices = []
        for device in devices:
            try:
                parsed_devices.append(system_devices[device])
            except KeyError:
                continue

        return parsed_devices

    def report(self, args):
        strategy = get_strategy(self.get_filtered_devices(args.devices), args)
        if args.format == 'pretty':
            strategy.report_pretty()
        elif args.format == 'json':
            strategy.report_json()
        else:
            raise RuntimeError('report format must be "pretty" or "json"')

    @decorators.needs_root
    def main(self):
        terminal.dispatch(self.mapper, self.argv)
        parser = argparse.ArgumentParser(
            prog='ceph-volume auto',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self.print_help(terminal.subhelp(self.mapper)),
        )
        parser.add_argument(
            'devices',
            metavar='DEVICES',
            nargs='*',
            default=[],
            help='Devices to provision OSDs',
        )
        parser.add_argument(
            '--bluestore',
            action='store_true',
            help='bluestore objectstore (default)',
        )
        parser.add_argument(
            '--filestore',
            action='store_true',
            help='filestore objectstore',
        )
        parser.add_argument(
            '--report',
            action='store_true',
            help='Autodetect the objectstore by inspecting the OSD',
        )
        parser.add_argument(
            '--format',
            help='output format, defaults to "pretty"',
            default='pretty',
            choices=['json', 'pretty'],
        )
        parser.add_argument(
            '--no-systemd',
            dest='no_systemd',
            action='store_true',
            help='Skip creating and enabling systemd units and starting OSD services',
        )
        args = parser.parse_args(self.argv)
        if len(self.argv) <= 1:
            return parser.print_help()
        # Default to bluestore here since defaulting it in add_argument may
        # cause both to be True
        if not args.bluestore and not args.filestore:
            args.bluestore = True

        if args.report:
            self.report(args)
