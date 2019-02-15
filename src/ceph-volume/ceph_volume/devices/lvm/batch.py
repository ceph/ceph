import argparse
import logging
from textwrap import dedent
from ceph_volume import terminal, decorators
from ceph_volume.util import disk, prompt_bool
from ceph_volume.util import arg_validators
from . import strategies

mlogger = terminal.MultiLogger(__name__)
logger = logging.getLogger(__name__)


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
    types = [device.sys_api['rotational'] for device in device_facts]
    if len(set(types)) == 1:
        return strategies.bluestore.SingleType


def bluestore_mixed_type(device_facts):
    """
    Detect if devices are HDDs as well as solid state so that block.db can be
    placed in solid devices while data is kept in the spinning drives.
    """
    types = [device.sys_api['rotational'] for device in device_facts]
    if len(set(types)) > 1:
        return strategies.bluestore.MixedType


def filestore_single_type(device_facts):
    """
    Detect devices that are just HDDs or solid state so that a 1:1
    device-to-osd provisioning can be done, keeping the journal on the OSD
    """
    types = [device.sys_api['rotational'] for device in device_facts]
    if len(set(types)) == 1:
        return strategies.filestore.SingleType


def filestore_mixed_type(device_facts):
    """
    Detect if devices are HDDs as well as solid state so that the journal can be
    placed in solid devices while data is kept in the spinning drives.
    """
    types = [device.sys_api['rotational'] for device in device_facts]
    if len(set(types)) > 1:
        return strategies.filestore.MixedType


def get_strategy(args, devices):
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
            return backend


def filter_devices(args):
    unused_devices = [device for device in args.devices if not device.used_by_ceph]
    # only data devices, journals can be reused
    used_devices = [device.abspath for device in args.devices if device.used_by_ceph]
    filtered_devices = {}
    if used_devices:
        for device in used_devices:
            filtered_devices[device] = {"reasons": ["Used by ceph as a data device already"]}
        logger.info("Ignoring devices already used by ceph: %s" % ", ".join(used_devices))
    if len(unused_devices) == 1:
        last_device = unused_devices[0]
        if not last_device.rotational and last_device.is_lvm_member:
            reason = "Used by ceph as a %s already and there are no devices left for data/block" % (
                last_device.lvs[0].tags.get("ceph.type"),
            )
            filtered_devices[last_device.abspath] = {"reasons": [reason]}
            logger.info(reason + ": %s" % last_device.abspath)
            unused_devices = []

    return unused_devices, filtered_devices


class Batch(object):

    help = 'Automatically size devices for multi-OSD provisioning with minimal interaction'

    _help = dedent("""
    Automatically size devices ready for OSD provisioning based on default strategies.

    Detected devices:
    {detected_devices}

    Usage:

        ceph-volume lvm batch [DEVICE...]

    Optional reporting on possible outcomes is enabled with --report

        ceph-volume lvm batch --report [DEVICE...]
    """)

    def __init__(self, argv):
        parser = argparse.ArgumentParser(
            prog='ceph-volume lvm batch',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self.print_help(),
        )

        parser.add_argument(
            'devices',
            metavar='DEVICES',
            nargs='*',
            type=arg_validators.ValidDevice(),
            default=[],
            help='Devices to provision OSDs',
        )
        parser.add_argument(
            '--db-devices',
            nargs='*',
            type=arg_validators.ValidDevice(),
            default=[],
            help='Devices to provision OSDs db volumes',
        )
        parser.add_argument(
            '--wal-devices',
            nargs='*',
            type=arg_validators.ValidDevice(),
            default=[],
            help='Devices to provision OSDs wal volumes',
        )
        parser.add_argument(
            '--journal-devices',
            nargs='*',
            type=arg_validators.ValidDevice(),
            default=[],
            help='Devices to provision OSDs journal volumes',
        )
        parser.add_argument(
            '--no-auto',
            action='store_true',
            help=('deploy standalone OSDs if rotational and non-rotational drives '
                  'are passed in DEVICES'),
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
            '--yes',
            action='store_true',
            help='Avoid prompting for confirmation when provisioning',
        )
        parser.add_argument(
            '--format',
            help='output format, defaults to "pretty"',
            default='pretty',
            choices=['json', 'pretty'],
        )
        parser.add_argument(
            '--dmcrypt',
            action='store_true',
            help='Enable device encryption via dm-crypt',
        )
        parser.add_argument(
            '--crush-device-class',
            dest='crush_device_class',
            help='Crush device class to assign this OSD to',
        )
        parser.add_argument(
            '--no-systemd',
            dest='no_systemd',
            action='store_true',
            help='Skip creating and enabling systemd units and starting OSD services',
        )
        parser.add_argument(
            '--osds-per-device',
            type=int,
            default=1,
            help='Provision more than 1 (the default) OSD per device',
        )
        parser.add_argument(
            '--block-db-size',
            type=int,
            help='Set (or override) the "bluestore_block_db_size" value, in bytes'
        )
        parser.add_argument(
            '--block-wal-size',
            type=int,
            help='Set (or override) the "bluestore_block_wal_size" value, in bytes'
        )
        parser.add_argument(
            '--journal-size',
            type=int,
            help='Override the "osd_journal_size" value, in megabytes'
        )
        parser.add_argument(
            '--prepare',
            action='store_true',
            help='Only prepare all OSDs, do not activate',
        )
        parser.add_argument(
            '--osd-ids',
            nargs='*',
            default=[],
            help='Reuse existing OSD ids',
        )
        self.args = parser.parse_args(argv)
        self.parser = parser
        for dev_list in ['', 'db_', 'wal_', 'journal_']:
            setattr(self, '{}usable'.format(dev_list), [])

    def get_devices(self):
        # remove devices with partitions
        devices = [(device, details) for device, details in
                       disk.get_devices().items() if details.get('partitions') == {}]
        size_sort = lambda x: (x[0], x[1]['size'])
        return device_formatter(sorted(devices, key=size_sort))

    def print_help(self):
        return self._help.format(
            detected_devices=self.get_devices(),
        )

    def report(self):
        if self.args.format == 'pretty':
            self.strategy.report_pretty(self.filtered_devices)
        elif self.args.format == 'json':
            self.strategy.report_json(self.filtered_devices)
        else:
            raise RuntimeError('report format must be "pretty" or "json"')

    def execute(self):
        if not self.args.yes:
            self.strategy.report_pretty(self.filtered_devices)
            terminal.info('The above OSDs would be created if the operation continues')
            if not prompt_bool('do you want to proceed? (yes/no)'):
                devices = ','.join([device.abspath for device in self.args.devices])
                terminal.error('aborting OSD provisioning for %s' % devices)
                raise SystemExit(0)

        self.strategy.execute()

    def _get_strategy(self):
        strategy = get_strategy(self.args, self.args.devices)
        unused_devices, self.filtered_devices = filter_devices(self.args)
        if not unused_devices and not self.args.format == 'json':
            # report nothing changed
            mlogger.info("All devices are already used by ceph. No OSDs will be created.")
            raise SystemExit(0)
        else:
            new_strategy = get_strategy(self.args, unused_devices)
            if new_strategy and strategy != new_strategy:
                mlogger.error("Aborting because strategy changed from %s to %s after filtering" % (strategy.type(), new_strategy.type()))
                raise SystemExit(1)

        self.strategy = strategy.with_auto_devices(self.args, unused_devices)

    @decorators.needs_root
    def main(self):
        if not self.args.devices:
            return self.parser.print_help()

        # Default to bluestore here since defaulting it in add_argument may
        # cause both to be True
        if not self.args.bluestore and not self.args.filestore:
            self.args.bluestore = True

        if (self.args.no_auto or self.args.db_devices or
                                  self.args.journal_devices or
                                  self.args.wal_devices):
            self._get_explicit_strategy()
        else:
            self._get_strategy()

        if self.args.report:
            self.report()
        else:
            self.execute()

    def _get_explicit_strategy(self):
        # TODO assert that none of the device lists overlap?
        self._filter_devices()
        if self.args.bluestore:
            if self.db_usable or self.wal_usable:
                self.strategy = strategies.bluestore.MixedType(
                    self.args,
                    self.usable,
                    self.db_usable,
                    self.wal_usable)
            else:
                self.strategy = strategies.bluestore.SingleType(
                    self.args,
                    self.usable)
        else:
            if self.journal_usable:
                self.strategy = strategies.filestore.MixedType(
                    self.args,
                    self.usable,
                    self.journal_usable)
            else:
                self.strategy = strategies.filestore.SingleType(
                    self.args,
                    self.usable)


    def _filter_devices(self):
        # filter devices by their available property.
        # TODO: Some devices are rejected in the argparser already. maybe it
        # makes sense to unifiy this
        used_reason = {"reasons": ["Used by ceph as a data device already"]}
        self.filtered_devices = {}
        for dev_list in ['', 'db_', 'wal_', 'journal_']:
            dev_list_prop = '{}devices'.format(dev_list)
            if hasattr(self.args, dev_list_prop):
                usable_dev_list_prop = '{}usable'.format(dev_list)
                usable = [d for d in getattr(self.args, dev_list_prop) if d.available]
                setattr(self, usable_dev_list_prop, usable)
                self.filtered_devices.update({d: used_reason for d in
                                              getattr(self.args, dev_list_prop)
                                              if d.used_by_ceph})
