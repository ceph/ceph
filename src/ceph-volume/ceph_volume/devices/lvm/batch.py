import argparse
from collections import namedtuple
import json
import logging
from textwrap import dedent
from ceph_volume import terminal, decorators
from ceph_volume.util import disk, prompt_bool, arg_validators, templates
from ceph_volume.util import prepare
from . import common
from .create import Create
from .prepare import Prepare

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


def ensure_disjoint_device_lists(data, db=[], wal=[]):
    # check that all device lists are disjoint with each other
    if not all([set(data).isdisjoint(set(db)),
                set(data).isdisjoint(set(wal)),
                set(db).isdisjoint(set(wal))]):
        raise Exception('Device lists are not disjoint')


def separate_devices_from_lvs(devices):
    phys = []
    lvm = []
    for d in devices:
        phys.append(d) if d.is_device else lvm.append(d)
    return phys, lvm


def get_physical_osds(devices, args):
    '''
    Goes through passed physical devices and assigns OSDs
    '''
    data_slots = args.osds_per_device
    if args.data_slots:
        data_slots = max(args.data_slots, args.osds_per_device)
    rel_data_size = args.data_allocate_fraction / data_slots
    mlogger.debug('relative data size: {}'.format(rel_data_size))
    ret = []
    for dev in devices:
        if dev.available_lvm:
            dev_size = dev.vg_size[0]
            abs_size = disk.Size(b=int(dev_size * rel_data_size))
            free_size = dev.vg_free[0]
            for _ in range(args.osds_per_device):
                if abs_size > free_size:
                    break
                free_size -= abs_size.b
                osd_id = None
                if args.osd_ids:
                    osd_id = args.osd_ids.pop()
                ret.append(Batch.OSD(dev.path,
                                     rel_data_size,
                                     abs_size,
                                     args.osds_per_device,
                                     osd_id,
                                     'dmcrypt' if args.dmcrypt else None,
                                     dev.symlink))
    return ret


def get_lvm_osds(lvs, args):
    '''
    Goes through passed LVs and assigns planned osds
    '''
    ret = []
    for lv in lvs:
        if lv.used_by_ceph:
            continue
        osd_id = None
        if args.osd_ids:
            osd_id = args.osd_ids.pop()
        osd = Batch.OSD("{}/{}".format(lv.vg_name, lv.lv_name),
                        100.0,
                        disk.Size(b=int(lv.lvs[0].lv_size)),
                        1,
                        osd_id,
                        'dmcrypt' if args.dmcrypt else None)
        ret.append(osd)
    return ret


def get_physical_fast_allocs(devices, type_, fast_slots_per_device, new_osds, args):
    requested_slots = getattr(args, '{}_slots'.format(type_))
    if not requested_slots or requested_slots < fast_slots_per_device:
        if requested_slots:
            mlogger.info('{}_slots argument is too small, ignoring'.format(type_))
        requested_slots = fast_slots_per_device

    requested_size = getattr(args, '{}_size'.format(type_), 0)
    if not requested_size or requested_size == 0:
        # no size argument was specified, check ceph.conf
        get_size_fct = getattr(prepare, 'get_{}_size'.format(type_))
        requested_size = get_size_fct(lv_format=False)

    ret = []
    vg_device_map = group_devices_by_vg(devices)
    for vg_name, vg_devices in vg_device_map.items():
        for dev in vg_devices:
            if not dev.available_lvm:
                continue
            # any LV present is considered a taken slot
            occupied_slots = len(dev.lvs)
            # prior to v15.2.8, db/wal deployments were grouping multiple fast devices into single VGs - we need to
            # multiply requested_slots (per device) by the number of devices in the VG in order to ensure that
            # abs_size is calculated correctly from vg_size
            if vg_name == 'unused_devices':
                slots_for_vg = requested_slots
            else:
                if len(vg_devices) > 1:
                    slots_for_vg = len(args.devices)
                else:
                    slots_for_vg = len(vg_devices) * requested_slots
            dev_size = dev.vg_size[0]
            # this only looks at the first vg on device, unsure if there is a better
            # way
            abs_size = disk.Size(b=int(dev_size / slots_for_vg))
            free_size = dev.vg_free[0]
            relative_size = int(abs_size) / dev_size
            if requested_size:
                if requested_size <= abs_size:
                    abs_size = requested_size
                    relative_size = int(abs_size) / dev_size
                else:
                    mlogger.error(
                        '{} was requested for {}, but only {} can be fulfilled'.format(
                            requested_size,
                            '{}_size'.format(type_),
                            abs_size,
                        ))
                    exit(1)
            while abs_size <= free_size and len(ret) < new_osds and occupied_slots < fast_slots_per_device:
                free_size -= abs_size.b
                occupied_slots += 1
                ret.append((dev.path, relative_size, abs_size, requested_slots))
    return ret

def group_devices_by_vg(devices):
    result = dict()
    result['unused_devices'] = []
    for dev in devices:
        if len(dev.vgs) > 0:
            vg_name = dev.vgs[0].name
            if vg_name in result:
                result[vg_name].append(dev)
            else:
                result[vg_name] = [dev]
        else:
            result['unused_devices'].append(dev)
    return result

def get_lvm_fast_allocs(lvs):
    return [("{}/{}".format(d.vg_name, d.lv_name), 100.0,
             disk.Size(b=int(d.lvs[0].lv_size)), 1) for d in lvs if not
            d.journal_used_by_ceph]


class Batch(object):

    help = 'Automatically size devices for multi-OSD provisioning with minimal interaction'

    _help = dedent("""
    Automatically size devices ready for OSD provisioning based on default strategies.

    Usage:

        ceph-volume lvm batch [DEVICE...]

    Devices can be physical block devices or LVs.
    Optional reporting on possible outcomes is enabled with --report

        ceph-volume lvm batch --report [DEVICE...]
    """)

    def __init__(self, argv):
        parser = argparse.ArgumentParser(
            prog='ceph-volume lvm batch',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self._help,
        )

        parser.add_argument(
            'devices',
            metavar='DEVICES',
            nargs='*',
            type=arg_validators.ValidBatchDataDevice(),
            default=[],
            help='Devices to provision OSDs',
        )
        parser.add_argument(
            '--db-devices',
            nargs='*',
            type=arg_validators.ValidBatchDevice(),
            default=[],
            help='Devices to provision OSDs db volumes',
        )
        parser.add_argument(
            '--wal-devices',
            nargs='*',
            type=arg_validators.ValidBatchDevice(),
            default=[],
            help='Devices to provision OSDs wal volumes',
        )
        parser.add_argument(
            '--auto',
            action='store_true',
            help=('deploy multi-device OSDs if rotational and non-rotational drives '
                  'are passed in DEVICES'),
            default=True
        )
        parser.add_argument(
            '--no-auto',
            action='store_false',
            dest='auto',
            help=('deploy standalone OSDs if rotational and non-rotational drives '
                  'are passed in DEVICES'),
        )
        parser.add_argument(
            '--bluestore',
            action='store_true',
            help='bluestore objectstore (default)',
        )
        parser.add_argument(
            '--report',
            action='store_true',
            help='Only report on OSD that would be created and exit',
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
            choices=['json', 'json-pretty', 'pretty'],
        )
        parser.add_argument(
            '--dmcrypt',
            action=arg_validators.DmcryptAction,
            help='Enable device encryption via dm-crypt',
        )
        parser.add_argument(
            '--crush-device-class',
            dest='crush_device_class',
            help='Crush device class to assign this OSD to',
            default=""
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
            '--data-slots',
            type=int,
            help=('Provision more than 1 (the default) OSD slot per device'
                  ' if more slots then osds-per-device are specified, slots'
                  'will stay unoccupied'),
        )
        parser.add_argument(
            '--data-allocate-fraction',
            type=arg_validators.ValidFraction(),
            help='Fraction to allocate from data device (0,1.0]',
            default=1.0
        )
        parser.add_argument(
            '--block-db-size',
            type=disk.Size.parse,
            help='Set (or override) the "bluestore_block_db_size" value, in bytes'
        )
        parser.add_argument(
            '--block-db-slots',
            type=int,
            help='Provision slots on DB device, can remain unoccupied'
        )
        parser.add_argument(
            '--block-wal-size',
            type=disk.Size.parse,
            help='Set (or override) the "bluestore_block_wal_size" value, in bytes'
        )
        parser.add_argument(
            '--block-wal-slots',
            type=int,
            help='Provision slots on WAL device, can remain unoccupied'
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
            type=arg_validators.valid_osd_id
        )
        self.args = parser.parse_args(argv)
        self.parser = parser
        for dev_list in ['', 'db_', 'wal_']:
            setattr(self, '{}usable'.format(dev_list), [])

    def report(self, plan):
        report = self._create_report(plan)
        print(report)

    def _create_report(self, plan):
        if self.args.format == 'pretty':
            report = ''
            report += templates.total_osds.format(total_osds=len(plan))

            report += templates.osd_component_titles
            for osd in plan:
                report += templates.osd_header
                report += osd.report()
            return report
        else:
            json_report = []
            for osd in plan:
                json_report.append(osd.report_json())
            if self.args.format == 'json':
                return json.dumps(json_report)
            elif self.args.format == 'json-pretty':
                return json.dumps(json_report, indent=4,
                                  sort_keys=True)

    def _check_slot_args(self):
        '''
        checking if -slots args are consistent with other arguments
        '''
        if self.args.data_slots and self.args.osds_per_device:
            if self.args.data_slots < self.args.osds_per_device:
                raise ValueError('data_slots is smaller then osds_per_device')

    def _sort_rotational_disks(self):
        '''
        Helper for legacy auto behaviour.
        Sorts drives into rotating and non-rotating, the latter being used for
        db.
        '''
        mlogger.warning('DEPRECATION NOTICE')
        mlogger.warning('You are using the legacy automatic disk sorting behavior')
        mlogger.warning('The Pacific release will change the default to --no-auto')
        rotating = []
        ssd = []
        for d in self.args.devices:
            rotating.append(d) if d.rotational else ssd.append(d)
        if ssd and not rotating:
            # no need for additional sorting, we'll only deploy standalone on ssds
            return
        self.args.devices = rotating
        self.args.db_devices = ssd

    @decorators.needs_root
    def main(self):
        if not self.args.devices:
            return self.parser.print_help()

        # Default to bluestore here since defaulting it in add_argument may
        # cause both to be True
        if not self.args.bluestore:
            self.args.bluestore = True

        if (self.args.auto and not self.args.db_devices and not
            self.args.wal_devices):
            self._sort_rotational_disks()

        self._check_slot_args()

        ensure_disjoint_device_lists(self.args.devices,
                                     self.args.db_devices,
                                     self.args.wal_devices)

        plan = self.get_plan(self.args)

        if self.args.report:
            self.report(plan)
            return 0

        if not self.args.yes:
            self.report(plan)
            terminal.info('The above OSDs would be created if the operation continues')
            if not prompt_bool('do you want to proceed? (yes/no)'):
                terminal.error('aborting OSD provisioning')
                raise SystemExit(0)

        self._execute(plan)

    def _execute(self, plan):
        defaults = common.get_default_args()
        global_args = [
            'bluestore',
            'dmcrypt',
            'crush_device_class',
            'no_systemd',
        ]
        defaults.update({arg: getattr(self.args, arg) for arg in global_args})
        for osd in plan:
            args = osd.get_args(defaults)
            if self.args.prepare:
                p = Prepare([])
                p.safe_prepare(argparse.Namespace(**args))
            else:
                c = Create([])
                c.create(argparse.Namespace(**args))


    def get_plan(self, args):
        if args.bluestore:
            plan = self.get_deployment_layout(args, args.devices, args.db_devices,
                                              args.wal_devices)
        return plan

    def get_deployment_layout(self, args, devices, fast_devices=[],
                              very_fast_devices=[]):
        '''
        The methods here are mostly just organization, error reporting and
        setting up of (default) args. The heavy lifting code for the deployment
        layout can be found in the static get_*_osds and get_*_fast_allocs
        functions.
        '''
        plan = []
        phys_devs, lvm_devs = separate_devices_from_lvs(devices)
        mlogger.debug(('passed data devices: {} physical,'
                       ' {} LVM').format(len(phys_devs), len(lvm_devs)))

        plan.extend(get_physical_osds(phys_devs, args))

        plan.extend(get_lvm_osds(lvm_devs, args))

        num_osds = len(plan)
        if num_osds == 0:
            mlogger.info('All data devices are unavailable')
            return plan
        requested_osds = args.osds_per_device * len(phys_devs) + len(lvm_devs)

        if args.bluestore:
            fast_type = 'block_db'
        fast_allocations = self.fast_allocations(fast_devices,
                                                 requested_osds,
                                                 num_osds,
                                                 fast_type)
        if fast_devices and not fast_allocations:
            mlogger.info('{} fast devices were passed, but none are available'.format(len(fast_devices)))
            return []
        if fast_devices and not len(fast_allocations) == num_osds:
            mlogger.error('{} fast allocations != {} num_osds'.format(
                len(fast_allocations), num_osds))
            exit(1)

        very_fast_allocations = self.fast_allocations(very_fast_devices,
                                                      requested_osds,
                                                      num_osds,
                                                      'block_wal')
        if very_fast_devices and not very_fast_allocations:
            mlogger.info('{} very fast devices were passed, but none are available'.format(len(very_fast_devices)))
            return []
        if very_fast_devices and not len(very_fast_allocations) == num_osds:
            mlogger.error('{} very fast allocations != {} num_osds'.format(
                len(very_fast_allocations), num_osds))
            exit(1)

        for osd in plan:
            if fast_devices:
                osd.add_fast_device(*fast_allocations.pop(),
                                    type_=fast_type)
            if very_fast_devices and args.bluestore:
                osd.add_very_fast_device(*very_fast_allocations.pop())
        return plan

    def fast_allocations(self, devices, requested_osds, new_osds, type_):
        ret = []
        if not devices:
            return ret
        phys_devs, lvm_devs = separate_devices_from_lvs(devices)
        mlogger.debug(('passed {} devices: {} physical,'
                       ' {} LVM').format(type_, len(phys_devs), len(lvm_devs)))

        ret.extend(get_lvm_fast_allocs(lvm_devs))

        # fill up uneven distributions across fast devices: 5 osds and 2 fast
        # devices? create 3 slots on each device rather then deploying
        # heterogeneous osds
        slot_divider = max(1, len(phys_devs))
        if (requested_osds - len(lvm_devs)) % slot_divider:
            fast_slots_per_device = int((requested_osds - len(lvm_devs)) / slot_divider) + 1
        else:
            fast_slots_per_device = int((requested_osds - len(lvm_devs)) / slot_divider)


        ret.extend(get_physical_fast_allocs(phys_devs,
                                            type_,
                                            fast_slots_per_device,
                                            new_osds,
                                            self.args))
        return ret

    class OSD(object):
        '''
        This class simply stores info about to-be-deployed OSDs and provides an
        easy way to retrieve the necessary create arguments.
        '''
        VolSpec = namedtuple('VolSpec',
                             ['path',
                              'rel_size',
                              'abs_size',
                              'slots',
                              'type_'])

        def __init__(self,
                     data_path,
                     rel_size,
                     abs_size,
                     slots,
                     id_,
                     encryption,
                     symlink=None):
            self.id_ = id_
            self.data = self.VolSpec(path=data_path,
                                rel_size=rel_size,
                                abs_size=abs_size,
                                slots=slots,
                                type_='data')
            self.fast = None
            self.very_fast = None
            self.encryption = encryption
            self.symlink = symlink

        def add_fast_device(self, path, rel_size, abs_size, slots, type_):
            self.fast = self.VolSpec(path=path,
                                rel_size=rel_size,
                                abs_size=abs_size,
                                slots=slots,
                                type_=type_)

        def add_very_fast_device(self, path, rel_size, abs_size, slots):
            self.very_fast = self.VolSpec(path=path,
                                rel_size=rel_size,
                                abs_size=abs_size,
                                slots=slots,
                                type_='block_wal')

        def _get_osd_plan(self):
            plan = {
                'data': self.data.path,
                'data_size': self.data.abs_size,
                'encryption': self.encryption,
            }
            if self.fast:
                type_ = self.fast.type_.replace('.', '_')
                plan.update(
                    {
                        type_: self.fast.path,
                        '{}_size'.format(type_): self.fast.abs_size,
                    })
            if self.very_fast:
                plan.update(
                    {
                        'block_wal': self.very_fast.path,
                        'block_wal_size': self.very_fast.abs_size,
                    })
            if self.id_:
                plan.update({'osd_id': self.id_})
            return plan

        def get_args(self, defaults):
            my_defaults = defaults.copy()
            my_defaults.update(self._get_osd_plan())
            return my_defaults

        def report(self):
            report = ''
            if self.id_:
                report += templates.osd_reused_id.format(
                    id_=self.id_)
            if self.encryption:
                report += templates.osd_encryption.format(
                    enc=self.encryption)
            path = self.data.path
            if self.symlink:
                path = f'{self.symlink} -> {self.data.path}'
            report += templates.osd_component.format(
                _type=self.data.type_,
                path=path,
                size=self.data.abs_size,
                percent=self.data.rel_size)
            if self.fast:
                report += templates.osd_component.format(
                    _type=self.fast.type_,
                    path=self.fast.path,
                    size=self.fast.abs_size,
                    percent=self.fast.rel_size)
            if self.very_fast:
                report += templates.osd_component.format(
                    _type=self.very_fast.type_,
                    path=self.very_fast.path,
                    size=self.very_fast.abs_size,
                    percent=self.very_fast.rel_size)
            return report

        def report_json(self):
            # cast all values to string so that the report can be dumped in to
            # json.dumps
            return {k: str(v) for k, v in self._get_osd_plan().items()}
