import argparse
from collections import namedtuple
import json
import logging
from textwrap import dedent
from ceph_volume import terminal, decorators
from ceph_volume.util import device, disk, prompt_bool, arg_validators, templates
from ceph_volume.util import prepare
from . import common
from .create import Create
from .prepare import Prepare
from typing import Any, Dict, List, Optional, Tuple

mlogger = terminal.MultiLogger(__name__)
logger = logging.getLogger(__name__)

DEFAULT_AUTO_BLOCK_DB_RATIO = 0.04
UNSET_REQUESTED_SIZE = object()


device_list_template = """
  * {path: <25} {size: <10} {state}"""


def ensure_disjoint_device_lists(data: List[device.Device],
                                 db: Optional[List[device.Device]] = None,
                                 wal: Optional[List[device.Device]] = None) -> None:
    if db is None:
        db = []
    if wal is None:
        wal = []
    # check that all device lists are disjoint with each other
    if not all([set(data).isdisjoint(set(db)),
                set(data).isdisjoint(set(wal)),
                set(db).isdisjoint(set(wal))]):
        raise Exception('Device lists are not disjoint')


def separate_devices_from_lvs(devices: List[device.Device]) -> Tuple[List[device.Device], List[device.Device]]:
    phys = []
    lvm = []
    for d in devices:
        phys.append(d) if d.is_device else lvm.append(d)
    return phys, lvm


def get_physical_fast_allocs(devices: List[device.Device], type_: str, fast_slots_per_device: int, new_osds: int, args: argparse.Namespace, requested_size: Any = UNSET_REQUESTED_SIZE) -> List[Tuple[str, float, disk.Size, int]]:
    requested_slots = getattr(args, '{}_slots'.format(type_))
    if not requested_slots or requested_slots < fast_slots_per_device:
        if requested_slots:
            mlogger.info('{}_slots argument is too small, ignoring'.format(type_))
        requested_slots = fast_slots_per_device

    if requested_size is UNSET_REQUESTED_SIZE:
        requested_size = getattr(args, '{}_size'.format(type_), 0)
        if not requested_size or requested_size == 0:
            # no size argument was specified, check ceph.conf
            get_size_fct = getattr(prepare, 'get_{}_size'.format(type_))
            requested_size = get_size_fct(lv_format=False)

    ret: List[Tuple[str, float, disk.Size, int]] = []
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

def group_devices_by_vg(devices: List[device.Device]) -> Dict[str, Any]:
    result: Dict[str, Any] = dict()
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

def get_lvm_fast_allocs(lvs: List[device.Device]) -> List[Tuple[str, float, disk.Size, int]]:
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

    def __init__(self, argv: List[str]) -> None:
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
        )
        parser.add_argument(
            '--no-auto',
            action='store_false',
            dest='auto',
            help=('deploy standalone OSDs if rotational and non-rotational drives '
                  'are passed in DEVICES'),
        )
        parser.add_argument(
            '--objectstore',
            dest='objectstore',
            help='The OSD objectstore.',
            default='bluestore',
            choices=['bluestore', 'seastore'],
            type=str,
        )
        parser.add_argument(
            '--bluestore',
            action='store_true',
            help='bluestore objectstore (default). (DEPRECATED: use --objectstore instead)',
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
            '--with-tpm',
            dest='with_tpm',
            help='Whether encrypted OSDs should be enrolled with TPM.',
            action='store_true'
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
        parser.add_argument(
            '--dmcrypt-format-opts',
            type=str,
            default=None,
            help="Additional cryptsetup luksFormat options (use the same syntax as the cryptsetup CLI)",
        )
        parser.add_argument(
            '--dmcrypt-open-opts',
            type=str,
            default=None,
            help="Additional cryptsetup luksOpen options (use the same syntax as the cryptsetup CLI)",
        )
        parser.add_argument(
            '--osd-collocate-on-fast',
            dest='osd_collocate_on_fast',
            action='store_true',
            help='Create an additional data-only OSD on each fast device (db/wal) using remaining space',
        )
        parser.add_argument(
            '--min-collocated-osd-size',
            dest='min_collocated_osd_size',
            type=disk.Size.parse,
            default=disk.Size(gb=10),
            help='Minimum size for collocated OSD on fast device (default: 10G)',
        )
        self.args = parser.parse_args(argv)
        if self.args.bluestore:
            self.args.objectstore = 'bluestore'
        self.parser = parser
        for dev_list in ['', 'db_', 'wal_']:
            setattr(self, '{}usable'.format(dev_list), [])

    def report(self, plan: List["OSD"]) -> None:
        report = self._create_report(plan)
        print(report)

    def _create_report(self, plan: List["OSD"]) -> str:
        result: str = ''
        if self.args.format == 'pretty':
            result += templates.total_osds.format(total_osds=len(plan))

            result += templates.osd_component_titles
            for osd in plan:
                result += templates.osd_header
                result += osd.report()
        else:
            json_report = []
            for osd in plan:
                json_report.append(osd.report_json())
            if self.args.format == 'json':
                result = json.dumps(json_report)
            elif self.args.format == 'json-pretty':
                result = json.dumps(json_report, indent=4,
                                  sort_keys=True)
        return result

    def _check_slot_args(self) -> None:
        '''
        checking if -slots args are consistent with other arguments
        '''
        if self.args.data_slots and self.args.osds_per_device:
            if self.args.data_slots < self.args.osds_per_device:
                raise ValueError('data_slots is smaller then osds_per_device')

    def _sort_rotational_disks(self) -> None:
        '''
        Helper for legacy auto behaviour.
        Sorts drives into rotating and non-rotating, the latter being used for
        db.
        '''
        mlogger.warning('DEPRECATION NOTICE')
        mlogger.warning('You are using the legacy automatic disk sorting behavior')
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
    def main(self) -> None:
        if not self.args.devices:
            self.parser.print_help()
            raise SystemExit(0)

        self.args.has_block_db_size_without_db_devices = (
            self.args.block_db_size is not None and not self.args.db_devices
        )

        if (self.args.auto and not self.args.db_devices and not
            self.args.wal_devices):
            self._sort_rotational_disks()

        self._check_slot_args()

        ensure_disjoint_device_lists(self.args.devices,
                                     self.args.db_devices,
                                     self.args.wal_devices)

        plan = self.get_deployment_layout()

        if self.args.report:
            self.report(plan)
            return

        if not self.args.yes:
            self.report(plan)
            terminal.info('The above OSDs would be created if the operation continues')
            if not prompt_bool('do you want to proceed? (yes/no)'):
                terminal.error('aborting OSD provisioning')
                raise SystemExit(0)

        self._execute(plan)

    def _execute(self, plan: List["OSD"]) -> None:
        defaults = common.get_default_args()
        global_args = [
            'objectstore',
            'bluestore',
            'dmcrypt',
            'with_tpm',
            'crush_device_class',
            'no_systemd',
            'dmcrypt_format_opts',
            'dmcrypt_open_opts',
        ]
        defaults.update({arg: getattr(self.args, arg) for arg in global_args})
        for osd in plan:
            args = osd.get_args(defaults)
            if self.args.prepare:
                p = Prepare([], args=argparse.Namespace(**args))
                p.main()
            else:
                c = Create([], args=argparse.Namespace(**args))
                c.create()

    def get_deployment_layout(self) -> List["OSD"]:
        '''
        The methods here are mostly just organization, error reporting and
        setting up of (default) args. The heavy lifting code for the deployment
        layout can be found in the static get_*_osds and get_*_fast_allocs
        functions.
        '''
        devices = self.args.devices
        if self.args.block_db_size is not None:
            fast_devices = self.args.db_devices or self.args.devices
        else:
            fast_devices = self.args.db_devices
        very_fast_devices = self.args.wal_devices
        plan = []
        phys_devs, lvm_devs = separate_devices_from_lvs(devices)
        mlogger.debug(('passed data devices: {} physical,'
                       ' {} LVM').format(len(phys_devs), len(lvm_devs)))

        plan.extend(get_physical_osds(phys_devs, self.args))

        plan.extend(get_lvm_osds(lvm_devs, self.args))

        num_osds = len(plan)
        if num_osds == 0:
            mlogger.info('All data devices are unavailable')
            return plan
        requested_osds = self.args.osds_per_device * len(phys_devs) + len(lvm_devs)

        fast_type = 'block_db'
        requested_fast_size = self._get_requested_fast_size(fast_type)
        fast_allocations = self.fast_allocations(fast_devices,
                                                 requested_osds,
                                                 num_osds,
                                                 fast_type,
                                                 requested_fast_size)
        if fast_devices and not fast_allocations:
            mlogger.info('{} fast devices were passed, but none are available'.format(len(fast_devices)))
            return []
        if fast_devices and not len(fast_allocations) == num_osds:
            mlogger.error('{} fast allocations != {} num_osds'.format(
                len(fast_allocations), num_osds))
            exit(1)

        requested_very_fast_size = self._get_requested_fast_size('block_wal')
        very_fast_allocations = self.fast_allocations(very_fast_devices,
                                                      requested_osds,
                                                      num_osds,
                                                      'block_wal',
                                                      requested_very_fast_size)
        if very_fast_devices and not very_fast_allocations:
            mlogger.info('{} very fast devices were passed, but none are available'.format(len(very_fast_devices)))
            return []
        if very_fast_devices and not len(very_fast_allocations) == num_osds:
            mlogger.error('{} very fast allocations != {} num_osds'.format(
                len(very_fast_allocations), num_osds))
            exit(1)

        if fast_devices:
            fast_alloc: Optional[tuple[str, float, disk.Size, int]] = None
            planned_fast_allocations: List[Tuple[str, float, disk.Size, int]] = []
            fast_device_sizes = {dev.path: dev.vg_size[0] for dev in fast_devices if dev.available_lvm}
            for osd in plan:
                if self.args.has_block_db_size_without_db_devices:
                    for i, _fast_alloc in enumerate(fast_allocations):
                        if osd.data.path == _fast_alloc[0]:
                            fast_alloc = fast_allocations.pop(i)
                            break
                else:
                    fast_alloc = fast_allocations.pop() if fast_allocations else None

                if fast_alloc:
                    fast_alloc = self._auto_size_collocated_fast_alloc(
                        osd,
                        fast_alloc,
                        fast_type,
                        requested_fast_size,
                        fast_device_sizes,
                    )
                    planned_fast_allocations.append(fast_alloc)
                    osd.add_fast_device(*fast_alloc, type_=fast_type)

            planned_very_fast_allocations = []
            if very_fast_devices and self.args.objectstore == 'bluestore':
                very_fast_alloc = very_fast_allocations.pop()
                planned_very_fast_allocations.append(very_fast_alloc)
                osd.add_very_fast_device(*very_fast_alloc)
        else:
            planned_fast_allocations = []
            planned_very_fast_allocations = []

        if getattr(self.args, 'osd_collocate_on_fast', False):
            if not fast_devices and not very_fast_devices:
                mlogger.error('--osd-collocate-on-fast requires --db-devices or --wal-devices')
                raise SystemExit(1)

            all_fast_allocs = planned_fast_allocations + planned_very_fast_allocations
            collocated_osds = self._create_collocated_osds(
                fast_devices or [],
                very_fast_devices or [],
                all_fast_allocs
            )
            plan.extend(collocated_osds)

        return plan

    def _create_collocated_osds(
        self,
        db_devices: List[device.Device],
        wal_devices: List[device.Device],
        fast_allocations: List[Tuple[str, float, disk.Size, int]]
    ) -> List["OSD"]:
        '''
        Create data-only OSDs on fast devices using remaining space after
        DB/WAL allocations.
        '''
        collocated: List["OSD"] = []

        all_fast_devices = {dev.path: dev for dev in db_devices + wal_devices}

        planned_usage: Dict[str, int] = {}
        for path, _, abs_size, _ in fast_allocations:
            if path not in planned_usage:
                planned_usage[path] = 0
            planned_usage[path] += int(abs_size)

        for dev_path, dev in all_fast_devices.items():
            if not dev.available_lvm:
                continue

            device_size = dev.vg_size[0]
            used_space = planned_usage.get(dev_path, 0)
            remaining_size = disk.Size(b=device_size - used_space)

            min_size = getattr(self.args, 'min_collocated_osd_size', disk.Size(gb=10))
            if remaining_size < min_size:
                mlogger.warning(
                    f'Remaining space on {dev_path} ({remaining_size}) is less than '
                    f'minimum ({min_size}), skipping collocated OSD'
                )
                continue

            rel_size = int(remaining_size) / device_size
            osd_id = self.args.osd_ids.pop() if self.args.osd_ids else None

            osd = Batch.OSD(
                data_path=dev_path,
                rel_size=rel_size,
                abs_size=remaining_size,
                slots=1,
                id_=osd_id,
                encryption='dmcrypt' if self.args.dmcrypt else None,
                symlink=dev.symlink,
                collocated=True
            )
            collocated.append(osd)
            mlogger.info(f'Adding collocated data OSD on {dev_path} with size {remaining_size}')

        return collocated

    def _get_requested_fast_size(self, type_: str) -> Optional[disk.Size]:
        requested_size = getattr(self.args, '{}_size'.format(type_), None)
        if requested_size is not None:
            return requested_size
        get_size_fct = getattr(prepare, 'get_{}_size'.format(type_))
        return get_size_fct(lv_format=False)

    def _auto_size_collocated_fast_alloc(self,
                                         osd: "OSD",
                                         fast_alloc: Tuple[str, float, disk.Size, int],
                                         type_: str,
                                         requested_size: Optional[disk.Size],
                                         fast_device_sizes: Dict[str, int]) -> Tuple[str, float, disk.Size, int]:
        if type_ != 'block_db' or requested_size is not None:
            return fast_alloc
        if not getattr(self.args, 'osd_collocate_on_fast', False):
            return fast_alloc
        fast_path, _, abs_size, slots = fast_alloc
        fast_device_size = fast_device_sizes.get(fast_path)
        if fast_device_size is None:
            return fast_alloc

        target_size = disk.Size(b=int(int(osd.data.abs_size) * DEFAULT_AUTO_BLOCK_DB_RATIO))
        target_size = min(target_size, abs_size)
        relative_size = int(target_size) / fast_device_size
        return (fast_path, relative_size, target_size, slots)

    def fast_allocations(self, devices: List[device.Device], requested_osds: int, new_osds: int, type_: str, requested_size: Any = UNSET_REQUESTED_SIZE) -> List[Tuple[str, float, disk.Size, int]]:
        ret: List[Tuple[str, float, disk.Size, int]] = []
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
                                            self.args,
                                            requested_size))
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
                     data_path: str,
                     rel_size: float,
                     abs_size: disk.Size,
                     slots: int,
                     id_: Optional[str] = None,
                     encryption: Optional[str] = None,
                     symlink: Optional[str] = None,
                     collocated: bool = False) -> None:
            self.id_ = id_
            self.data = self.VolSpec(path=data_path,
                                rel_size=rel_size,
                                abs_size=abs_size,
                                slots=slots,
                                type_='data')
            self.fast: Optional[Any] = None
            self.very_fast: Optional[Any] = None
            self.encryption: Optional[str] = encryption
            self.symlink: Optional[str] = symlink
            self.collocated: bool = collocated

        def add_fast_device(self, path: str, rel_size: float, abs_size: disk.Size, slots: int, type_: str) -> None:
            self.fast = self.VolSpec(path=path,
                                     rel_size=rel_size,
                                     abs_size=abs_size,
                                     slots=slots,
                                     type_=type_)

        def add_very_fast_device(self, path: str, rel_size: float, abs_size: disk.Size, slots: int) -> None:
            self.very_fast = self.VolSpec(path=path,
                                          rel_size=rel_size,
                                          abs_size=abs_size,
                                          slots=slots,
                                          type_='block_wal')

        def _get_osd_plan(self) -> Dict[str, Any]:
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

        def get_args(self, defaults: Dict[str, Any]) -> Dict[str, Any]:
            my_defaults = defaults.copy()
            my_defaults.update(self._get_osd_plan())
            return my_defaults

        def report(self) -> str:
            report = ''
            if self.collocated:
                report += templates.osd_collocated
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

        def report_json(self) -> Dict[str, Any]:
            # cast all values to string so that the report can be dumped in to
            # json.dumps
            return {k: str(v) for k, v in self._get_osd_plan().items()}

def get_physical_osds(devices: List[device.Device], args: argparse.Namespace) -> List[Batch.OSD]:
    """
    Goes through passed physical devices and assigns OSDs.
    """
    data_slots = max(args.data_slots, args.osds_per_device) if args.data_slots else args.osds_per_device
    ret = []

    for dev in devices:
        if not dev.available_lvm:
            continue

        total_dev_size = dev.vg_size[0]
        dev_size = total_dev_size
        rel_data_size = args.data_allocate_fraction / data_slots

        if args.has_block_db_size_without_db_devices:
            all_db_space = args.block_db_size * data_slots
            dev_size -= all_db_space.b.as_int()

        abs_size = disk.Size(b=int(dev_size * rel_data_size))

        if args.has_block_db_size_without_db_devices:
            rel_data_size = abs_size / disk.Size(b=total_dev_size)

        free_size = dev.vg_free[0]

        for _ in range(args.osds_per_device):
            if abs_size.b > free_size:
                break

            free_size -= abs_size.b
            osd_id = args.osd_ids.pop() if args.osd_ids else None

            ret.append(Batch.OSD(
                dev.path,
                rel_data_size,
                abs_size,
                args.osds_per_device,
                osd_id,
                'dmcrypt' if args.dmcrypt else None,
                dev.symlink
            ))

    return ret

def get_lvm_osds(lvs: List[device.Device], args: argparse.Namespace) -> List[Batch.OSD]:
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
