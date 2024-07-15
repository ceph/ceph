# host_facts.py - classes/functions for gathering metadata on the host

import ipaddress
import json
import logging
import os
import platform
import re
import string
import time

from glob import glob
from pathlib import Path

from typing import Any, cast, Dict, List, Optional, Set, Union

from cephadmlib.call_wrappers import call, call_throws, CallVerbosity
from cephadmlib.context import CephadmContext
from cephadmlib.data_utils import bytes_to_human
from cephadmlib.exe_utils import find_executable
from cephadmlib.file_utils import read_file
from cephadmlib.net_utils import get_fqdn, get_ipv4_address, get_ipv6_address

logger = logging.getLogger()


class Enclosure:
    def __init__(self, enc_id: str, enc_path: str, dev_path: str):
        """External disk enclosure metadata

        Args:
        :param enc_id: enclosure id (normally a WWN)
        :param enc_path: sysfs path to HBA attached to the enclosure
                         e.g. /sys/class/scsi_generic/sg11/device/enclosure/0:0:9:0
        :param dev_path: sysfs path to the generic scsi device for the enclosure HBA
                         e.g. /sys/class/scsi_generic/sg2
        """
        self._path: str = dev_path
        self._dev_path: str = os.path.join(dev_path, 'device')
        self._enc_path: str = enc_path
        self.ses_paths: List[str] = []
        self.path_count: int = 0
        self.vendor: str = ''
        self.model: str = ''
        self.enc_id: str = enc_id
        self.components: Union[int, str] = 0
        self.device_lookup: Dict[str, str] = {}
        self.device_count: int = 0
        self.slot_map: Dict[str, Dict[str, str]] = {}

        self._probe()

    def _probe(self) -> None:
        """Analyse the dev paths to identify enclosure related information"""

        self.vendor = read_file([os.path.join(self._dev_path, 'vendor')])
        self.model = read_file([os.path.join(self._dev_path, 'model')])
        self.components = read_file(
            [os.path.join(self._enc_path, 'components')]
        )
        slot_paths = glob(os.path.join(self._enc_path, '*', 'slot'))
        for slot_path in slot_paths:
            slot = read_file([slot_path])
            serial_path = os.path.join(
                os.path.dirname(slot_path), 'device', 'vpd_pg80'
            )
            serial = ''
            if os.path.exists(serial_path):
                serial_raw = read_file([serial_path])
                serial = (
                    ''.join(
                        char
                        for char in serial_raw
                        if char in string.printable
                    )
                ).strip()
                self.device_lookup[serial] = slot
            slot_dir = os.path.dirname(slot_path)
            self.slot_map[slot] = {
                'status': read_file([os.path.join(slot_dir, 'status')]),
                'fault': read_file([os.path.join(slot_dir, 'fault')]),
                'locate': read_file([os.path.join(slot_dir, 'locate')]),
                'serial': serial,
            }

        self.device_count = len(self.device_lookup)
        self.update(os.path.basename(self._path))

    def update(self, dev_id: str) -> None:
        """Update an enclosure object with a related sg device name

        :param dev_id (str): device name e.g. sg2
        """
        self.ses_paths.append(dev_id)
        self.path_count = len(self.ses_paths)

    def _dump(self) -> Dict[str, Any]:
        """Return a dict representation of the object"""
        return {
            k: v for k, v in self.__dict__.items() if not k.startswith('_')
        }

    def __str__(self) -> str:
        """Return a formatted json representation of the object as a string"""
        return json.dumps(self._dump(), indent=2)

    def __repr__(self) -> str:
        """Return a json representation of the object as a string"""
        return json.dumps(self._dump())

    def as_json(self) -> Dict[str, Any]:
        """Return a dict representing the object"""
        return self._dump()


class HostFacts:
    _dmi_path_list = ['/sys/class/dmi/id']
    _nic_path_list = ['/sys/class/net']
    _apparmor_path_list = ['/etc/apparmor']
    _disk_vendor_workarounds = {'0x1af4': 'Virtio Block Device'}
    _excluded_block_devices = ('sr', 'zram', 'dm-', 'loop', 'md')
    _sg_generic_glob = '/sys/class/scsi_generic/*'

    def __init__(self, ctx: CephadmContext):
        self.ctx: CephadmContext = ctx
        self.cpu_model: str = 'Unknown'
        self.sysctl_options: Dict[str, str] = self._populate_sysctl_options()
        self.cpu_count: int = 0
        self.cpu_cores: int = 0
        self.cpu_threads: int = 0
        self.interfaces: Dict[str, Any] = {}

        self._meminfo: List[str] = read_file(['/proc/meminfo']).splitlines()
        self._get_cpuinfo()
        self._process_nics()
        self.arch: str = platform.processor()
        self.kernel: str = platform.release()
        self._enclosures = self._discover_enclosures()
        self._block_devices = self._get_block_devs()
        self._device_list = self._get_device_info()

    def _populate_sysctl_options(self) -> Dict[str, str]:
        sysctl_options = {}
        out, _, _ = call_throws(
            self.ctx,
            ['sysctl', '-a'],
            verbosity=CallVerbosity.QUIET_UNLESS_ERROR,
        )
        if out:
            for line in out.splitlines():
                option, value = line.split('=')
                sysctl_options[option.strip()] = value.strip()
        return sysctl_options

    def _discover_enclosures(self) -> Dict[str, Enclosure]:
        """Build a dictionary of discovered scsi enclosures

        Enclosures are detected by walking the scsi generic sysfs hierarchy.
        Any device tree that holds an 'enclosure' subdirectory is interpreted as
        an enclosure. Once identified the enclosure directory is analysis to
        identify key descriptors that will help relate disks to enclosures and
        disks to enclosure slots.

        :return: Dict[str, Enclosure]: a map of enclosure id (hex) to enclosure object
        """
        sg_paths: List[str] = glob(HostFacts._sg_generic_glob)
        enclosures: Dict[str, Enclosure] = {}

        for sg_path in sg_paths:
            enc_path = os.path.join(sg_path, 'device', 'enclosure')
            if os.path.exists(enc_path):
                enc_dirs = glob(os.path.join(enc_path, '*'))
                if len(enc_dirs) != 1:
                    # incomplete enclosure spec - expecting ONE dir in the format
                    # host(adapter):bus:target:lun e.g. 16:0:0:0
                    continue
                enc_path = enc_dirs[0]
                enc_id = read_file([os.path.join(enc_path, 'id')])
                if enc_id in enclosures:
                    enclosures[enc_id].update(os.path.basename(sg_path))
                    continue

                enclosure = Enclosure(enc_id, enc_path, sg_path)
                enclosures[enc_id] = enclosure

        return enclosures

    @property
    def enclosures(self) -> Dict[str, Dict[str, Any]]:
        """Dump the enclosure objects as dicts"""
        return {k: v._dump() for k, v in self._enclosures.items()}

    @property
    def enclosure_count(self) -> int:
        """Return the number of enclosures detected"""
        return len(self._enclosures.keys())

    def _get_cpuinfo(self):
        # type: () -> None
        """Determine cpu information via /proc/cpuinfo"""
        raw = read_file(['/proc/cpuinfo'])
        output = raw.splitlines()
        cpu_set = set()

        for line in output:
            field = [f.strip() for f in line.split(':')]
            if 'model name' in line:
                self.cpu_model = field[1]
            if 'physical id' in line:
                cpu_set.add(field[1])
            if 'siblings' in line:
                self.cpu_threads = int(field[1].strip())
            if 'cpu cores' in line:
                self.cpu_cores = int(field[1].strip())
            pass
        self.cpu_count = len(cpu_set)

    def _get_block_devs(self):
        # type: () -> List[str]
        """Determine the list of block devices by looking at /sys/block"""
        return [
            dev
            for dev in os.listdir('/sys/block')
            if not dev.startswith(HostFacts._excluded_block_devices)
        ]

    @property
    def operating_system(self):
        # type: () -> str
        """Determine OS version"""
        raw_info = read_file(['/etc/os-release'])
        os_release = raw_info.splitlines()
        rel_str = 'Unknown'
        rel_dict = dict()

        for line in os_release:
            if '=' in line:
                var_name, var_value = line.split('=')
                rel_dict[var_name] = var_value.strip('"')

        # Would normally use PRETTY_NAME, but NAME and VERSION are more
        # consistent
        if all(_v in rel_dict for _v in ['NAME', 'VERSION']):
            rel_str = '{} {}'.format(rel_dict['NAME'], rel_dict['VERSION'])
        return rel_str

    @property
    def hostname(self):
        # type: () -> str
        """Return the hostname"""
        return platform.node()

    @property
    def shortname(self) -> str:
        return platform.node().split('.', 1)[0]

    @property
    def fqdn(self) -> str:
        return get_fqdn()

    @property
    def subscribed(self):
        # type: () -> str
        """Highlevel check to see if the host is subscribed to receive updates/support"""

        def _red_hat():
            # type: () -> str
            # RHEL 7 and RHEL 8
            entitlements_dir = '/etc/pki/entitlement'
            if os.path.exists(entitlements_dir):
                pems = glob('{}/*.pem'.format(entitlements_dir))
                if len(pems) >= 2:
                    return 'Yes'

            return 'No'

        os_name = self.operating_system
        if os_name.upper().startswith('RED HAT'):
            return _red_hat()

        return 'Unknown'

    @property
    def hdd_count(self):
        # type: () -> int
        """Return a count of HDDs (spinners)"""
        return len(self.hdd_list)

    def _get_capacity(self, dev):
        # type: (str) -> int
        """Determine the size of a given device

        The kernel always bases device size calculations based on a 512 byte
        sector. For more information see
        https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git/tree/include/linux/types.h?h=v5.15.63#n120
        """
        size_path = os.path.join('/sys/block', dev, 'size')
        size_blocks = int(read_file([size_path]))
        return size_blocks * 512

    def _get_capacity_by_type(self, disk_type='hdd'):
        # type: (str) -> int
        """Return the total capacity of a category of device (flash or hdd)"""
        capacity: int = 0
        for dev in self._device_list:
            if dev['disk_type'] == disk_type:
                disk_capacity = cast(int, dev.get('disk_size_bytes', 0))
                capacity += disk_capacity
        return capacity

    def _get_device_info(self):
        # type: () -> List[Dict[str, object]]
        """Return a 'pretty' name list for each unique device in the `dev_list`"""
        disk_list = list()

        # serial_num_lookup is a dict of serial number -> List of devices with that serial number
        serial_num_lookup: Dict[str, List[str]] = {}

        # make a map of devname -> disk path. this path name may indicate the physical slot
        # of a drive (phyXX)
        disk_path_map: Dict[str, str] = {}
        for path in glob('/dev/disk/by-path/*'):
            tgt_raw = Path(path).resolve()
            tgt = os.path.basename(str(tgt_raw))
            disk_path_map[tgt] = path

        # make a map of holder (dm-XX) -> full mpath name
        dm_device_map: Dict[str, str] = {}
        for mpath in glob('/dev/mapper/mpath*'):
            tgt_raw = Path(mpath).resolve()
            tgt = os.path.basename(str(tgt_raw))
            dm_device_map[tgt] = mpath

        # main loop to process all eligible block devices
        for dev in self._block_devices:
            enclosure_id = ''
            enclosure_slot = ''
            scsi_addr = ''
            mpath = ''

            disk_model = read_file(
                ['/sys/block/{}/device/model'.format(dev)]
            ).strip()
            disk_rev = read_file(
                ['/sys/block/{}/device/rev'.format(dev)]
            ).strip()
            disk_wwid = read_file(
                ['/sys/block/{}/device/wwid'.format(dev)]
            ).strip()
            vendor = read_file(
                ['/sys/block/{}/device/vendor'.format(dev)]
            ).strip()
            rotational = read_file(
                ['/sys/block/{}/queue/rotational'.format(dev)]
            )
            holders_raw = glob('/sys/block/{}/holders/*'.format(dev))
            if len(holders_raw) == 1:
                # mpath will have 1 holder entry
                holder = os.path.basename(holders_raw[0])
                mpath = dm_device_map.get(holder, '')

            disk_type = 'hdd' if rotational == '1' else 'flash'
            scsi_addr_path = glob('/sys/block/{}/device/bsg/*'.format(dev))
            if len(scsi_addr_path) == 1:
                scsi_addr = os.path.basename(scsi_addr_path[0])

            # vpd_pg80 isn't guaranteed (libvirt, vmware for example)
            serial_raw = read_file(
                ['/sys/block/{}/device/vpd_pg80'.format(dev)]
            )
            serial = (
                ''.join(i for i in serial_raw if i in string.printable)
            ).strip()
            if serial.lower() == 'unknown':
                serial = ''
            else:
                if serial in serial_num_lookup:
                    serial_num_lookup[serial].append(dev)
                else:
                    serial_num_lookup[serial] = [dev]
                for enc_id, enclosure in self._enclosures.items():
                    if serial in enclosure.device_lookup.keys():
                        enclosure_id = enc_id
                        enclosure_slot = enclosure.device_lookup[serial]

            disk_vendor = HostFacts._disk_vendor_workarounds.get(
                vendor, vendor
            )
            disk_size_bytes = self._get_capacity(dev)
            disk_list.append(
                {
                    'description': '{} {} ({})'.format(
                        disk_vendor,
                        disk_model,
                        bytes_to_human(disk_size_bytes),
                    ),
                    'vendor': disk_vendor,
                    'model': disk_model,
                    'rev': disk_rev,
                    'wwid': disk_wwid,
                    'dev_name': dev,
                    'disk_size_bytes': disk_size_bytes,
                    'disk_type': disk_type,
                    'serial': serial,
                    'alt_dev_name': '',
                    'scsi_addr': scsi_addr,
                    'enclosure_id': enclosure_id,
                    'enclosure_slot': enclosure_slot,
                    'path_id': disk_path_map.get(dev, ''),
                    'mpath': mpath,
                }
            )

        # process the devices to drop duplicate physical devs based on matching
        # the unique serial number
        disk_list_unique: List[Dict[str, Any]] = []
        serials_seen: List[str] = []
        for dev in disk_list:
            serial = str(dev['serial'])
            if serial:
                if serial in serials_seen:
                    continue
                else:
                    serials_seen.append(serial)
                    devs = serial_num_lookup[serial].copy()
                    devs.remove(str(dev['dev_name']))
                    dev['alt_dev_name'] = ','.join(devs)
            disk_list_unique.append(dev)

        return disk_list_unique

    @property
    def hdd_list(self):
        # type: () -> List[Dict[str, object]]
        """Return a list of devices that are HDDs (spinners)"""
        return [dev for dev in self._device_list if dev['disk_type'] == 'hdd']

    @property
    def flash_list(self):
        # type: () -> List[Dict[str, object]]
        """Return a list of devices that are flash based (SSD, NVMe)"""
        return [
            dev for dev in self._device_list if dev['disk_type'] == 'flash'
        ]

    @property
    def hdd_capacity_bytes(self):
        # type: () -> int
        """Return the total capacity for all HDD devices (bytes)"""
        return self._get_capacity_by_type(disk_type='hdd')

    @property
    def hdd_capacity(self):
        # type: () -> str
        """Return the total capacity for all HDD devices (human readable format)"""
        return bytes_to_human(self.hdd_capacity_bytes)

    @property
    def cpu_load(self):
        # type: () -> Dict[str, float]
        """Return the cpu load average data for the host"""
        raw = read_file(['/proc/loadavg']).strip()
        data = raw.split()
        return {
            '1min': float(data[0]),
            '5min': float(data[1]),
            '15min': float(data[2]),
        }

    @property
    def flash_count(self):
        # type: () -> int
        """Return the number of flash devices in the system (SSD, NVMe)"""
        return len(self.flash_list)

    @property
    def flash_capacity_bytes(self):
        # type: () -> int
        """Return the total capacity for all flash devices (bytes)"""
        return self._get_capacity_by_type(disk_type='flash')

    @property
    def flash_capacity(self):
        # type: () -> str
        """Return the total capacity for all Flash devices (human readable format)"""
        return bytes_to_human(self.flash_capacity_bytes)

    def _process_nics(self):
        # type: () -> None
        """Look at the NIC devices and extract network related metadata"""
        # from https://github.com/torvalds/linux/blob/master/include/uapi/linux/if_arp.h
        hw_lookup = {
            '1': 'ethernet',
            '32': 'infiniband',
            '772': 'loopback',
        }

        for nic_path in HostFacts._nic_path_list:
            if not os.path.exists(nic_path):
                continue
            for iface in os.listdir(nic_path):
                if os.path.exists(os.path.join(nic_path, iface, 'bridge')):
                    nic_type = 'bridge'
                elif os.path.exists(os.path.join(nic_path, iface, 'bonding')):
                    nic_type = 'bonding'
                else:
                    nic_type = hw_lookup.get(
                        read_file([os.path.join(nic_path, iface, 'type')]),
                        'Unknown',
                    )

                if nic_type == 'loopback':  # skip loopback devices
                    continue

                lower_devs_list = [
                    os.path.basename(link.replace('lower_', ''))
                    for link in glob(os.path.join(nic_path, iface, 'lower_*'))
                ]
                upper_devs_list = [
                    os.path.basename(link.replace('upper_', ''))
                    for link in glob(os.path.join(nic_path, iface, 'upper_*'))
                ]

                try:
                    mtu = int(
                        read_file([os.path.join(nic_path, iface, 'mtu')])
                    )
                except ValueError:
                    mtu = 0

                operstate = read_file(
                    [os.path.join(nic_path, iface, 'operstate')]
                )
                try:
                    speed = int(
                        read_file([os.path.join(nic_path, iface, 'speed')])
                    )
                except (OSError, ValueError):
                    # OSError : device doesn't support the ethtool get_link_ksettings
                    # ValueError : raised when the read fails, and returns Unknown
                    #
                    # Either way, we show a -1 when speed isn't available
                    speed = -1

                dev_link = os.path.join(nic_path, iface, 'device')
                if os.path.exists(dev_link):
                    iftype = 'physical'
                    driver_path = os.path.join(dev_link, 'driver')
                    if os.path.exists(driver_path):
                        driver = os.path.basename(
                            os.path.realpath(driver_path)
                        )
                    else:
                        driver = 'Unknown'

                else:
                    iftype = 'logical'
                    driver = ''

                self.interfaces[iface] = {
                    'mtu': mtu,
                    'upper_devs_list': upper_devs_list,
                    'lower_devs_list': lower_devs_list,
                    'operstate': operstate,
                    'iftype': iftype,
                    'nic_type': nic_type,
                    'driver': driver,
                    'speed': speed,
                    'ipv4_address': get_ipv4_address(iface),
                    'ipv6_address': get_ipv6_address(iface),
                }

    @property
    def nic_count(self):
        # type: () -> int
        """Return a total count of all physical NICs detected in the host"""
        phys_devs = []
        for iface in self.interfaces:
            if self.interfaces[iface]['iftype'] == 'physical':
                phys_devs.append(iface)
        return len(phys_devs)

    def _get_mem_data(self, field_name):
        # type: (str) -> int
        for line in self._meminfo:
            if line.startswith(field_name):
                _d = line.split()
                return int(_d[1])
        return 0

    @property
    def memory_total_kb(self):
        # type: () -> int
        """Determine the memory installed (kb)"""
        return self._get_mem_data('MemTotal')

    @property
    def memory_free_kb(self):
        # type: () -> int
        """Determine the memory free (not cache, immediately usable)"""
        return self._get_mem_data('MemFree')

    @property
    def memory_available_kb(self):
        # type: () -> int
        """Determine the memory available to new applications without swapping"""
        return self._get_mem_data('MemAvailable')

    @property
    def vendor(self):
        # type: () -> str
        """Determine server vendor from DMI data in sysfs"""
        return read_file(HostFacts._dmi_path_list, 'sys_vendor')

    @property
    def model(self):
        # type: () -> str
        """Determine server model information from DMI data in sysfs"""
        family = read_file(HostFacts._dmi_path_list, 'product_family')
        product = read_file(HostFacts._dmi_path_list, 'product_name')
        if family == 'Unknown' and product:
            return '{}'.format(product)

        return '{} ({})'.format(family, product)

    @property
    def bios_version(self):
        # type: () -> str
        """Determine server BIOS version from  DMI data in sysfs"""
        return read_file(HostFacts._dmi_path_list, 'bios_version')

    @property
    def bios_date(self):
        # type: () -> str
        """Determine server BIOS date from  DMI data in sysfs"""
        return read_file(HostFacts._dmi_path_list, 'bios_date')

    @property
    def chassis_serial(self):
        # type: () -> str
        """Determine chassis serial number from DMI data in sysfs"""
        return read_file(HostFacts._dmi_path_list, 'chassis_serial')

    @property
    def board_serial(self):
        # type: () -> str
        """Determine mainboard serial number from DMI data in sysfs"""
        return read_file(HostFacts._dmi_path_list, 'board_serial')

    @property
    def product_serial(self):
        # type: () -> str
        """Determine server's serial number from DMI data in sysfs"""
        return read_file(HostFacts._dmi_path_list, 'product_serial')

    @property
    def timestamp(self):
        # type: () -> float
        """Return the current time as Epoch seconds"""
        return time.time()

    @property
    def system_uptime(self):
        # type: () -> float
        """Return the system uptime (in secs)"""
        raw_time = read_file(['/proc/uptime'])
        up_secs, _ = raw_time.split()
        return float(up_secs)

    @property
    def kernel_security(self):
        # type: () -> Dict[str, str]
        """Determine the security features enabled in the kernel - SELinux, AppArmor"""

        def _fetch_selinux() -> Dict[str, str]:
            """Get the selinux status"""
            security = {}
            try:
                out, err, code = call(
                    self.ctx, ['sestatus'], verbosity=CallVerbosity.QUIET
                )
                security['type'] = 'SELinux'
                status, mode, policy = '', '', ''
                for line in out.split('\n'):
                    if line.startswith('SELinux status:'):
                        k, v = line.split(':')
                        status = v.strip()
                    elif line.startswith('Current mode:'):
                        k, v = line.split(':')
                        mode = v.strip()
                    elif line.startswith('Loaded policy name:'):
                        k, v = line.split(':')
                        policy = v.strip()
                if status == 'disabled':
                    security['description'] = 'SELinux: Disabled'
                else:
                    security[
                        'description'
                    ] = 'SELinux: Enabled({}, {})'.format(mode, policy)
            except Exception as e:
                logger.info('unable to get selinux status: %s' % e)
            return security

        def _fetch_apparmor() -> Dict[str, str]:
            """Read the apparmor profiles directly, returning an overview of AppArmor status"""
            security = {}
            for apparmor_path in HostFacts._apparmor_path_list:
                if os.path.exists(apparmor_path):
                    security['type'] = 'AppArmor'
                    security['description'] = 'AppArmor: Enabled'
                    try:
                        profiles = read_file(
                            ['/sys/kernel/security/apparmor/profiles']
                        )
                        if len(profiles) == 0:
                            return {}
                    except OSError:
                        pass
                    else:
                        summary = {}  # type: Dict[str, int]
                        for line in profiles.split('\n'):
                            mode = line.rsplit(' ', 1)[-1]
                            assert mode[0] == '(' and mode[-1] == ')'
                            mode = mode[1:-1]
                            if mode in summary:
                                summary[mode] += 1
                            else:
                                summary[mode] = 0
                        summary_str = ','.join(
                            ['{} {}'.format(v, k) for k, v in summary.items()]
                        )
                        security = {**security, **summary}  # type: ignore
                        security['description'] += '({})'.format(summary_str)

                    return security
            return {}

        ret = {}
        if os.path.exists('/sys/kernel/security/lsm'):
            lsm = read_file(['/sys/kernel/security/lsm']).strip()
            if 'selinux' in lsm:
                ret = _fetch_selinux()
            elif 'apparmor' in lsm:
                ret = _fetch_apparmor()
            else:
                return {
                    'type': 'Unknown',
                    'description': 'Linux Security Module framework is active, but is not using SELinux or AppArmor',
                }

        if ret:
            return ret

        return {
            'type': 'None',
            'description': 'Linux Security Module framework is not available',
        }

    @property
    def selinux_enabled(self) -> bool:
        return (self.kernel_security['type'] == 'SELinux') and (
            self.kernel_security['description'] != 'SELinux: Disabled'
        )

    @property
    def kernel_parameters(self):
        # type: () -> Dict[str, str]
        """Get kernel parameters required/used in Ceph clusters"""

        k_param = {}
        out, _, _ = call_throws(
            self.ctx, ['sysctl', '-a'], verbosity=CallVerbosity.SILENT
        )
        if out:
            param_list = out.split('\n')
            param_dict = {
                param.split(' = ')[0]: param.split(' = ')[-1]
                for param in param_list
            }

            # return only desired parameters
            if 'net.ipv4.ip_nonlocal_bind' in param_dict:
                k_param['net.ipv4.ip_nonlocal_bind'] = param_dict[
                    'net.ipv4.ip_nonlocal_bind'
                ]

        return k_param

    @staticmethod
    def _process_net_data(tcp_file: str, protocol: str = 'tcp') -> List[int]:
        listening_ports = []
        # Connections state documentation
        # tcp - https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/include/net/tcp_states.h
        # udp - uses 07 (TCP_CLOSE or UNCONN, since udp is stateless. test with netcat -ul <port>)
        listening_state = {'tcp': '0A', 'udp': '07'}

        if protocol not in listening_state.keys():
            return []

        if os.path.exists(tcp_file):
            with open(tcp_file) as f:
                tcp_data = f.readlines()[1:]

            for con in tcp_data:
                con_info = con.strip().split()
                if con_info[3] == listening_state[protocol]:
                    local_port = int(con_info[1].split(':')[1], 16)
                    listening_ports.append(local_port)

        return listening_ports

    @property
    def tcp_ports_used(self) -> List[int]:
        return HostFacts._process_net_data('/proc/net/tcp')

    @property
    def tcp6_ports_used(self) -> List[int]:
        return HostFacts._process_net_data('/proc/net/tcp6')

    @property
    def udp_ports_used(self) -> List[int]:
        return HostFacts._process_net_data('/proc/net/udp', 'udp')

    @property
    def udp6_ports_used(self) -> List[int]:
        return HostFacts._process_net_data('/proc/net/udp6', 'udp')

    def dump(self):
        # type: () -> str
        """Return the attributes of this HostFacts object as json"""
        data = {
            k: getattr(self, k)
            for k in dir(self)
            if not k.startswith('_')
            and isinstance(
                getattr(self, k), (float, int, str, list, dict, tuple)
            )
        }
        return json.dumps(data, indent=2, sort_keys=True)


def list_networks(ctx):
    # type: (CephadmContext) -> Dict[str,Dict[str, Set[str]]]

    # sadly, 18.04's iproute2 4.15.0-2ubun doesn't support the -j flag,
    # so we'll need to use a regex to parse 'ip' command output.
    #
    # out, _, _ = call_throws(['ip', '-j', 'route', 'ls'])
    # j = json.loads(out)
    # for x in j:
    res = _list_ipv4_networks(ctx)
    res.update(_list_ipv6_networks(ctx))
    return res


def _list_ipv4_networks(
    ctx: CephadmContext,
) -> Dict[str, Dict[str, Set[str]]]:
    execstr: Optional[str] = find_executable('ip')
    if not execstr:
        raise FileNotFoundError("unable to find 'ip' command")
    out, _, _ = call_throws(
        ctx,
        [execstr, 'route', 'ls'],
        verbosity=CallVerbosity.QUIET_UNLESS_ERROR,
    )
    return _parse_ipv4_route(out)


def _parse_ipv4_route(out: str) -> Dict[str, Dict[str, Set[str]]]:
    r = {}  # type: Dict[str, Dict[str, Set[str]]]
    p = re.compile(
        r'^(\S+) (?:via \S+)? ?dev (\S+) (.*)scope link (.*)src (\S+)'
    )
    for line in out.splitlines():
        m = p.findall(line)
        if not m:
            continue
        net = m[0][0]
        if '/' not in net:  # aggregate /32 mask for single host sub-networks
            net += '/32'
        iface = m[0][1]
        ip = m[0][4]
        if net not in r:
            r[net] = {}
        if iface not in r[net]:
            r[net][iface] = set()
        r[net][iface].add(ip)
    return r


def _list_ipv6_networks(
    ctx: CephadmContext,
) -> Dict[str, Dict[str, Set[str]]]:
    execstr: Optional[str] = find_executable('ip')
    if not execstr:
        raise FileNotFoundError("unable to find 'ip' command")
    routes, _, _ = call_throws(
        ctx,
        [execstr, '-6', 'route', 'ls'],
        verbosity=CallVerbosity.QUIET_UNLESS_ERROR,
    )
    ips, _, _ = call_throws(
        ctx,
        [execstr, '-6', 'addr', 'ls'],
        verbosity=CallVerbosity.QUIET_UNLESS_ERROR,
    )
    return _parse_ipv6_route(routes, ips)


def _parse_ipv6_route(
    routes: str, ips: str
) -> Dict[str, Dict[str, Set[str]]]:
    r = {}  # type: Dict[str, Dict[str, Set[str]]]
    route_p = re.compile(
        r'^(\S+) dev (\S+) proto (\S+) metric (\S+) .*pref (\S+)$'
    )
    ip_p = re.compile(r'^\s+inet6 (\S+)/(.*)scope (.*)$')
    iface_p = re.compile(r'^(\d+): (\S+): (.*)$')
    for line in routes.splitlines():
        m = route_p.findall(line)
        if not m or m[0][0].lower() == 'default':
            continue
        net = m[0][0]
        if '/' not in net:  # aggregate /128 mask for single host sub-networks
            net += '/128'
        iface = m[0][1]
        if iface == 'lo':  # skip loopback devices
            continue
        if net not in r:
            r[net] = {}
        if iface not in r[net]:
            r[net][iface] = set()

    iface = None
    for line in ips.splitlines():
        m = ip_p.findall(line)
        if not m:
            m = iface_p.findall(line)
            if m:
                # drop @... suffix, if present
                iface = m[0][1].split('@')[0]
            continue
        ip = m[0][0]
        # find the network it belongs to
        net = [
            n
            for n in r.keys()
            if ipaddress.ip_address(ip) in ipaddress.ip_network(n)
        ]
        if net and iface in r[net[0]]:
            assert iface
            r[net[0]][iface].add(ip)

    return r
