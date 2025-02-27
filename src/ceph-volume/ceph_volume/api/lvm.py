"""
API for CRUD lvm tag operations. Follows the Ceph LVM tag naming convention
that prefixes tags with ``ceph.`` and uses ``=`` for assignment, and provides
set of utilities for interacting with LVM.
"""
import logging
import os
import uuid
from math import floor
from ceph_volume import process, util, conf
from ceph_volume.exceptions import SizeAllocationError
from typing import Any, Dict, Optional, List, Union, Set


logger = logging.getLogger(__name__)


def convert_filters_to_str(filters: Dict[str, Any]) -> str:
    """
    Convert filter args from dictionary to following format -
        filters={filter_name=filter_val,...}
    """
    if not filters:
        return ''

    filter_arg = ''
    for k, v in filters.items():
        filter_arg += k + '=' + v + ','
    # get rid of extra comma at the end
    filter_arg = filter_arg[:len(filter_arg) - 1]

    return filter_arg


def convert_tags_to_str(tags: Dict[str, Any]) -> str:
    """
    Convert tags from dictionary to following format -
        tags={tag_name=tag_val,...}
    """
    if not tags:
        return ''

    tag_arg = 'tags={'
    for k, v in tags.items():
        tag_arg += k + '=' + v + ','
    # get rid of extra comma at the end
    tag_arg = tag_arg[:len(tag_arg) - 1] + '}'

    return tag_arg


def make_filters_lvmcmd_ready(filters: Dict[str, Any], tags: Dict[str, Any]) -> str:
    """
    Convert filters (including tags) from dictionary to following format -
        filter_name=filter_val...,tags={tag_name=tag_val,...}

    The command will look as follows =
        lvs -S filter_name=filter_val...,tags={tag_name=tag_val,...}
    """
    filters_str = convert_filters_to_str(filters)
    tags_str = convert_tags_to_str(tags)

    if filters_str and tags_str:
        return filters_str + ',' + tags_str
    if filters_str and not tags_str:
        return filters_str
    if not filters_str and tags_str:
        return tags_str
    else:
        return ''


def _output_parser(output: List[str], fields: str) -> List[Dict[str, Any]]:
    """
    Newer versions of LVM allow ``--reportformat=json``, but older versions,
    like the one included in Xenial do not. LVM has the ability to filter and
    format its output so we assume the output will be in a format this parser
    can handle (using ';' as a delimiter)

    :param fields: A string, possibly using ',' to group many items, as it
                   would be used on the CLI
    :param output: The CLI output from the LVM call
    """
    field_items = fields.split(',')
    report = []
    for line in output:
        # clear the leading/trailing whitespace
        line = line.strip()

        # remove the extra '"' in each field
        line = line.replace('"', '')

        # prevent moving forward with empty contents
        if not line:
            continue

        # splitting on ';' because that is what the lvm call uses as
        # '--separator'
        output_items = [i.strip() for i in line.split(';')]
        # map the output to the fields
        report.append(
            dict(zip(field_items, output_items))
        )

    return report


def _splitname_parser(line: str) -> Dict[str, Any]:
    """
    Parses the output from ``dmsetup splitname``, that should contain prefixes
    (--nameprefixes) and set the separator to ";"

    Output for /dev/mapper/vg-lv will usually look like::

        DM_VG_NAME='/dev/mapper/vg';DM_LV_NAME='lv';DM_LV_LAYER=''


    The ``VG_NAME`` will usually not be what other callers need (e.g. just 'vg'
    in the example), so this utility will split ``/dev/mapper/`` out, so that
    the actual volume group name is kept

    :returns: dictionary with stripped prefixes
    """
    parsed: Dict[str, Any] = {}
    try:
        parts = line[0].split(';')
    except IndexError:
        logger.exception('Unable to parse mapper device: %s', line)
        return parsed

    for part in parts:
        part = part.replace("'", '')
        key, value = part.split('=')
        if 'DM_VG_NAME' in key:
            value = value.split('/dev/mapper/')[-1]
        key = key.split('DM_')[-1]
        parsed[key] = value

    return parsed


def sizing(device_size: int, parts: Optional[int] = None, size: Optional[int] = None) -> Dict[str, Any]:
    """
    Calculate proper sizing to fully utilize the volume group in the most
    efficient way possible. To prevent situations where LVM might accept
    a percentage that is beyond the vg's capabilities, it will refuse with
    an error when requesting a larger-than-possible parameter, in addition
    to rounding down calculations.

    A dictionary with different sizing parameters is returned, to make it
    easier for others to choose what they need in order to create logical
    volumes::

        >>> sizing(100, parts=2)
        >>> {'parts': 2, 'percentages': 50, 'sizes': 50}

    """
    if parts is not None and size is not None:
        raise ValueError(
            "Cannot process sizing with both parts (%s) and size (%s)" % (parts, size)
        )

    if size and size > device_size:
        raise SizeAllocationError(size, device_size)

    def get_percentage(parts: int) -> int:
        return int(floor(100 / float(parts)))

    if parts is not None:
        # Prevent parts being 0, falling back to 1 (100% usage)
        parts = parts or 1
        percentages = get_percentage(parts)

    if size:
        parts = int(device_size / size) or 1
        percentages = get_percentage(parts)

    sizes = device_size / parts if parts else int(floor(device_size))

    return {
        'parts': parts,
        'percentages': percentages,
        'sizes': int(sizes/1024/1024/1024),
    }


def parse_tags(lv_tags: str) -> Dict[str, Any]:
    """
    Return a dictionary mapping of all the tags associated with
    a Volume from the comma-separated tags coming from the LVM API

    Input look like::

       "ceph.osd_fsid=aaa-fff-bbbb,ceph.osd_id=0"

    For the above example, the expected return value would be::

        {
            "ceph.osd_fsid": "aaa-fff-bbbb",
            "ceph.osd_id": "0"
        }
    """
    if not lv_tags:
        return {}
    tag_mapping = {}
    tags = lv_tags.split(',')
    for tag_assignment in tags:
        if not tag_assignment.startswith('ceph.'):
            continue
        key, value = tag_assignment.split('=', 1)
        tag_mapping[key] = value

    return tag_mapping


def _vdo_parents(devices: List[str]) -> List[str]:
    """
    It is possible we didn't get a logical volume, or a mapper path, but
    a device like /dev/sda2, to resolve this, we must look at all the slaves of
    every single device in /sys/block and if any of those devices is related to
    VDO devices, then we can add the parent
    """
    parent_devices = []
    for parent in os.listdir('/sys/block'):
        for slave in os.listdir('/sys/block/%s/slaves' % parent):
            if slave in devices:
                parent_devices.append('/dev/%s' % parent)
                parent_devices.append(parent)
    return parent_devices


def _vdo_slaves(vdo_names: List[str]) -> List[str]:
    """
    find all the slaves associated with each vdo name (from realpath) by going
    into /sys/block/<realpath>/slaves
    """
    devices = []
    for vdo_name in vdo_names:
        mapper_path = '/dev/mapper/%s' % vdo_name
        if not os.path.exists(mapper_path):
            continue
        # resolve the realpath and realname of the vdo mapper
        vdo_realpath = os.path.realpath(mapper_path)
        vdo_realname = vdo_realpath.split('/')[-1]
        slaves_path = '/sys/block/%s/slaves' % vdo_realname
        if not os.path.exists(slaves_path):
            continue
        devices.append(vdo_realpath)
        devices.append(mapper_path)
        devices.append(vdo_realname)
        for slave in os.listdir(slaves_path):
            devices.append('/dev/%s' % slave)
            devices.append(slave)
    return devices


def _is_vdo(path: str) -> bool:
    """
    A VDO device can be composed from many different devices, go through each
    one of those devices and its slaves (if any) and correlate them back to
    /dev/mapper and their realpaths, and then check if they appear as part of
    /sys/kvdo/<name>/statistics

    From the realpath of a logical volume, determine if it is a VDO device or
    not, by correlating it to the presence of the name in
    /sys/kvdo/<name>/statistics and all the previously captured devices
    """
    if not os.path.isdir('/sys/kvdo'):
        return False
    realpath = os.path.realpath(path)
    realpath_name = realpath.split('/')[-1]
    devices = []
    vdo_names = set()
    # get all the vdo names
    for dirname in os.listdir('/sys/kvdo/'):
        if os.path.isdir('/sys/kvdo/%s/statistics' % dirname):
            vdo_names.add(dirname)

    # find all the slaves associated with each vdo name (from realpath) by
    # going into /sys/block/<realpath>/slaves
    devices.extend(_vdo_slaves(list(vdo_names)))

    # Find all possible parents, looking into slaves that are related to VDO
    devices.extend(_vdo_parents(devices))

    return any([
        path in devices,
        realpath in devices,
        realpath_name in devices])


def is_vdo(path: str) -> str:
    """
    Detect if a path is backed by VDO, proxying the actual call to _is_vdo so
    that we can prevent an exception breaking OSD creation. If an exception is
    raised, it will get captured and logged to file, while returning
    a ``False``.
    """
    try:
        if _is_vdo(path):
            return '1'
        return '0'
    except Exception:
        logger.exception('Unable to properly detect device as VDO: %s', path)
        return '0'


def dmsetup_splitname(dev: str) -> Dict[str, Any]:
    """
    Run ``dmsetup splitname`` and parse the results.

    .. warning:: This call does not ensure that the device is correct or that
    it exists. ``dmsetup`` will happily take a non existing path and still
    return a 0 exit status.
    """
    command = [
        'dmsetup', 'splitname', '--noheadings',
        "--separator=';'", '--nameprefixes', dev
    ]
    out, err, rc = process.call(command)
    return _splitname_parser(out)


def is_ceph_device(lv: "Volume") -> bool:
    osd_id = lv.tags.get('ceph.osd_id', 'null')

    if osd_id == 'null':
         logger.warning('device is not part of ceph: %s', lv)
         return False

    return True

class Lvm:
    def __init__(self, name_key: str, tags_key: str, **kw: Any) -> None:
        self.name: str = kw.get(name_key, '')
        self.binary_change: str = ''
        self.path: str = ''
        if not self.name:
            raise ValueError(f'{self.__class__.__name__} must have a non-empty name')

        self.api_data = kw
        self.tags = parse_tags(kw.get(tags_key, ''))

        for k, v in kw.items():
            setattr(self, k, v)

    def __str__(self) -> str:
        return f'<{self.name}>'

    def __repr__(self) -> str:
        return self.__str__()

    def _format_tag_args(self, op: str, tags: Dict[str, Any]) -> List[str]:
        result: List[str] = []
        for k, v in tags.items():
            result.extend([op, f'{k}={v}'])
        return result

    def clear_tags(self, keys: Optional[List[str]] = None) -> None:
        """
        Removes all or passed tags.
        """
        if not keys:
            keys = list(self.tags.keys())

        del_tags = {k: self.tags[k] for k in keys if k in self.tags}
        if not del_tags:
            # nothing to clear
            return
        del_tag_args = self._format_tag_args('--deltag', del_tags)
        # --deltag returns successful even if the to be deleted tag is not set
        process.call([self.binary_change] + del_tag_args + [self.path], run_on_host=True)
        for k in del_tags.keys():
            del self.tags[k]

    def clear_tag(self, key: str) -> None:
        if self.tags.get(key):
            current_value = self.tags[key]
            tag = "%s=%s" % (key, current_value)
            process.call([self.binary_change, '--deltag', tag, self.path], run_on_host=True)
            del self.tags[key]

    def set_tag(self, key: str, value: str) -> None:
        """
        Set the key/value pair as an LVM tag.
        """
        # remove it first if it exists
        self.clear_tag(key)

        process.call(
            [
                self.binary_change,
                '--addtag', '%s=%s' % (key, value), self.path
            ],
            run_on_host=True
        )
        self.tags[key] = value

    def set_tags(self, tags: Dict[str, Any]) -> None:
        """
        :param tags: A dictionary of tag names and values, like::

            {
                "ceph.osd_fsid": "aaa-fff-bbbb",
                "ceph.osd_id": "0"
            }

        At the end of all modifications, the tags are refreshed to reflect
        LVM's most current view.
        """
        self.clear_tags(list(tags.keys()))
        add_tag_args = self._format_tag_args('--addtag', tags)
        process.call([self.binary_change] + add_tag_args + [self.path], run_on_host=True)
        for k, v in tags.items():
            self.tags[k] = v

    def deactivate(self) -> None:
        """
        Deactivate the LV by calling lvchange -an
        """
        process.call([self.binary_change, '-an', self.path], run_on_host=True)

####################################
#
# Code for LVM Physical Volumes
#
################################

PV_FIELDS = 'pv_name,pv_tags,pv_uuid,vg_name,lv_uuid'

class PVolume(Lvm):
    """
    Represents a Physical Volume from LVM, with some top-level attributes like
    ``pv_name`` and parsed tags as a dictionary of key/value pairs.
    """

    def __init__(self, **kw: Any) -> None:
        self.pv_name: str = ''
        self.pv_uuid: str = ''
        super().__init__('pv_name', 'pv_tags', **kw)
        self.pv_api = kw
        self.binary_change: str = 'pvchange'
        self.path: str = self.pv_name

    def set_tags(self, tags: Dict[str, Any]) -> None:
        """
        :param tags: A dictionary of tag names and values, like::

            {
                "ceph.osd_fsid": "aaa-fff-bbbb",
                "ceph.osd_id": "0"
            }

        At the end of all modifications, the tags are refreshed to reflect
        LVM's most current view.
        """
        for k, v in tags.items():
            self.set_tag(k, v)
        # after setting all the tags, refresh them for the current object, use the
        # pv_* identifiers to filter because those shouldn't change
        pv_object = get_single_pv(filters={'pv_name': self.pv_name,
                                           'pv_uuid': self.pv_uuid})

        if not pv_object:
            raise RuntimeError('No PV was found.')

        self.tags = pv_object.tags

    def set_tag(self, key: str, value: str) -> None:
        """
        Set the key/value pair as an LVM tag. Does not "refresh" the values of
        the current object for its tags. Meant to be a "fire and forget" type
        of modification.

        **warning**: Altering tags on a PV has to be done ensuring that the
        device is actually the one intended. ``pv_name`` is *not* a persistent
        value, only ``pv_uuid`` is. Using ``pv_uuid`` is the best way to make
        sure the device getting changed is the one needed.
        """
        # remove it first if it exists
        if self.tags.get(key):
            current_value = self.tags[key]
            tag = "%s=%s" % (key, current_value)
            process.call(['pvchange', '--deltag', tag, self.pv_name], run_on_host=True)

        process.call(
            [
                'pvchange',
                '--addtag', '%s=%s' % (key, value), self.pv_name
            ],
            run_on_host=True
        )


def create_pv(device: str) -> None:
    """
    Create a physical volume from a device, useful when devices need to be later mapped
    to journals.
    """
    process.run([
        'pvcreate',
        '-v',  # verbose
        '-f',  # force it
        '--yes', # answer yes to any prompts
        device
    ], run_on_host=True)


def remove_pv(pv_name: str) -> None:
    """
    Removes a physical volume using a double `-f` to prevent prompts and fully
    remove anything related to LVM. This is tremendously destructive, but so is all other actions
    when zapping a device.

    In the case where multiple PVs are found, it will ignore that fact and
    continue with the removal, specifically in the case of messages like::

        WARNING: PV $UUID /dev/DEV-1 was already found on /dev/DEV-2

    These situations can be avoided with custom filtering rules, which this API
    cannot handle while accommodating custom user filters.
    """
    fail_msg = "Unable to remove vg %s" % pv_name
    process.run(
        [
            'pvremove',
            '-v',  # verbose
            '-f',  # force it
            '-f',  # force it
            pv_name
        ],
        run_on_host=True,
        fail_msg=fail_msg,
    )


def get_pvs(fields: str = PV_FIELDS, filters: Optional[Dict[str, Any]] = None, tags: Optional[Dict[str, Any]] = None) -> List[PVolume]:
    """
    Return a list of PVs that are available on the system and match the
    filters and tags passed. Argument filters takes a dictionary containing
    arguments required by -S option of LVM. Passing a list of LVM tags can be
    quite tricky to pass as a dictionary within dictionary, therefore pass
    dictionary of tags via tags argument and tricky part will be taken care of
    by the helper methods.

    :param fields: string containing list of fields to be displayed by the
                   pvs command
    :param sep: string containing separator to be used between two fields
    :param filters: dictionary containing LVM filters
    :param tags: dictionary containng LVM tags
    :returns: list of class PVolume object representing pvs on the system
    """
    if filters is None:
        filters = {}
    if tags is None:
        tags = {}
    filters_str = make_filters_lvmcmd_ready(filters, tags)
    args = ['pvs', '--noheadings', '--readonly', '--separator=";"', '-S',
            filters_str, '-o', fields]

    stdout, stderr, returncode = process.call(args, run_on_host=True, verbose_on_failure=False)
    pvs_report = _output_parser(stdout, fields)
    return [PVolume(**pv_report) for pv_report in pvs_report]


def get_single_pv(fields: str = PV_FIELDS, filters: Optional[Dict[str, Any]] = None, tags: Optional[Dict[str, Any]] = None) -> Optional[PVolume]:
    """
    Wrapper of get_pvs() meant to be a convenience method to avoid the phrase::
        pvs = get_pvs()
        if len(pvs) >= 1:
            pv = pvs[0]
    """
    pvs = get_pvs(fields=fields, filters=filters, tags=tags)

    if len(pvs) == 0:
        return None
    if len(pvs) > 1:
        raise RuntimeError('Filters {} matched more than 1 PV present on this host.'.format(str(filters)))

    return pvs[0]


################################
#
# Code for LVM Volume Groups
#
#############################

VG_FIELDS = 'vg_name,pv_count,lv_count,vg_attr,vg_extent_count,vg_free_count,vg_extent_size'
VG_CMD_OPTIONS = ['--noheadings', '--readonly', '--units=b', '--nosuffix', '--separator=";"']


class VolumeGroup(Lvm):
    """
    Represents an LVM group, with some top-level attributes like ``vg_name``
    """

    def __init__(self, **kw: Any) -> None:
        self.pv_name: str = ''
        self.vg_name: str = ''
        self.vg_free_count: str = ''
        self.vg_extent_size: str = ''
        self.vg_extent_count: str = ''
        super().__init__('vg_name', 'vg_tags', **kw)
        self.vg_api = kw

    @property
    def free(self) -> int:
        """
        Return free space in VG in bytes
        """
        return int(self.vg_extent_size) * int(self.vg_free_count)

    @property
    def free_percent(self) -> float:
        """
        Return free space in VG in bytes
        """
        return int(self.vg_free_count) / int(self.vg_extent_count)

    @property
    def size(self) -> int:
        """
        Returns VG size in bytes
        """
        return int(self.vg_extent_size) * int(self.vg_extent_count)

    def sizing(self, parts: Optional[int] = None, size: Optional[int] = None) -> Dict[str, Any]:
        """
        Calculate proper sizing to fully utilize the volume group in the most
        efficient way possible. To prevent situations where LVM might accept
        a percentage that is beyond the vg's capabilities, it will refuse with
        an error when requesting a larger-than-possible parameter, in addition
        to rounding down calculations.

        A dictionary with different sizing parameters is returned, to make it
        easier for others to choose what they need in order to create logical
        volumes:

        >>> data_vg.free
        1024
        >>> data_vg.sizing(parts=4)
        {'parts': 4, 'sizes': 256, 'percentages': 25}
        >>> data_vg.sizing(size=512)
        {'parts': 2, 'sizes': 512, 'percentages': 50}


        :param parts: Number of parts to create LVs from
        :param size: Size in gigabytes to divide the VG into

        :raises SizeAllocationError: When requested size cannot be allocated with
        :raises ValueError: If both ``parts`` and ``size`` are given
        """
        if parts is not None and size is not None:
            raise ValueError(
                "Cannot process sizing with both parts (%s) and size (%s)" % (parts, size)
            )

        # if size is given we need to map that to extents so that we avoid
        # issues when trying to get this right with a size in gigabytes find
        # the percentage first, cheating, because these values are thrown out
        vg_free_count = util.str_to_int(self.vg_free_count)

        if size:
            size = size * 1024 * 1024 * 1024
            extents = int(size / int(self.vg_extent_size))
            disk_sizing = sizing(self.free, size=size, parts=parts)
        else:
            if parts is None or parts == 0:
                # Prevent parts being 0, falling back to 1 (100% usage)
                parts = 1
            size = int(self.free / parts)
            extents = size * vg_free_count / self.free
            disk_sizing = sizing(self.free, parts=parts)

        extent_sizing = sizing(vg_free_count, size=extents)

        disk_sizing['extents'] = int(extents)
        disk_sizing['percentages'] = extent_sizing['percentages']
        return disk_sizing

    def bytes_to_extents(self, size: int) -> int:
        '''
        Return a how many free extents we can fit into a size in bytes. This has
        some uncertainty involved. If size/extent_size is within 1% of the
        actual free extents we will return the extent count, otherwise we'll
        throw an error.
        This accomodates for the size calculation in batch. We need to report
        the OSD layout but have not yet created any LVM structures. We use the
        disk size in batch if no VG is present and that will overshoot the
        actual free_extent count due to LVM overhead.

        '''
        b_to_ext = int(size / int(self.vg_extent_size))
        if b_to_ext < int(self.vg_free_count):
            # return bytes in extents if there is more space
            return b_to_ext
        elif b_to_ext / int(self.vg_free_count) - 1 < 0.01:
            # return vg_fre_count if its less then 1% off
            logger.info(
                'bytes_to_extents results in {} but only {} '
                'are available, adjusting the latter'.format(b_to_ext,
                                                             self.vg_free_count))
            return int(self.vg_free_count)
        # else raise an exception
        raise RuntimeError('Can\'t convert {} to free extents, only {} ({} '
                           'bytes) are free'.format(size, self.vg_free_count,
                                                    self.free))

    def slots_to_extents(self, slots: int) -> int:
        '''
        Return how many extents fit the VG slot times
        '''
        return int(int(self.vg_extent_count) / slots)


def create_vg(devices: Union[str, Set, List[str]], name: Optional[str] = None, name_prefix: str = '') -> Optional[VolumeGroup]:
    """
    Create a Volume Group. Command looks like::

        vgcreate --force --yes group_name device

    Once created the volume group is returned as a ``VolumeGroup`` object

    :param devices: A list of devices to create a VG. Optionally, a single
                    device (as a string) can be used.
    :param name: Optionally set the name of the VG, defaults to 'ceph-{uuid}'
    :param name_prefix: Optionally prefix the name of the VG, which will get combined
                        with a UUID string
    """
    if isinstance(devices, set):
        devices = list(devices)
    if not isinstance(devices, list):
        devices = [devices]
    if name_prefix:
        name = "%s-%s" % (name_prefix, str(uuid.uuid4()))
    elif name is None:
        name = "ceph-%s" % str(uuid.uuid4())
    process.run([
        'vgcreate',
        '--force',
        '--yes',
        name] + devices,
        run_on_host=True
    )

    return get_single_vg(filters={'vg_name': name})


def extend_vg(vg: VolumeGroup, devices: Union[List[str], str]) -> Optional[VolumeGroup]:
    """
    Extend a Volume Group. Command looks like::

        vgextend --force --yes group_name [device, ...]

    Once created the volume group is extended and returned as a ``VolumeGroup`` object

    :param vg: A VolumeGroup object
    :param devices: A list of devices to extend the VG. Optionally, a single
                    device (as a string) can be used.
    """
    if not isinstance(devices, list):
        devices = [devices]
    process.run([
        'vgextend',
        '--force',
        '--yes',
        vg.name] + devices,
        run_on_host=True
    )

    return get_single_vg(filters={'vg_name': vg.name})


def reduce_vg(vg: VolumeGroup, devices: Union[List[str], str]) -> Optional[VolumeGroup]:
    """
    Reduce a Volume Group. Command looks like::

        vgreduce --force --yes group_name [device, ...]

    :param vg: A VolumeGroup object
    :param devices: A list of devices to remove from the VG. Optionally, a
                    single device (as a string) can be used.
    """
    if not isinstance(devices, list):
        devices = [devices]
    process.run([
        'vgreduce',
        '--force',
        '--yes',
        vg.name] + devices,
        run_on_host=True
    )

    return get_single_vg(filters={'vg_name': vg.name})


def remove_vg(vg_name: str) -> None:
    """
    Removes a volume group.
    """
    if not vg_name:
        logger.warning('Skipping removal of invalid VG name: "%s"', vg_name)
        return
    fail_msg = "Unable to remove vg %s" % vg_name
    process.run(
        [
            'vgremove',
            '-v',  # verbose
            '-f',  # force it
            vg_name
        ],
        run_on_host=True,
        fail_msg=fail_msg,
    )


def get_vgs(fields: str = VG_FIELDS, filters: Optional[Dict[str, Any]] = None, tags: Optional[Dict[str, Any]] = None) -> List[VolumeGroup]:
    """
    Return a list of VGs that are available on the system and match the
    filters and tags passed. Argument filters takes a dictionary containing
    arguments required by -S option of LVM. Passing a list of LVM tags can be
    quite tricky to pass as a dictionary within dictionary, therefore pass
    dictionary of tags via tags argument and tricky part will be taken care of
    by the helper methods.

    :param fields: string containing list of fields to be displayed by the
                   vgs command
    :param sep: string containing separator to be used between two fields
    :param filters: dictionary containing LVM filters
    :param tags: dictionary containng LVM tags
    :returns: list of class VolumeGroup object representing vgs on the system
    """
    if filters is None:
        filters = {}
    if tags is None:
        tags = {}
    filters_str = make_filters_lvmcmd_ready(filters, tags)
    args = ['vgs'] + VG_CMD_OPTIONS + ['-S', filters_str, '-o', fields]

    stdout, stderr, returncode = process.call(args, run_on_host=True, verbose_on_failure=False)
    vgs_report =_output_parser(stdout, fields)
    return [VolumeGroup(**vg_report) for vg_report in vgs_report]


def get_single_vg(fields: str = VG_FIELDS, filters: Optional[Dict[str, Any]] = None, tags: Optional[Dict[str, Any]] = None) -> Optional[VolumeGroup]:
    """
    Wrapper of get_vgs() meant to be a convenience method to avoid the phrase::
        vgs = get_vgs()
        if len(vgs) >= 1:
            vg = vgs[0]
    """
    vgs = get_vgs(fields=fields, filters=filters, tags=tags)

    if len(vgs) == 0:
        return None
    if len(vgs) > 1:
        raise RuntimeError('Filters {} matched more than 1 VG present on this host.'.format(str(filters)))

    return vgs[0]


def get_device_vgs(device: str, name_prefix: str = '') -> List[VolumeGroup]:
    stdout, stderr, returncode = process.call(
        ['pvs'] + VG_CMD_OPTIONS + ['-o', VG_FIELDS, device],
        run_on_host=True,
        verbose_on_failure=False
    )
    vgs = _output_parser(stdout, VG_FIELDS)
    return [VolumeGroup(**vg) for vg in vgs if vg['vg_name'] and vg['vg_name'].startswith(name_prefix)]


def get_all_devices_vgs(name_prefix: str = '') -> List[VolumeGroup]:
    vg_fields = f'pv_name,{VG_FIELDS}'
    cmd = ['pvs'] + VG_CMD_OPTIONS + ['-o', vg_fields]
    stdout, stderr, returncode = process.call(
        cmd,
        run_on_host=True,
        verbose_on_failure=False
    )
    vgs = _output_parser(stdout, vg_fields)
    return [VolumeGroup(**vg) for vg in vgs if vg['vg_name']]

#################################
#
# Code for LVM Logical Volumes
#
###############################

LV_FIELDS = 'lv_tags,lv_path,lv_name,vg_name,lv_uuid,lv_size'
LV_CMD_OPTIONS =  ['--noheadings', '--readonly', '--separator=";"', '-a',
                   '--units=b', '--nosuffix']


class Volume(Lvm):
    """
    Represents a Logical Volume from LVM, with some top-level attributes like
    ``lv_name`` and parsed tags as a dictionary of key/value pairs.
    """

    def __init__(self, **kw: Any) -> None:
        self.lv_path: str = ''
        self.lv_name: str = ''
        self.lv_uuid: str = ''
        self.vg_name: str = ''
        self.lv_size: str = ''
        self.tags: Dict[str, Any] = {}
        self.lv_tags: Dict[str, Any] = {}
        super().__init__('lv_name', 'lv_tags', **kw)
        self.lv_api = kw
        self.encrypted = self.tags.get('ceph.encrypted', '0') == '1'
        self.used_by_ceph = 'ceph.osd_id' in self.tags
        self.binary_change: str = 'lvchange'
        self.path: str = self.lv_path

    def __str__(self) -> str:
        return f'<{self.api_data.get("lv_path", self.name)}>'

    def as_dict(self) -> Dict[Any, Any]:
        obj: Dict[Any, Any] = {}
        obj.update(self.lv_api)
        obj['tags'] = self.tags
        obj['name'] = self.name
        obj['type'] = self.tags['ceph.type']
        obj['path'] = self.lv_path
        return obj

    def report(self) -> Dict[str, Any]:
        if not self.used_by_ceph:
            return {
                'name': self.lv_name,
                'comment': 'not used by ceph'
            }
        else:
            type_ = self.tags['ceph.type']
            report = {
                'name': self.lv_name,
                'osd_id': self.tags['ceph.osd_id'],
                'cluster_name': self.tags.get('ceph.cluster_name', conf.cluster),
                'type': type_,
                'osd_fsid': self.tags['ceph.osd_fsid'],
                'cluster_fsid': self.tags['ceph.cluster_fsid'],
                'osdspec_affinity': self.tags.get('ceph.osdspec_affinity', ''),
            }
            type_uuid = '{}_uuid'.format(type_)
            report[type_uuid] = self.tags['ceph.{}'.format(type_uuid)]
            return report

def create_lv(name_prefix: str,
              uuid: str,
              vg: Optional[VolumeGroup] = None,
              device: Optional[str] = None,
              slots: Optional[int] = None,
              extents: Optional[int] = None,
              size: Optional[int] = None,
              tags: Optional[Dict[str, str]] = None) -> Optional[Volume]:
    """
    Create a Logical Volume in a Volume Group. Command looks like::

        lvcreate -L 50G -n gfslv vg0

    ``name_prefix`` is required. If ``size`` is provided its expected to be a
    byte count. Tags are an optional dictionary and is expected to
    conform to the convention of prefixing them with "ceph." like::

        {"ceph.block_device": "/dev/ceph/osd-1"}

    :param name_prefix: name prefix for the LV, typically somehting like ceph-osd-block
    :param uuid: UUID to ensure uniqueness; is combined with name_prefix to
                 form the LV name
    :param vg: optional, pass an existing VG to create LV
    :param device: optional, device to use. Either device of vg must be passed
    :param slots: optional, number of slots to divide vg up, LV will occupy one
                    one slot if enough space is available
    :param extends: optional, how many lvm extends to use, supersedes slots
    :param size: optional, target LV size in bytes, supersedes extents,
                            resulting LV might be smaller depending on extent
                            size of the underlying VG
    :param tags: optional, a dict of lvm tags to set on the LV
    """
    name = '{}-{}'.format(name_prefix, uuid)
    if not vg:
        if not device:
            raise RuntimeError("Must either specify vg or device, none given")
        # check if a vgs starting with ceph already exists
        vgs = get_device_vgs(device, 'ceph')
        if vgs:
            vg = vgs[0]
        else:
            # create on if not
            vg = create_vg(device, name_prefix='ceph')
    assert(vg)

    if size:
        extents = vg.bytes_to_extents(size)
        logger.debug('size was passed: {} -> {}'.format(size, extents))
    elif slots and not extents:
        extents = vg.slots_to_extents(slots)
        logger.debug('slots was passed: {} -> {}'.format(slots, extents))

    if extents:
        command = [
            'lvcreate',
            '--yes',
            '-l',
            '{}'.format(extents),
            '-n', name, vg.vg_name
        ]
    # create the lv with all the space available, this is needed because the
    # system call is different for LVM
    else:
        command = [
            'lvcreate',
            '--yes',
            '-l',
            '100%FREE',
            '-n', name, vg.vg_name
        ]
    process.run(command, run_on_host=True)

    lv = get_single_lv(filters={'lv_name': name, 'vg_name': vg.vg_name})

    if tags is None:
        tags = {
            "ceph.osd_id": "null",
            "ceph.type": "null",
            "ceph.cluster_fsid": "null",
            "ceph.osd_fsid": "null",
        }
    # when creating a distinct type, the caller doesn't know what the path will
    # be so this function will set it after creation using the mapping
    # XXX add CEPH_VOLUME_LVM_DEBUG to enable -vvvv on lv operations
    type_path_tag = {
        'data': 'ceph.data_device',
        'block': 'ceph.block_device',
        'wal': 'ceph.wal_device',
        'db': 'ceph.db_device',
        'lockbox': 'ceph.lockbox_device',  # XXX might not ever need this lockbox sorcery
    }
    path_tag = type_path_tag.get(tags.get('ceph.type', ''))
    if path_tag and isinstance(lv, Volume):
        tags.update({path_tag: lv.lv_path})

    if isinstance(lv, Volume):
        lv.set_tags(tags)

    return lv


def create_lvs(volume_group: VolumeGroup, parts: int = 1, size: Optional[int] = None, name_prefix: str = 'ceph-lv') -> List[Optional[Volume]]:
    """
    Create multiple Logical Volumes from a Volume Group by calculating the
    proper extents from ``parts`` or ``size``. A custom prefix can be used
    (defaults to ``ceph-lv``), these names are always suffixed with a uuid.

    LV creation in ceph-volume will require tags, this is expected to be
    pre-computed by callers who know Ceph metadata like OSD IDs and FSIDs. It
    will probably not be the case when mass-creating LVs, so common/default
    tags will be set to ``"null"``.

    .. note:: LVs that are not in use can be detected by querying LVM for tags that are
              set to ``"null"``.

    :param volume_group: The volume group (vg) to use for LV creation
    :type group: ``VolumeGroup()`` object
    :param parts: Number of LVs to create *instead of* ``size``.
    :type parts: int
    :param size: Size (in gigabytes) of LVs to create, e.g. "as many 10gb LVs as possible"
    :type size: int
    :param extents: The number of LVM extents to use to create the LV. Useful if looking to have
    accurate LV sizes (LVM rounds sizes otherwise)
    """
    lvs = []
    tags = {
        "ceph.osd_id": "null",
        "ceph.type": "null",
        "ceph.cluster_fsid": "null",
        "ceph.osd_fsid": "null",
    }
    sizing = volume_group.sizing(parts=parts, size=size)
    for part in range(0, sizing['parts']):
        size = sizing['sizes']
        extents = sizing['extents']
        lvs.append(
            create_lv(name_prefix, str(uuid.uuid4()), vg=volume_group, extents=extents, tags=tags)
        )
    return lvs


def remove_lv(lv: Union[str, Volume]) -> bool:
    """
    Removes a logical volume given it's absolute path.

    Will return True if the lv is successfully removed or
    raises a RuntimeError if the removal fails.

    :param lv: A ``Volume`` object or the path for an LV
    """
    if isinstance(lv, Volume):
        path = lv.lv_path
    else:
        path = lv

    stdout, stderr, returncode = process.call(
        [
            'lvremove',
            '-v',  # verbose
            '-f',  # force it
            path
        ],
        run_on_host=True,
        show_command=True,
        terminal_verbose=True,
    )
    if returncode != 0:
        raise RuntimeError("Unable to remove %s" % path)
    return True


def get_lvs(fields: str = LV_FIELDS, filters: Optional[Dict[str, Any]] = None, tags: Optional[Dict[str, Any]] = None) -> List[Volume]:
    """
    Return a list of LVs that are available on the system and match the
    filters and tags passed. Argument filters takes a dictionary containing
    arguments required by -S option of LVM. Passing a list of LVM tags can be
    quite tricky to pass as a dictionary within dictionary, therefore pass
    dictionary of tags via tags argument and tricky part will be taken care of
    by the helper methods.

    :param fields: string containing list of fields to be displayed by the
                   lvs command
    :param sep: string containing separator to be used between two fields
    :param filters: dictionary containing LVM filters
    :param tags: dictionary containng LVM tags
    :returns: list of class Volume object representing LVs on the system
    """
    if filters is None:
        filters = {}
    if tags is None:
        tags = {}
    filters_str = make_filters_lvmcmd_ready(filters, tags)
    args = ['lvs'] + LV_CMD_OPTIONS + ['-S', filters_str, '-o', fields]

    stdout, stderr, returncode = process.call(args, run_on_host=True, verbose_on_failure=False)
    lvs_report = _output_parser(stdout, fields)
    return [Volume(**lv_report) for lv_report in lvs_report]


def get_single_lv(fields: str = LV_FIELDS, filters: Optional[Dict[str, Any]] = None, tags: Optional[Dict[str, Any]] = None) -> Optional[Volume]:
    """
    Wrapper of get_lvs() meant to be a convenience method to avoid the phrase::
        lvs = get_lvs()
        if len(lvs) >= 1:
            lv = lvs[0]
    """
    lvs = get_lvs(fields=fields, filters=filters, tags=tags)

    if len(lvs) == 0:
        return None
    if len(lvs) > 1:
        raise RuntimeError('Filters {} matched more than 1 LV present on this host.'.format(str(filters)))

    return lvs[0]


def get_lvs_from_osd_id(osd_id: str) -> List[Volume]:
    return get_lvs(tags={'ceph.osd_id': osd_id})


def get_single_lv_from_osd_id(osd_id: str) -> Optional[Volume]:
    return get_single_lv(tags={'ceph.osd_id': osd_id})


def get_lv_by_name(name: str) -> List[Volume]:
    stdout, stderr, returncode = process.call(
        ['lvs', '--noheadings', '-o', LV_FIELDS, '-S',
         'lv_name={}'.format(name)],
        run_on_host=True,
        verbose_on_failure=False
    )
    lvs = _output_parser(stdout, LV_FIELDS)
    return [Volume(**lv) for lv in lvs]


def get_lvs_by_tag(lv_tag: str) -> List[Volume]:
    stdout, stderr, returncode = process.call(
        ['lvs', '--noheadings', '--separator=";"', '-a', '-o', LV_FIELDS, '-S',
         'lv_tags={{{}}}'.format(lv_tag)],
        run_on_host=True,
        verbose_on_failure=False
    )
    lvs = _output_parser(stdout, LV_FIELDS)
    return [Volume(**lv) for lv in lvs]


def get_device_lvs(device: str, name_prefix: str = '') -> List[Volume]:
    stdout, stderr, returncode = process.call(
        ['pvs'] + LV_CMD_OPTIONS + ['-o', LV_FIELDS, device],
        run_on_host=True,
        verbose_on_failure=False
    )
    lvs = _output_parser(stdout, LV_FIELDS)
    return [Volume(**lv) for lv in lvs if lv['lv_name'] and
            lv['lv_name'].startswith(name_prefix)]

def get_lvs_from_path(devpath: str) -> List[Volume]:
    lvs = []
    if os.path.isabs(devpath):
        # we have a block device
        lvs = get_device_lvs(devpath)
        if not lvs:
            # maybe this was a LV path /dev/vg_name/lv_name or /dev/mapper/
            lvs = get_lvs(filters={'path': devpath})

    return lvs

def get_lv_by_fullname(full_name: str) -> Optional[Volume]:
    """
    returns LV by the specified LV's full name (formatted as vg_name/lv_name)
    """
    try:
        vg_name, lv_name = full_name.split('/')
        res_lv = get_single_lv(filters={'lv_name': lv_name,
                                        'vg_name': vg_name})
    except ValueError:
        res_lv = None
    return res_lv
