"""
API for CRUD lvm tag operations. Follows the Ceph LVM tag naming convention
that prefixes tags with ``ceph.`` and uses ``=`` for assignment, and provides
set of utilities for interacting with LVM.
"""
from ceph_volume import process
from ceph_volume.exceptions import MultipleLVsError, MultipleVGsError, MultiplePVsError


def _output_parser(output, fields):
    """
    Newer versions of LVM allow ``--reportformat=json``, but older versions,
    like the one included in Xenial do not. LVM has the ability to filter and
    format its output so we assume the output will be in a format this parser
    can handle (using ',' as a delimiter)

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

        # spliting on ';' because that is what the lvm call uses as
        # '--separator'
        output_items = [i.strip() for i in line.split(';')]
        # map the output to the fiels
        report.append(
            dict(zip(field_items, output_items))
        )

    return report


def parse_tags(lv_tags):
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


def get_api_vgs():
    """
    Return the list of group volumes available in the system using flags to
    include common metadata associated with them

    Command and sample delimeted output, should look like::

        $ vgs --noheadings --separator=';' \
          -o vg_name,pv_count,lv_count,snap_count,vg_attr,vg_size,vg_free
          ubuntubox-vg;1;2;0;wz--n-;299.52g;12.00m
          osd_vg;3;1;0;wz--n-;29.21g;9.21g

    """
    fields = 'vg_name,pv_count,lv_count,snap_count,vg_attr,vg_size,vg_free'
    stdout, stderr, returncode = process.call(
        ['vgs', '--noheadings', '--separator=";"', '-o', fields]
    )
    return _output_parser(stdout, fields)


def get_api_lvs():
    """
    Return the list of logical volumes available in the system using flags to include common
    metadata associated with them

    Command and delimeted output, should look like::

        $ lvs --noheadings --separator=';' -o lv_tags,lv_path,lv_name,vg_name
          ;/dev/ubuntubox-vg/root;root;ubuntubox-vg
          ;/dev/ubuntubox-vg/swap_1;swap_1;ubuntubox-vg

    """
    fields = 'lv_tags,lv_path,lv_name,vg_name,lv_uuid'
    stdout, stderr, returncode = process.call(
        ['lvs', '--noheadings', '--separator=";"', '-o', fields]
    )
    return _output_parser(stdout, fields)


def get_api_pvs():
    """
    Return the list of physical volumes configured for lvm and available in the
    system using flags to include common metadata associated with them like the uuid

    Command and delimeted output, should look like::

        $ pvs --noheadings --separator=';' -o pv_name,pv_tags,pv_uuid
          /dev/sda1;;
          /dev/sdv;;07A4F654-4162-4600-8EB3-88D1E42F368D

    """
    fields = 'pv_name,pv_tags,pv_uuid'

    # note the use of `pvs -a` which will return every physical volume including
    # ones that have not been initialized as "pv" by LVM
    stdout, stderr, returncode = process.call(
        ['pvs', '-a', '--no-heading', '--separator=";"', '-o', fields]
    )

    return _output_parser(stdout, fields)


def get_lv_from_argument(argument):
    """
    Helper proxy function that consumes a possible logical volume passed in from the CLI
    in the form of `vg/lv`, but with some validation so that an argument that is a full
    path to a device can be ignored
    """
    if argument.startswith('/'):
        lv = get_lv(lv_path=argument)
        return lv
    try:
        vg_name, lv_name = argument.split('/')
    except (ValueError, AttributeError):
        return None
    return get_lv(lv_name=lv_name, vg_name=vg_name)


def get_lv(lv_name=None, vg_name=None, lv_path=None, lv_uuid=None, lv_tags=None):
    """
    Return a matching lv for the current system, requiring ``lv_name``,
    ``vg_name``, ``lv_path`` or ``tags``. Raises an error if more than one lv
    is found.

    It is useful to use ``tags`` when trying to find a specific logical volume,
    but it can also lead to multiple lvs being found, since a lot of metadata
    is shared between lvs of a distinct OSD.
    """
    if not any([lv_name, vg_name, lv_path, lv_uuid, lv_tags]):
        return None
    lvs = Volumes()
    return lvs.get(
        lv_name=lv_name, vg_name=vg_name, lv_path=lv_path, lv_uuid=lv_uuid,
        lv_tags=lv_tags
    )


def get_pv(pv_name=None, pv_uuid=None, pv_tags=None):
    """
    Return a matching pv (physical volume) for the current system, requiring
    ``pv_name``, ``pv_uuid``, or ``pv_tags``. Raises an error if more than one
    pv is found.
    """
    if not any([pv_name, pv_uuid, pv_tags]):
        return None
    pvs = PVolumes()
    return pvs.get(pv_name=pv_name, pv_uuid=pv_uuid, pv_tags=pv_tags)


def create_pv(device):
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
    ])


def create_vg(name, *devices):
    """
    Create a Volume Group. Command looks like::

        vgcreate --force --yes group_name device

    Once created the volume group is returned as a ``VolumeGroup`` object
    """
    process.run([
        'vgcreate',
        '--force',
        '--yes',
        name] + list(devices)
    )

    vg = get_vg(vg_name=name)
    return vg


def remove_lv(path):
    """
    Removes a logical volume given it's absolute path.

    Will return True if the lv is successfully removed or
    raises a RuntimeError if the removal fails.
    """
    stdout, stderr, returncode = process.call(
        [
            'lvremove',
            '-v',  # verbose
            '-f',  # force it
            path
        ],
        show_command=True,
        terminal_verbose=True,
    )
    if returncode != 0:
        raise RuntimeError("Unable to remove %s".format(path))
    return True


def create_lv(name, group, size=None, tags=None):
    """
    Create a Logical Volume in a Volume Group. Command looks like::

        lvcreate -L 50G -n gfslv vg0

    ``name``, ``group``, are required. If ``size`` is provided it must follow
    lvm's size notation (like 1G, or 20M). Tags are an optional dictionary and is expected to
    conform to the convention of prefixing them with "ceph." like::

        {"ceph.block_device": "/dev/ceph/osd-1"}
    """
    # XXX add CEPH_VOLUME_LVM_DEBUG to enable -vvvv on lv operations
    type_path_tag = {
        'journal': 'ceph.journal_device',
        'data': 'ceph.data_device',
        'block': 'ceph.block_device',
        'wal': 'ceph.wal_device',
        'db': 'ceph.db_device',
        'lockbox': 'ceph.lockbox_device',  # XXX might not ever need this lockbox sorcery
    }
    if size:
        process.run([
            'lvcreate',
            '--yes',
            '-L',
            '%s' % size,
            '-n', name, group
        ])
    # create the lv with all the space available, this is needed because the
    # system call is different for LVM
    else:
        process.run([
            'lvcreate',
            '--yes',
            '-l',
            '100%FREE',
            '-n', name, group
        ])

    lv = get_lv(lv_name=name, vg_name=group)
    lv.set_tags(tags)

    # when creating a distinct type, the caller doesn't know what the path will
    # be so this function will set it after creation using the mapping
    path_tag = type_path_tag.get(tags.get('ceph.type'))
    if path_tag:
        lv.set_tags(
            {path_tag: lv.lv_path}
        )
    return lv


def get_vg(vg_name=None, vg_tags=None):
    """
    Return a matching vg for the current system, requires ``vg_name`` or
    ``tags``. Raises an error if more than one vg is found.

    It is useful to use ``tags`` when trying to find a specific volume group,
    but it can also lead to multiple vgs being found.
    """
    if not any([vg_name, vg_tags]):
        return None
    vgs = VolumeGroups()
    return vgs.get(vg_name=vg_name, vg_tags=vg_tags)


class VolumeGroups(list):
    """
    A list of all known volume groups for the current system, with the ability
    to filter them via keyword arguments.
    """

    def __init__(self):
        self._populate()

    def _populate(self):
        # get all the vgs in the current system
        for vg_item in get_api_vgs():
            self.append(VolumeGroup(**vg_item))

    def _purge(self):
        """
        Deplete all the items in the list, used internally only so that we can
        dynamically allocate the items when filtering without the concern of
        messing up the contents
        """
        self[:] = []

    def _filter(self, vg_name=None, vg_tags=None):
        """
        The actual method that filters using a new list. Useful so that other
        methods that do not want to alter the contents of the list (e.g.
        ``self.find``) can operate safely.

        .. note:: ``vg_tags`` is not yet implemented
        """
        filtered = [i for i in self]
        if vg_name:
            filtered = [i for i in filtered if i.vg_name == vg_name]

        # at this point, `filtered` has either all the volumes in self or is an
        # actual filtered list if any filters were applied
        if vg_tags:
            tag_filtered = []
            for volume in filtered:
                matches = all(volume.tags.get(k) == str(v) for k, v in vg_tags.items())
                if matches:
                    tag_filtered.append(volume)
            return tag_filtered

        return filtered

    def filter(self, vg_name=None, vg_tags=None):
        """
        Filter out groups on top level attributes like ``vg_name`` or by
        ``vg_tags`` where a dict is required. For example, to find a Ceph group
        with dmcache as the type, the filter would look like::

            vg_tags={'ceph.type': 'dmcache'}

        .. warning:: These tags are not documented because they are currently
                     unused, but are here to maintain API consistency
        """
        if not any([vg_name, vg_tags]):
            raise TypeError('.filter() requires vg_name or vg_tags (none given)')
        # first find the filtered volumes with the values in self
        filtered_groups = self._filter(
            vg_name=vg_name,
            vg_tags=vg_tags
        )
        # then purge everything
        self._purge()
        # and add the filtered items
        self.extend(filtered_groups)

    def get(self, vg_name=None, vg_tags=None):
        """
        This is a bit expensive, since it will try to filter out all the
        matching items in the list, filter them out applying anything that was
        added and return the matching item.

        This method does *not* alter the list, and it will raise an error if
        multiple VGs are matched

        It is useful to use ``tags`` when trying to find a specific volume group,
        but it can also lead to multiple vgs being found (although unlikely)
        """
        if not any([vg_name, vg_tags]):
            return None
        vgs = self._filter(
            vg_name=vg_name,
            vg_tags=vg_tags
        )
        if not vgs:
            return None
        if len(vgs) > 1:
            # this is probably never going to happen, but it is here to keep
            # the API code consistent
            raise MultipleVGsError(vg_name)
        return vgs[0]


class Volumes(list):
    """
    A list of all known (logical) volumes for the current system, with the ability
    to filter them via keyword arguments.
    """

    def __init__(self):
        self._populate()

    def _populate(self):
        # get all the lvs in the current system
        for lv_item in get_api_lvs():
            self.append(Volume(**lv_item))

    def _purge(self):
        """
        Deplete all the items in the list, used internally only so that we can
        dynamically allocate the items when filtering without the concern of
        messing up the contents
        """
        self[:] = []

    def _filter(self, lv_name=None, vg_name=None, lv_path=None, lv_uuid=None, lv_tags=None):
        """
        The actual method that filters using a new list. Useful so that other
        methods that do not want to alter the contents of the list (e.g.
        ``self.find``) can operate safely.
        """
        filtered = [i for i in self]
        if lv_name:
            filtered = [i for i in filtered if i.lv_name == lv_name]

        if vg_name:
            filtered = [i for i in filtered if i.vg_name == vg_name]

        if lv_uuid:
            filtered = [i for i in filtered if i.lv_uuid == lv_uuid]

        if lv_path:
            filtered = [i for i in filtered if i.lv_path == lv_path]

        # at this point, `filtered` has either all the volumes in self or is an
        # actual filtered list if any filters were applied
        if lv_tags:
            tag_filtered = []
            for volume in filtered:
                # all the tags we got need to match on the volume
                matches = all(volume.tags.get(k) == str(v) for k, v in lv_tags.items())
                if matches:
                    tag_filtered.append(volume)
            return tag_filtered

        return filtered

    def filter(self, lv_name=None, vg_name=None, lv_path=None, lv_uuid=None, lv_tags=None):
        """
        Filter out volumes on top level attributes like ``lv_name`` or by
        ``lv_tags`` where a dict is required. For example, to find a volume
        that has an OSD ID of 0, the filter would look like::

            lv_tags={'ceph.osd_id': '0'}

        """
        if not any([lv_name, vg_name, lv_path, lv_uuid, lv_tags]):
            raise TypeError('.filter() requires lv_name, vg_name, lv_path, lv_uuid, or tags (none given)')
        # first find the filtered volumes with the values in self
        filtered_volumes = self._filter(
            lv_name=lv_name,
            vg_name=vg_name,
            lv_path=lv_path,
            lv_uuid=lv_uuid,
            lv_tags=lv_tags
        )
        # then purge everything
        self._purge()
        # and add the filtered items
        self.extend(filtered_volumes)

    def get(self, lv_name=None, vg_name=None, lv_path=None, lv_uuid=None, lv_tags=None):
        """
        This is a bit expensive, since it will try to filter out all the
        matching items in the list, filter them out applying anything that was
        added and return the matching item.

        This method does *not* alter the list, and it will raise an error if
        multiple LVs are matched

        It is useful to use ``tags`` when trying to find a specific logical volume,
        but it can also lead to multiple lvs being found, since a lot of metadata
        is shared between lvs of a distinct OSD.
        """
        if not any([lv_name, vg_name, lv_path, lv_uuid, lv_tags]):
            return None
        lvs = self._filter(
            lv_name=lv_name,
            vg_name=vg_name,
            lv_path=lv_path,
            lv_uuid=lv_uuid,
            lv_tags=lv_tags
        )
        if not lvs:
            return None
        if len(lvs) > 1:
            raise MultipleLVsError(lv_name, lv_path)
        return lvs[0]


class PVolumes(list):
    """
    A list of all known (physical) volumes for the current system, with the ability
    to filter them via keyword arguments.
    """

    def __init__(self):
        self._populate()

    def _populate(self):
        # get all the pvs in the current system
        for pv_item in get_api_pvs():
            self.append(PVolume(**pv_item))

    def _purge(self):
        """
        Deplete all the items in the list, used internally only so that we can
        dynamically allocate the items when filtering without the concern of
        messing up the contents
        """
        self[:] = []

    def _filter(self, pv_name=None, pv_uuid=None, pv_tags=None):
        """
        The actual method that filters using a new list. Useful so that other
        methods that do not want to alter the contents of the list (e.g.
        ``self.find``) can operate safely.
        """
        filtered = [i for i in self]
        if pv_name:
            filtered = [i for i in filtered if i.pv_name == pv_name]

        if pv_uuid:
            filtered = [i for i in filtered if i.pv_uuid == pv_uuid]

        # at this point, `filtered` has either all the physical volumes in self
        # or is an actual filtered list if any filters were applied
        if pv_tags:
            tag_filtered = []
            for pvolume in filtered:
                matches = all(pvolume.tags.get(k) == str(v) for k, v in pv_tags.items())
                if matches:
                    tag_filtered.append(pvolume)
            # return the tag_filtered pvolumes here, the `filtered` list is no
            # longer useable
            return tag_filtered

        return filtered

    def filter(self, pv_name=None, pv_uuid=None, pv_tags=None):
        """
        Filter out volumes on top level attributes like ``pv_name`` or by
        ``pv_tags`` where a dict is required. For example, to find a physical volume
        that has an OSD ID of 0, the filter would look like::

            pv_tags={'ceph.osd_id': '0'}

        """
        if not any([pv_name, pv_uuid, pv_tags]):
            raise TypeError('.filter() requires pv_name, pv_uuid, or pv_tags (none given)')
        # first find the filtered volumes with the values in self
        filtered_volumes = self._filter(
            pv_name=pv_name,
            pv_uuid=pv_uuid,
            pv_tags=pv_tags
        )
        # then purge everything
        self._purge()
        # and add the filtered items
        self.extend(filtered_volumes)

    def get(self, pv_name=None, pv_uuid=None, pv_tags=None):
        """
        This is a bit expensive, since it will try to filter out all the
        matching items in the list, filter them out applying anything that was
        added and return the matching item.

        This method does *not* alter the list, and it will raise an error if
        multiple pvs are matched

        It is useful to use ``tags`` when trying to find a specific logical volume,
        but it can also lead to multiple pvs being found, since a lot of metadata
        is shared between pvs of a distinct OSD.
        """
        if not any([pv_name, pv_uuid, pv_tags]):
            return None
        pvs = self._filter(
            pv_name=pv_name,
            pv_uuid=pv_uuid,
            pv_tags=pv_tags
        )
        if not pvs:
            return None
        if len(pvs) > 1:
            raise MultiplePVsError(pv_name)
        return pvs[0]


class VolumeGroup(object):
    """
    Represents an LVM group, with some top-level attributes like ``vg_name``
    """

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)
        self.name = kw['vg_name']
        self.tags = parse_tags(kw.get('vg_tags', ''))

    def __str__(self):
        return '<%s>' % self.name

    def __repr__(self):
        return self.__str__()


class Volume(object):
    """
    Represents a Logical Volume from LVM, with some top-level attributes like
    ``lv_name`` and parsed tags as a dictionary of key/value pairs.
    """

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)
        self.lv_api = kw
        self.name = kw['lv_name']
        self.tags = parse_tags(kw['lv_tags'])

    def __str__(self):
        return '<%s>' % self.lv_api['lv_path']

    def __repr__(self):
        return self.__str__()

    def as_dict(self):
        obj = {}
        obj.update(self.lv_api)
        obj['tags'] = self.tags
        obj['name'] = self.name
        obj['type'] = self.tags['ceph.type']
        obj['path'] = self.lv_path
        return obj

    def clear_tags(self):
        """
        Removes all tags from the Logical Volume.
        """
        for k, v in self.tags.items():
            tag = "%s=%s" % (k, v)
            process.run(['lvchange', '--deltag', tag, self.lv_path])

    def set_tags(self, tags):
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
        # lv_* identifiers to filter because those shouldn't change
        lv_object = get_lv(lv_name=self.lv_name, lv_path=self.lv_path)
        self.tags = lv_object.tags

    def set_tag(self, key, value):
        """
        Set the key/value pair as an LVM tag. Does not "refresh" the values of
        the current object for its tags. Meant to be a "fire and forget" type
        of modification.
        """
        # remove it first if it exists
        if self.tags.get(key):
            current_value = self.tags[key]
            tag = "%s=%s" % (key, current_value)
            process.call(['lvchange', '--deltag', tag, self.lv_api['lv_path']])

        process.call(
            [
                'lvchange',
                '--addtag', '%s=%s' % (key, value), self.lv_path
            ]
        )


class PVolume(object):
    """
    Represents a Physical Volume from LVM, with some top-level attributes like
    ``pv_name`` and parsed tags as a dictionary of key/value pairs.
    """

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)
        self.pv_api = kw
        self.name = kw['pv_name']
        self.tags = parse_tags(kw['pv_tags'])

    def __str__(self):
        return '<%s>' % self.pv_api['pv_name']

    def __repr__(self):
        return self.__str__()

    def set_tags(self, tags):
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
        pv_object = get_pv(pv_name=self.pv_name, pv_uuid=self.pv_uuid)
        self.tags = pv_object.tags

    def set_tag(self, key, value):
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
            process.call(['pvchange', '--deltag', tag, self.pv_name])

        process.call(
            [
                'pvchange',
                '--addtag', '%s=%s' % (key, value), self.pv_name
            ]
        )
