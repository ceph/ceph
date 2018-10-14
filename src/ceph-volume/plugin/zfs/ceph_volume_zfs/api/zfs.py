# vim: expandtab smarttab shiftwidth=4 softtabstop=4

"""
    API for zpool or zfs volume tag operations.
    Follows the Ceph ZFS tag naming convention
    that prefixes tags with ``ceph:`` and use
    ``=`` for assignment, and provides
    set of utilities for interacting with ZFS.
"""

import logging
import os
import uuid
import string
from ceph_volume import process
from ceph_volume import exceptions
from ceph_volume_zfs.exceptions import (
    MultipleZFSError, MultipleZPoolsError,
)

TAG = "ceph"
TAGEND = ":"
TAGPREFIX = TAG + TAGEND
TAGASSIGN = "="
CEPHTAGS = TAGPREFIX + "tags"

logger = logging.getLogger(__name__)


def create_osd_path(device, osd_id):
    """ 
    """ 
    return

def _zfs_parser(output, deleteTags=None):
    """
    Loading all output of a ZFS/zpool list command.
    First line contains the field names in CAPS
    remainder are the zpools/zvols per line

    :param output: The CLI output from the ZFS call
    """
    report = []
    field_items = output[0].lower().split()
    if deleteTags is not None:
        # delete the mlslabel tag, since it has no value
        for k in deleteTags:
            if k in field_items:
                field_items.remove(k)
    for line in output[1:]:
        line = line.strip()
        output_items = [i.strip() for i in line.split()]
        # map the output to the fields
        report.append(
            dict(zip(field_items, output_items))
        )
    return report


def parse_tags(zfs_tags):
    """
    Return a dictionary mapping of all the tags associated with
    a Volume from the ZFS API

    Input looks like::

       "ceph:osd_fsid=aaa-fff-bbbb,ceph:osd_id=0"

    For the above example, the expected return value would be::

        {
            "osd_fsid": "aaa-fff-bbbb",
            "osd_id": "0"
        }
    """
    if not zfs_tags:
        return {}
    tag_mapping = {}
    tags = zfs_tags.split(MULTITAGSEP)
    for ceph_tag in tags:
        if not ceph_tag.startswith(TAGPREFIX):
            continue
        cephpref, tag_assignment = ceph_tag.split(TAGEND, 1)
        key, value = tag_assignment.split(TAGASSIGN, 1)
        tag_mapping[key] = value
    return tag_mapping


def is_zfs(dev, zfsfs=None):
    """
    Boolean to detect if a device is a ZFS FS or not.
    """
    # Allowing to optionally pass `zfsfs` can help reduce repetitive checks for
    # multiple devices at once.
    zfsfs = zfsfs if zfsfs is not None else ZVolumes()
    if splitname.get('ZFS_NAME'):
        zfsfs.filter(name=splitname['ZFS_NAME'],
                     zpool_name=splitname['POOL_NAME'])
        return len(zfsfs) > 0
    return False


def get_zfsname_from_argument(argument):
    """
    Helper proxy function that consumes a possible logical volume passed
    in from the CLI in the form of `zpool/zfsvol`, but with some validation
    so that an argument that is a full path to a device can be ignored
    """
    if argument.startswith('/'):
        zfs = get_zfs(mountpoint=argument)
        return zfs
    try:
        zpool_name, zfs_name = argument.split('/')
    except (ValueError, AttributeError):
        return None
    return get_zfs(zfs_name=zfs_name, zpool_name=zpool_name)


def get_zfs(name=None, zpool_name=None, mountpoint=None, guid=None,
            zfs_tags=None):
    """
    Return a matching zfs for the current system, requiring ``name``,
    ``zpool_name``, ``mountpoint`` or ``tags``.
    Raises an error if more than one zfsvol is found.

    It is useful to use ``tags`` when trying to find a specific logical volume
    but it can also lead to multiple zfs being found, since a lot of metadata
    is shared between lvs of a distinct OSD.
    """
    if not any([name, zpool_name, mountpoint, guid, zfs_tags]):
        return None
    zfss = ZVolumes()
    return zfss.get(
        name=name, zpool_name=zpool_name, mountpoint=mountpoint, guid=guid,
        zfs_tags=zfs_tags
    )


def get_api_zfs_list():
    """
    Return the list of logical volumes available in the system using flags to
    include common metadata associated with them
    """
    stdout, stderr, returncode = process.call(
        ['zfs', 'list', '-p', '-o', 'all']
    )
    return _zfs_parser(stdout, ['mlslabel'])


def get_api_zpool_list():
    """
    Return the list of physical volumes configured for zpool

    """
    stdout, stderr, returncode = process.call(
        ['zpool', 'list', '-p', '-o', 'all']
    )
    return _zfs_parser(stdout)


def get_zfs_from_argument(argument):
    """
    Helper proxy function that consumes a possible zfs directory path passed in
    from the CLI in the form of `zfs`, but with some validation so that an
    argument that is a full path to a device can be ignored
    """
    if argument.startswith('/'):
        zfs = get_zfs(mountpoint=argument)
        return zfs
    try:
        pool_name, name = argument.split('/')
    except (ValueError, AttributeError):
        return None
    return get_zfs(name=name, pool_name=pool_name)


def get_zfs(name=None, zpool_name=None, mountpoint=None, guid=None,
            zfs_tags=None):
    """
    Return a matching zfs for the current system, requiring ``name``,
    ``zpool_name``, ``mountpoint`` or ``tags``.
    Raises an error if more than one zfs is found.

    It is useful to use ``tags`` when trying to find a specific logical volume
    """
    if not any([name, zpool_name, mountpoint, guid, zfs_tags]):
        return None
    zfss = ZVolumes()
    zfs = zfss.get(
        name=name, zpool_name=zpool_name, mountpoint=mountpoint, guid=guid,
        zfs_tags=zfs_tags
    )
    return zfs


def get_zpool(zpool_name=None):
    """
    Return a matching zpool (physical volume) for the current system,
    requiring ``zpool_name``. Raises an error if more than one
    zpool is found.
    """
    if not any([zpool_name]):
        return None
    zpools = ZVolumes()
    return zpools.get(zpool_name=zpool_name)


def create_zpool(device, osd_id):
    """
    Create a physical volume from a device, useful when devices need
    to be later mapped to journals.
    Return the path of the pool created.
    """
    fail_msg = "Unable to create zpool osd.%s on %s" % (osd_id, device)
    osdpool = 'osd.%s' % osd_id

    # gpart create -s GPT da0
    process.run(['gpart', 'create', '-s', 'GPT', device])

    # Create a partition aligned on 1M boundary, for the full size of
    # the remainder of the disk
    # gpart add -a 1M -t freebsd-zfs -l osd1 da0
    process.run(['gpart', 'add',
                 '-a', '1M',
                 '-t', 'freebsd-zfs',
                 '-l', osdpool,
                 device],
                )
    # Then create the ZFS filesystem on the partion using the label
    # but hide it from the regular tools
    process.run(['zpool', 'create',
                 '-m', 'none',
                 osdpool,
                 '/dev/gpt/%s' % osdpool],
                )
    zpools = ZPools()
    return zpools.get(name=osdpool)


def remove_zpool(zpool_name):
    """
    Removes a physical volume.
    """
    fail_msg = "Unable to remove zpool %s" % zpool_name
    process.run([
        'zpool', 'remove',
        '-f',  # force it
        zpool_name
        ],
        show_command=True,
        fail_msg=fail_msg,
    )


def create_zfsvol(device, osd_id, tags=None):
    """
    Create a Zpool and  Volume

    Tags are an optional dictionary and is expected to
    conform to the convention of prefixing them with "ceph:" like::

        {"ceph:block_device": "/dev/ceph/osd-1"}
    """
    type_path_tag = {
        'journal': 'journal_device',
        'data': 'data_device',
        'block': 'block_device',
        'wal': 'wal_device',
        'db': 'db_device',
    }
    fail_msg = "zfs create has failed"
    osdpool = 'osd.%s' % osd_id
    osdname = 'ceph-%s' % osd_id
    process.run([
        'zfs', 'create',
        '-o', 'mountpoint=/var/lib/ceph/osd/%s' % osdname,
        '-o', 'compression=on',
        '-o', 'checksum=off',
        '-o', 'atime=off',
        '-o', 'devices=off',
        '-o', 'exec=off',
        '-o', 'setuid=off',
        '%s/osd' % osdpool,
        ],
        fail_msg=fail_msg,
    )
    zfs = get_zfs(name='%s/osd' % osdpool)
    zfs.set_tags(tags)

    # when creating a distinct type, the caller doesn't know what the path will
    # be so this function will set it after creation using the mapping
    path_tag = type_path_tag.get(tags.get('type'))
    if path_tag:
        zfs.set_tags(
            {mountpoint: zfs.mountpoint}
        )
    return zfs


def remove_zfs(path):
    """
    Removes a logical volume given it's absolute path.

    Will return True if the lv is successfully removed or
    raises a RuntimeError if the removal fails.
    """
    stdout, stderr, returncode = process.call([
        'zfs', 'remove',
        '-v',  # verbose
        '-f',  # force it
        path
        ],
        show_command=True,
        terminal_verbose=True,
    )
    if returncode != 0:
        raise RuntimeError("Unable to remove %s" % path)
    return True


def create_zfs(zpool, parts=None, size=None, name_prefix='ceph-zfs'):
    """
    Create Logical ZVolume from a ZPool by calculating the
    proper extents from ``parts`` or ``size``. A custom prefix can be used
    (defaults to ``ceph-zfs``), these names are always suffixed with a uuid.

    ZV creation in ceph-volume will require tags, this is expected to be
    pre-computed by callers who know Ceph metadata like OSD IDs and FSIDs. It
    will probably not be the case when mass-creating ZVs, so common/default
    tags will be set to ``"null"``.

    .. note:: ZVs that are not in use can be detected by querying ZFS for tags
              that are set to ``"null"``.

    :param zpool: The zpool to use for zfs creation
    :type group: ``ZPool()`` object
    :param parts: Number of ZVs to create *instead of* ``size``.
    :type parts: int
    :param size: Size (in gigabytes) of ZVs to create,
                 e.g. "as many 10gb ZVs as possible"
    :type size: int
    """
    if parts is None and size is None:
        # fallback to just one part (using 100% of the vg)
        parts = 1
    zfs = []
    tags = {
        "ceph:osd_id": "null",
        "ceph:type": "null",
        "ceph:cluster_fsid": "null",
        "ceph:osd_fsid": "null",
    }
    sizing = zpool.sizing(parts=parts, size=size)
    for part in range(0, sizing['parts']):
        size = sizing['sizes']
        name = '%s-%s' % (name_prefix, uuid.uuid4())
        zfs.append(
            create_zfs(name, zpool.name, size="%sg" % size, tags=tags)
        )
    return zfs


def get_pool(pool_name=None, pool_tags=None):
    """
    Return a matching pool for the current system, requires ``pool_name`` or
    ``tags``. Raises an error if more than one vg is found.

    It is useful to use ``tags`` when trying to find a specific volume group,
    but it can also lead to multiple vgs being found.
    """
    if not any([pool_name, pool_tags]):
        return None
    pools = ZPools()
    return pools.get(name=pool_name, pool_tags=pool_tags)


class ZPools(list):
    """
    A list of all disks in the system
    """

    def __init__(self):
        self._populate()

    def _populate(self):
        # get all the zpools in the current system
        for zp_item in get_api_zpool_list():
            self.append(ZPool(**zp_item))

    def get(self, name=None, guid=None, zpool_tags=None):
        """
        fields = 'name,guid,size,comment'

        This is a bit expensive, since it will try to filter out all the
        matching items in the list, filter them out applying anything that was
        added and return the matching item.

        This method does *not* alter the list, and it will raise an error if
        multiple ZPools are matched

        It is useful to use ``tags`` when trying to find a specific logical
        volume, but it can also lead to multiple zfs being found, since a
        lot of metadata is shared between zfs of a distinct OSD.
        """
        if not any([name, guid, zpool_tags]):
            return None
        zpool = self._filter(
            name=name,
            guid=guid,
            zpool_tags=zpool_tags
        )
        if not zpool:
            return None
        if len(zpool) > 1:
            raise MultipleZPoolError(name)
        return zpool[0]

    def _filter(self, name=None, guid=None, zpool_tags=None):
        """
        The actual method that filters using a new list. Useful so that other
        methods that do not want to alter the contents of the list (e.g.
        ``self.find``) can operate safely.
        """
        filtered = [i for i in self]
        if name:
            filtered = [i for i in filtered if i.tags['name'] == name]

        if guid:
            filtered = [i for i in filtered if i.tags['guid'] == guid]

        # at this point, `filtered` has either all the zpool in self or is an
        # actual filtered list if any filters were applied
        if zpool_tags:
            tag_filtered = []
            for zpool in filtered:
                # all the tags we got need to match on the pool
                matches = all(zpool.tags.get(k) == str(v)
                              for k, v in zpool_tags.items())
                if matches:
                    tag_filtered.append(zpool)
            return tag_filtered

        return filtered


class ZVolumes(list):
    """
    A list of all known (logical) volumes for the current system,
    with the ability to filter them via keyword arguments.
    """

    def __init__(self):
        self._populate()

    def _populate(self):
        # get all the zfs volumes in the current system
        for zfs_item in get_api_zfs_list():
            self.append(ZVolume(**zfs_item))

    def _purge(self):
        """
        Delete all the items in the list, used internally only so that we can
        dynamically allocate the items when filtering without the concern of
        messing up the contents
        """
        self[:] = []

    def _filter(self, name=None, zpool_name=None, mountpoint=None, guid=None,
                zfs_tags=None):
        """
        The actual method that filters using a new list. Useful so that other
        methods that do not want to alter the contents of the list (e.g.
        ``self.find``) can operate safely.
        """
        filtered = [i for i in self]
        if name:
            filtered = [i for i in filtered if i.name == name]

        if zpool_name:
            filtered = [i for i in filtered if i.zpool_name == zpool_name]

        if guid:
            filtered = [i for i in filtered if i.guid == guid]

        if mountpoint:
            filtered = [i for i in filtered if i.mountpoint == mountpoint]

        # at this point, `filtered` has either all the volumes in self or is an
        # actual filtered list if any filters were applied
        if zfs_tags:
            tag_filtered = []
            for zfs in filtered:
                # all the tags we got need to match on the volume
                matches = all(getattr(zfs, k) == str(v)
                              for k, v in zfs_tags.items())
                if matches:
                    tag_filtered.append(zfs)
            return tag_filtered

        return filtered

    def filter(self, name=None, zpool_name=None, mountpoint=None,
               guid=None, zfs_tags=None):
        """
        Filter out volumes on top level attributes like ``name`` or by
        ``zfs_tags`` where a dict is required. For example, to find a volume
        that has an OSD ID of 0, the filter would look like::

            zfs_tags={'ceph:osd_id': '0'}

        """
        if not any([name, zpool_name, mountpoint, guid, zfs_tags]):
            raise TypeError('.filter() requires name, pool_name, mountpoint, '
                            ' guid, or tags (none given)')
        # first find the filtered volumes with the values in self
        filtered_zfs = self._filter(
            name=name,
            zpool_name=zpool_name,
            mountpoint=mountpoint,
            guid=guid,
            zfs_tags=zfs_tags
        )
        # then purge everything
        self._purge()
        # and add the filtered items
        self.extend(filtered_zfs)

    def get(self, name=None, zpool_name=None, mountpoint=None, guid=None,
            zfs_tags=None):
        """
        This is a bit expensive, since it will try to filter out all the
        matching items in the list, filter them out applying anything that was
        added and return the matching item.

        This method does *not* alter the list, and it will raise an error if
        multiple ZVs are matched

        It is useful to use ``tags`` when trying to find a specific logical
        volume, but it can also lead to multiple zfs being found, since a
        lot of metadata is shared between zfs of a distinct OSD.
        """
        if not any([name, mountpoint, guid, zfs_tags]):
            return None
        zfs = self._filter(
            name=name,
            zpool_name=zpool_name,
            mountpoint=mountpoint,
            guid=guid,
            zfs_tags=zfs_tags
        )
        if not zfs:
            return None
        if len(zfs) > 1:
            raise MultipleZFSError(name, mountpoint)
        return zfs[0]


class ZPool(object):
    """
    Represents a ZPool on the system, with some top-level attributes like
    name and path
    """

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)
        self.tags = kw
        self.name = kw['name']

    def __str__(self):
        return '<%s>' % (self.name)

    def __repr__(self):
        return self.__str__()


class ZVolume(object):
    """
    Represents a Logical Volume from ZFS, with some top-level attributes like
    name and parsed tags as a dictionary of key/value pairs.
    """

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)
        self.tags = kw
        self.name = kw['name']

    def __str__(self):
        return '<%s,%s>' % (self.name, self.tags['mountpoint'])

    def __repr__(self):
        return self.__str__()

    def as_dict(self):
        obj = {}
        obj.update(self.tags)
        obj['tags'] = self.tags
        obj['zpool_name'] = self.zpool_name
        obj['type'] = self.tags['type']
        obj['path'] = self.mountpoint
        return obj

    def clear_tags(self):
        """
        Removes all tags from the Logical Volume.
        """
        for k, v in self.tags.items():
            clear_tag(key)

    def clear_tag(self, key):
        """
        :param key: the attrribute to delete from the zfs volume
        Only delete ceph:xxxxx tags.
        """
        if TAGPREFIX in key:
            tag = "%s" % k
            process.run(['zfs', 'inherit', tag, self.name])

    def set_tags(self, newtags):
        """
        :param newtags: A dictionary of tag names and values, like::

            {
                "ceph:osd_fsid": "aaa-fff-bbbb",
                "ceph:osd_id": "0"
            }

        At the end of all modifications, the tags are refreshed to reflect
        ZFS's most current view.
        """
        for k, v in newtags.items():
            self.set_tag(k, v)

        # after setting all the tags, refresh them for the current object,
        # use the zfs_* identifiers to filter because those shouldn't change
        zfs_object = get_zfs(name=self.name, mountpoint=self.mountpoint)
        self.tags = zfs_object.tags

    def set_tag(self, key, value):
        """
        Set the key/value pair as an ZFS tag. Does not "refresh" the values of
        the current object for its tags. Meant to be a "fire and forget" type
        of modification.

        Only set ceph:xxxxx tags.
        """
        if TAGPREFIX in key:
            process.call([
                'zfs',
                'set', '%s=%s' % (key, value), self.name
            ])
