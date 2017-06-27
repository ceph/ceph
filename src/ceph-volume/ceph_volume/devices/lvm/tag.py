"""
API for CRUD lvm tag operations. Follows the Ceph LVM tag naming convention that
prefixes tags with ``ceph.`` and uses ``=`` for assignment
"""
import json
from ceph_volume import process
from ceph_volume.exceptions import MultipleLVsError


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
        key, value = tag_assignment.split('=', 1)
        tag_mapping[key] = value

    return tag_mapping


def get_api_lvs():
    """
    Return the list of logical volumes available in the system using flags to include common
    metadata associated with them

    Command and sample JSON output, should look like::

        $ sudo lvs -o  lv_tags,lv_path,lv_name,vg_name --reportformat=json
        {
            "report": [
                {
                    "lv": [
                        {
                            "lv_tags":"",
                            "lv_path":"/dev/VolGroup00/LogVol00",
                            "lv_name":"LogVol00",
                            "vg_name":"VolGroup00"},
                        {
                            "lv_tags":"ceph.osd_fsid=aaa-fff-0000,ceph.osd_fsid=aaa-fff-bbbb,ceph.osd_id=0",
                            "lv_path":"/dev/osd_vg/OriginLV",
                            "lv_name":"OriginLV",
                            "vg_name":"osd_vg"
                        }
                    ]
                }
            ]
        }

    """
    stdout, stderr, returncode = process.call(
        ['sudo', 'lvs', '-o', 'lv_tags,lv_path,lv_name,vg_name', '--reportformat=json'])
    report = json.loads(b''.join(stdout).decode('utf-8'))
    for report_item in report.get('report', []):
        # is it possible to get more than one item in "report" ?
        return report_item['lv']
    return []


def get_lv(lv_name=None, lv_path=None):
    """
    Return a matching lv for the current system, requiring ``lv_name`` or
    ``lv_path``. Raises an error if more than one lv is found.
    """
    if not lv_name and not lv_path:
        raise TypeError('get_lv() requires either lv_name, or lv_path (none given)')
    lvs = get_lvs(lv_name=lv_name, lv_path=lv_path)
    if len(lvs) > 1:
        raise MultipleLVsError(lv_name, lv_path)
    return lvs[0]


def get_lvs(lv_name=None, vg_name=None, lv_path=None, lv_tags=None):
    """
    Return all known (logical) volumes for the current system, with the ability to filter
    them via keyword arguments. Always returns a list of volume objects

    To query by ``lv_tags`` a dict is required. For example, to find a volume
    that has an OSD ID of 0, the filter would look like::

        lv_tags={'ceph.osd_id': '0'}

    """
    api_lvs = get_api_lvs()
    volumes = []
    for lv_item in api_lvs:
        volumes.append(Volume(**lv_item))
    if lv_name:
        volumes = [i for i in volumes if i.lv_name == lv_name]

    if vg_name:
        volumes = [i for i in volumes if i.vg_name == vg_name]

    if lv_path:
        volumes = [i for i in volumes if i.lv_path == lv_path]

    if lv_tags:
        tag_filtered_volumes = []
        for k, v in lv_tags.items():
            for volume in volumes:
                if volume.tags.get(k) == v:
                    if volume not in tag_filtered_volumes:
                        tag_filtered_volumes.append(volume)
        return tag_filtered_volumes

    return volumes


class Volume(object):

    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)
        self.lv_api = kw
        self.name = kw['lv_name']
        self.tags = parse_tags(kw['lv_tags'])

    def __str__(self):
        return '%s - %s' % (self.name, self.lv_api['lv_path'])

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
        for current_key, current_value in self.tags.items():
            if current_key == key:
                tag = "%s=%s" % (current_key, current_value)
                process.call(['sudo', 'lvchange', '--deltag', tag, self.lv_api['lv_path']])
            process.call(
                [
                    'sudo', 'lvchange',
                    '--addtag', '%s=%s' % (key, value), self.lv_path
                ]
            )
