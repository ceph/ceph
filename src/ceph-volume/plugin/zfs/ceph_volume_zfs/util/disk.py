import re

from ceph_volume.util.disk import human_readable_size
from ceph_volume import process
from ceph_volume import sys_info

report_template = """
/dev/{geomname:<16} {mediasize:<16} {rotational!s:<7} {descr}"""
# {geomname:<25} {mediasize:<12} {rotational!s:<7} {mode!s:<9} {descr}"""

def geom_disk_parser(block):
    """
    Parses lines in 'geom disk list` output.

    Geom name: ada3
    Providers:
    1. Name: ada3
       Mediasize: 40018599936 (37G)
       Sectorsize: 512
       Stripesize: 4096
       Stripeoffset: 0
       Mode: r2w2e4
       descr: Corsair CSSD-F40GB2
       lunid: 5000000000000236
       ident: 111465010000101800EC
       rotationrate: 0
       fwsectors: 63
       fwheads: 16

    :param line: A string, with the full block for `geom disk list`
    """
    pairs = block.split(';')
    parsed = {}
    for pair in pairs:
        if 'Providers' in pair:
            continue
        try:
            column, value = pair.split(':')
        except ValueError:
            continue
        # fixup
        column = re.sub("\s+", "", column)
        column= re.sub("^[0-9]+\.", "", column)
        value = value.strip()
        value = re.sub('\([0-9A-Z]+\)', '', value)
        parsed[column.lower()] = value
    return parsed

def get_disk(diskname):
    """
    Captures all available info from geom
    along with interesting metadata like sectors, size, vendor,
    solid/rotational, etc...

    Returns a dictionary, with all the geom fields as keys.
    """

    command = ['/sbin/geom', 'disk', 'list', re.sub('/dev/', '', diskname)]
    out, err, rc = process.call(command)
    geom_block = "" 
    for line in out:
        line.strip()
        geom_block += ";" + line
    disk = geom_disk_parser(geom_block)
    return disk

def get_disks():
    command = ['/sbin/geom', 'disk', 'status', '-s']
    out, err, rc = process.call(command)
    disks = {}
    for path in out:
        dsk, rest1, rest2 = path.split()
        disk = get_disk(dsk)
        disks['/dev/'+dsk] = disk
    return disks

class Disks(object):

    def __init__(self, path=None):
        if not sys_info.devices:
            sys_info.devices = get_disks()
        self.disks = {}
        for k in sys_info.devices:
            if path != None:
                if path in k: 
                    self.disks[k] = Disk(k)
            else:
                self.disks[k] = Disk(k)

    def pretty_report(self, all=True):
        output = [
            report_template.format(
                geomname='Device Path',
                mediasize='Size',
                rotational='rotates',
                descr='Model name',
                mode='available',
            )]
        for disk in sorted(self.disks):
            output.append(self.disks[disk].report())
        return ''.join(output)
        
    def json_report(self):
        output = []
        for disk in sorted(self.disks):
            output.append(self.disks[disk].json_report())
        return output


class Disk(object):

    report_fields = [
        'rejected_reasons',
        'available',
        'path',
        'sys_api',
    ]
    pretty_report_sys_fields = [
        'human_readable_size',
        'model',
        'removable',
        'ro',
        'rotational',
        'sas_address',
        'scheduler_mode',
        'vendor',
    ]

    def __init__(self, path):
        self.abspath = path
        self.path = path
        self.reject_reasons = []
        self.available = True
        self.sys_api = sys_info.devices.get(path)

    def report(self):
        return report_template.format(
            geomname=self.sys_api.get('geomname'),
            mediasize=human_readable_size(int(self.sys_api.get('mediasize'))),
            rotational=int(self.sys_api.get('rotationrate')) != 0,
            mode=self.sys_api.get('mode'),
            descr=self.sys_api.get('descr')
        )

    def json_report(self):
        output = {k.strip('_'): v for k, v in vars(self).items()}
        return output

