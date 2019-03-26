import argparse
import sys
from ceph_volume.api import lvm as api
from ceph_volume.util import disk

"""
The user is responsible for splitting the disk into data and metadata partitions.

If we are to partition a disk to be used as a cache layer, no partition can be
smaller than 2GB because ceph-volume creates vgs with PE = 1GB.

"""


# partition sizes in GB
def _create_cache_lvs(vg_name, md_partition, data_partition, osdid):
    md_partition_size = disk.size_from_human_readable(disk.lsblk(md_partition)['SIZE'])
    data_partition_size = disk.size_from_human_readable(disk.lsblk(data_partition)['SIZE'])

    if md_partition_size < disk.Size(gb=2):
        print('Metadata partition is too small')
        return
    if data_partition_size < disk.Size(gb=2):
        print('Data partition is too small')
        return

    # ceph-volume creates volumes with extent size = 1GB
    # when a new lv is created, one extent needs to be used by LVM itself
    md_lv_size = md_partition_size - disk.Size(gb=1)
    data_lv_size = data_partition_size - disk.Size(gb=1)

    # TODO: update these new LVs' tags (ceph.cluster_fsid, etc.)
    cache_md_lv = api.create_lv(
        'cache_md_osd.' + osdid,
        vg_name,
        size=str(md_lv_size._b) + 'B',
        pv=md_partition)
    cache_md_lv.set_tag('ceph.type', 'cache_metadata')
    cache_md_lv.set_tag('ceph.partition', md_partition)

    cache_data_lv = api.create_lv(
        'cache_osd.' + osdid,
        vg_name,
        size=str(data_lv_size._b) + 'B',
        pv=data_partition)
    cache_data_lv.set_tag('ceph.type', 'cache_data')
    cache_data_lv.set_tag('ceph.partition', data_partition)

    return cache_md_lv, cache_data_lv


def _create_lvmcache(vg_name, osd_lv_name, cache_metadata_lv_name, cache_data_lv_name):
    osd_lv = api.get_lv(lv_name=osd_lv_name, vg_name=vg_name)
    # TODO: test that cache data is greater than metadata
    api.create_lvmcache_pool(vg_name, cache_data_lv_name, cache_metadata_lv_name)
    api.create_lvmcache(vg_name, cache_data_lv_name, osd_lv_name)

    osd_lv.set_tag('ceph.cache_lv', cache_data_lv_name)
    api.set_lvmcache_caching_mode('writeback', vg_name, osd_lv_name)

    _cache_data_lv_name = '[' + cache_data_lv_name + ']'
    cache_lv = api.get_lv(lv_name=_cache_data_lv_name, vg_name=vg_name)
    return cache_lv


def add_lvmcache(vgname, osd_lv_name, md_partition, cache_data_partition, osdid):
    """
    High-level function to be called. Expects the user or orchestrator to have
    partitioned the disk used for caching.
    """
    # TODO add pvcreate step?
    vg = api.get_vg(vg_name=vgname)
    # TODO don't fail if the LVs are already part of the vg
    api.extend_vg(vg, [md_partition, cache_data_partition])
    cache_md_lv, cache_data_lv = _create_cache_lvs(
        vg.name,
        md_partition,
        cache_data_partition,
        osdid
    )
    cachelv = _create_lvmcache(vg.name, osd_lv_name, cache_md_lv.name,
        cache_data_lv.name)

    return cachelv


def rm_lvmcache(vgname, osd_lv_name):
    osd_lv = api.get_lv(lv_name=osd_lv_name, vg_name=vgname)
    if not osd_lv or not osd_lv.tags['ceph.cache_lv']:
        print('Can\'t find cache data lv')
        return
    vg = api.get_vg(vg_name=vgname)
    cache_lv_name = osd_lv.tags['ceph.cache_lv']

    # get the partitions before removing the LVs
    data_lv_name = '[' + osd_lv.tags['ceph.cache_lv'] + '_cdata]'
    meta_lv_name = '[' + osd_lv.tags['ceph.cache_lv'] + '_cmeta]'
    data_lv = api.get_lv(lv_name=data_lv_name, vg_name=vgname)
    meta_lv = api.get_lv(lv_name=meta_lv_name, vg_name=vgname)
    data_partition = data_lv.tags['ceph.partition']
    md_partition = meta_lv.tags['ceph.partition']

    api.remove_lv(vgname + '/' + cache_lv_name)
    api.reduce_vg(vg, [data_partition, md_partition])

    osd_lv.clear_tag('ceph.cache_lv')


class Cache(object):

    help_menu = 'Deploy Cache'
    _help = """
Deploy lvmcache. Usage:

$> ceph-volume cache add --cachemetadata <metadata partition> --cachedata <data partition> --osddata <osd lvm name> --volumegroup <volume group>

or:

$> ceph-volume cache add --cachemetadata <metadata partition> --cachedata <data partition> --osdid <osd id>

Remove cache:

$> ceph-volume cache rm --osdid <id>
    """
    name = 'cache'

    def __init__(self, argv=sys.argv):
        self.mapper = {
        }
        self.argv = argv

    
    def help(self):
        return self._help


    def _get_split_args(self):
        subcommands = self.mapper.keys()
        slice_on_index = len(self.argv) + 1
        pruned_args = self.argv[1:]
        for count, arg in enumerate(pruned_args):
            if arg in subcommands:
                slice_on_index = count
                break
        return pruned_args[:slice_on_index], pruned_args[slice_on_index:]


    def main(self, argv=None):
        main_args, subcommand_args = self._get_split_args()
        parser = argparse.ArgumentParser(
            prog='cache',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self.help(),
        )
        parser.add_argument(
            '--cachemetadata',
            help='Cache metadata partition',
        )
        parser.add_argument(
            '--cachedata',
            help='Cache data partition',
        )
        parser.add_argument(
            '--osddata',
            help='OSD data partition',
        )
        parser.add_argument(
            '--osdid',
            help='OSD id',
        )
        parser.add_argument(
            '--volumegroup',
            help='Volume group',
        )
        args = parser.parse_args(main_args)
        if len(self.argv) <= 1:
            return parser.print_help()

        if args.osdid and not args.osddata:
            lvs = api.Volumes()
            for lv in lvs:
                if lv.tags.get('ceph.osd_id', '') == args.osdid:
                    osd_lv_name = lv.name
                    vg_name = lv.vg_name
                    osdid = args.osdid
                    break
        else:
            osd_lv_name = args.osddata
            vg_name = args.volumegroup
            lvs = api.Volumes()
            for lv in lvs:
                if lv.name == args.osddata:
                    osd_lv_name = lv.name
                    vg_name = lv.vg_name
                    osdid = lv.tags.get('ceph.osd_id', None)
                    break

        # TODO make sure the OSD exists (ie is on this node)
        if self.argv[0] == 'add':
            add_lvmcache(
                vg_name,
                osd_lv_name,
                args.cachemetadata,
                args.cachedata,
                osdid)
        elif self.argv[0] == 'rm':
            # TODO verify that the OSD has a cache
            rm_lvmcache(vg_name, osd_lv_name)


if __name__ == '__main__':
    main.Cache()
