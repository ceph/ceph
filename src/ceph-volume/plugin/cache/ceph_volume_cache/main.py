import argparse
import sys
from ceph_volume.api import lvm as api

"""
The user is responsible for splitting the disk into data and metadata partitions.

If we are to partition a disk to be used as a cache layer, no partition can be
smaller than 2GB because ceph-volume creates vgs with PE = 1GB.

"""

# partition sizes in GB
def _create_cache_lvs(vg_name, cache_md_partition, cache_data_partition, md_lv_size, data_lv_size):
    cache_md_lv = api.create_lv('cache_metadata', vg_name, extents=None, size=md_lv_size, tags=None,
        uuid_name=True, pv=cache_md_partition)
    cache_data_lv = api.create_lv('cache_data', vg_name, extents=None, size=data_lv_size, tags=None,
        uuid_name=True, pv=cache_data_partition)
    
    return cache_md_lv, cache_data_lv


def _create_lvmcache(vg_name, osd_lv_name, cache_metadata_lv_name, cache_data_lv_name):
    ''' TODO: test that cache is greater than meta to make sure the order of the
        arguments was respected '''
    api.create_lvmcache_pool(vg_name, cache_data_lv_name, cache_metadata_lv_name)
    api.create_lvmcache(vg_name, cache_data_lv_name, osd_lv_name)
    api.set_lvmcache_caching_mode('writeback', vg_name, osd_lv_name)


def add_lvmcache(vgname, osd_lv_name, cache_md_partition, cache_data_partition, md_partition_size, data_partition_size):
    """
    High-level function to be called. Expects the user or orchestrator to have
    partitioned the disk used for caching.
    """
    # TODO add pvcreate step?
    vg = api.get_vg(vg_name=vgname)
    api.extend_vg(vg, [cache_md_partition, cache_data_partition])
    cache_md_lv, cache_data_lv = _create_cache_lvs(vg.name, cache_md_partition, cache_data_partition, md_partition_size, data_partition_size)
    _create_lvmcache(vg.name, osd_lv_name, cache_md_lv.name, cache_data_lv.name)


class Cache(object):

    help_menu = 'Deploy Cache'
    _help = """
Deploy lvmcache. Usage:

$> ceph-volume cache add --cachemetadata <metadata partition> --cachedata <data partition> --cachemetadata_size <metadata partition size> --cachedata_size <data partition size> --osddata <osd lvm name> --volumegroup <volume group>

or:

$> ceph-volume cache add --cachemetadata <metadata partition> --cachedata <data partition> --cachemetadata_size <metadata partition size> --cachedata_size <data partition size> --osdid <osd id>
    """
    name = 'cache'

    def __init__(self, argv=None):
        self.mapper = {
        }
        if argv is None:
            self.argv = sys.argv
        else:
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
            '--cachemetadata_size',
            help='Cache metadata partition size in GB',
        )
        parser.add_argument(
            '--cachedata_size',
            help='Cache data partition size in GB',
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

        if args.osd_id and not args.osddata:
            lvs = api.Volumes()
            for lv in lvs:
                if lv.tags['ceph.osd_id'] == args.osdid:
                    osd_lv_name = lv.name
                    vg_name = lv.vg_name
        else:
            osd_lv_name = args.osddata
            vg_name = args.volumegroup

        add_lvmcache(vg_name,
            osd_lv_name,
            args.cachemetadata,
            args.cachedata,
            args.cachemetadata_size,
            args.cachedata_size)


if __name__ == '__main__':
    main.Cache()
