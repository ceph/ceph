import argparse
import sys
import time
import os
import json
from ceph_volume.api import lvm as api
from ceph_volume.util import disk
from ceph_volume import process

"""
The user is responsible for splitting the disk into data and metadata partitions.

If we are to partition a disk to be used as a cache layer, no partition can be
smaller than 2GB because ceph-volume creates vgs with PE = 1GB.

"""


# TODO handle OSD removal
# TODO add change caching mode
# TODO  stderr: WARNING: Maximum supported pool metadata size is 16.00 GiB.
# TODO  stderr: WARNING: recovery of pools without pool metadata spare LV is not automated.
# TODO do we want a spare partition or not?
# TODO what happens when the OSD is zapped?
#   the cache LVs will be detected by ceph-volume. See find_associated_devices()
#   actually, they will not. See ensure_associated_lvs(), which makes sure that
#   LVs are either lock, journal or wal. Will probably need to be done by the
#   orchestrator.
# TODO raise exceptions instead of print+return
# TODO add error handling at every step
# TODO what happens on disk failures?
# TODO make sure nothing breaks on filestore/non-LVM bluestore


"""
CHECKS

Q: What if the OSD ID passed with --osdid doesn't exists (ie isn't on this node)?
A: It will be filtered out when using .filter() and no origin_lv will be found.

Q: What if --db or --wal is passed and the OSD is standalone?
A: origin_lv won't be found as no LV will have the ceph.db_device or
    ceph.wal_device tag.

Q: For 'rm', what if the OSD doesn't have a cache?
A: origin_lv won't be found as no LV will have the ceph.cache_lv tag.

Q: What if you run cache add twice on the same OSD?
A: This plugin makes sure the origin LV isn't cached by looking at the tag ceph.cache_lv.

Q: What happens after hard reset?
A: 'ceph-volume cache info' shows that LVs are still cached as expeted.

"""

def pretty(s):
    print(json.dumps(s, indent=4, sort_keys=True))


def pretty_lv(lv):
    print('lv_name: ' + lv.lv_name)
    print('type: ' + lv.tags.get('ceph.type', ''))
    print('osdid: ' + lv.tags.get('ceph.osd_id', ''))
    print('vg_name: ' + lv.vg_name)
    print('lv_path: ' + lv.lv_path)
    print('\ntags:')
    pretty(lv.tags)
    # disabled as some tags might not be set
    # print('\nreport')
    # pretty(lv.report())
    print('-------------')


def get_terminal_size():
    rows, columns = os.popen('stty size', 'r').read().split()
    return int(rows), int(columns)


def get_lvs_caching_info():
    fields = 'lv_tags,lv_path,lv_name,vg_name,cache_mode,cache_policy,cache_settings'
    stdout, stderr, returncode = process.call(
        ['lvs', '--noheadings', '--readonly', '--separator=";"', '-a', '-o', fields],
        verbose_on_failure=False
    )
    return api._output_parser(stdout, fields)


# TODO add osdid to cache info
def print_cache_info(osdid=None):
    # TODO filter on osdid optionally
    osd_id_width = 8
    rows, columns = get_terminal_size()
    column_width = min(int((columns - osd_id_width) / 4), 18)
    s = '{0:>{w}}'.format('OSD', w=osd_id_width)
    # TODO change 'partition' to something else
    s = s + '{0:>{w}}'.format('Partition', w=column_width)
    s = s + '{0:>{w}}'.format('Cache mode', w=column_width)
    s = s + '{0:>{w}}'.format('Cache policy', w=column_width)
    s = s + '{0:>{w}}'.format('Cache settings', w=column_width)
    print(s)
    for item in get_lvs_caching_info():
        if item['cache_mode'] is not "" and item['lv_tags'] is not "":
            v = api.Volume(**item)
            s = '{0:>{w}}'.format(v.tags['ceph.osd_id'], w=osd_id_width)
            s = s + '{0:>{w}}'.format(v.tags['ceph.type'], w=column_width)
            s = s + '{0:>{w}}'.format(item['cache_mode'], w=column_width)
            s = s + '{0:>{w}}'.format(item['cache_policy'], w=column_width)
            s = s + '{0:>{w}}'.format(item['cache_settings'], w=column_width)
            print(s)


def get_lvs_caching_stats():
    fields = 'lv_tags,lv_path,lv_name,vg_name,cache_mode,cache_total_blocks,cache_used_blocks,cache_dirty_blocks,cache_read_hits,cache_read_misses,cache_write_hits,cache_write_misses'
    # Cache stats aren't printed by `lvs`` if --readonly is passed. This command
    # doesn't have the --readonly flag. TODO look into the consequences it might
    # have on the workload (with ongoing traffic against Ceph)
    stdout, stderr, returncode = process.call(
        ['lvs', '--noheadings', '--separator=";"', '-a', '-o', fields],
        verbose_on_failure=False
    )
    return api._output_parser(stdout, fields)


def print_cache_stats(osdid=None):
    # TODO print hit rate
    osd_id_width = 8
    rows, columns = get_terminal_size()
    column_width = min(int(columns / 4), 18)
    h = ''
    if not osdid:
        h = '{0:>{w}}'.format('OSD', w=osd_id_width)
        column_width = min(int((columns - osd_id_width) / 4), 18)
    h = h + '{0:>{w}}'.format('Read hits', w=column_width)
    h = h + '{0:>{w}}'.format('Read misses', w=column_width)
    h = h + '{0:>{w}}'.format('Write hits', w=column_width)
    h = h + '{0:>{w}}'.format('Write misses', w=column_width)
    if osdid:
        print(h)

    # TODO refactor this. Look at the mgr's iostat plugin.
    # TODO show the table's header again when it disappears
    while True:
        try:
            for item in get_lvs_caching_stats():
                # Need to test that lv_tags isn't empty because each OSD has two LVs
                # that will have cache_mode non-empty, its origin LV and the (new hidden) cache LV
                # The new hidden cache LV doesn't have tags, and we can't set them, so
                # we only want to print the info for the origin LV
                if item['cache_mode'] is not "" and item['lv_tags'] is not "":
                    v = api.Volume(**item)
                    if not osdid or (osdid and v.tags['ceph.osd_id'] == osdid):
                        s = ''
                        if not osdid:
                            print(h)
                            s = '{0:>{w}}'.format(v.tags['ceph.osd_id'], w=osd_id_width)
                        s = s + '{0:>{w}}'.format(item['cache_read_hits'], w=column_width)
                        s = s + '{0:>{w}}'.format(item['cache_read_misses'], w=column_width)
                        s = s + '{0:>{w}}'.format(item['cache_write_hits'], w=column_width)
                        s = s + '{0:>{w}}'.format(item['cache_write_misses'], w=column_width)
                        print(s)
                        if not osdid:
                            print()
            time.sleep(1)
        except KeyboardInterrupt:
            print('Interrupted')
            return


def create_lvmcache_pool(vg_name, data_lv, metadata_lv):
    process.run([
        'lvconvert',
        '--yes',
        '--poolmetadataspare', 'n',
        '--type', 'cache-pool',
        '--poolmetadata', metadata_lv.lv_path, data_lv.lv_path,
    ])


def create_lvmcache(vg_name, cache_lv, origin_lv):
    process.run([
        'lvconvert',
        '--yes',
        '--type', 'cache',
        '--cachepool', cache_lv.lv_path, origin_lv.lv_path
    ])


def set_lvmcache_caching_mode(caching_mode, vg_name, origin_lv):
    if caching_mode not in ['writeback', 'writethrough']:
        print('Unknown caching mode: ' + caching_mode)
        return

    process.run([
        'lvchange',
        '--cachemode', caching_mode, origin_lv.lv_path
    ])


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

    # TODO: update the "ceph.block_device" tag to point to the origin LV - later comment: why? is this still needed?
    # TODO: update these new LVs' tags (ceph.osd_id, ceph.cluster_fsid, etc.)
    cache_md_lv = api.create_lv(
        'cache_md_osd.' + osdid,
        vg_name,
        size=str(md_lv_size._b) + 'B',
        tags={'ceph.osd_id': osdid, 'ceph.type': 'cache_metadata', 'ceph.partition': md_partition},
        pv=md_partition)

    cache_data_lv = api.create_lv(
        'cache_osd.' + osdid,
        vg_name,
        size=str(data_lv_size._b) + 'B',
        tags={'ceph.osd_id': osdid, 'ceph.type': 'cache_data', 'ceph.partition': data_partition},
        pv=data_partition)

    return cache_md_lv, cache_data_lv


def _create_lvmcache(vg_name, origin_lv, cache_metadata_lv, cache_data_lv):
    # TODO: test that cache data is greater than metadata
    create_lvmcache_pool(vg_name, cache_data_lv, cache_metadata_lv)
    create_lvmcache(vg_name, cache_data_lv, origin_lv)

    # After this point, cache_metadata_lv and cache_data_lv are hidden, and
    # their names have changed from:
    #   cache_osd.5 to [cache_osd.5_cdata]
    #   cache_md_osd.5 to [cache_osd.5_cmeta]
    # these two partitions keep their tags. A new hidden partition has been created:
    #   [cache_osd.5]
    # We can't set its tags because "Operation not permitted on hidden LV [...]"

    cache_lv = api.get_lv(vg_name=vg_name, lv_name='['+cache_data_lv.name+']')
    if not cache_lv:
        print('Error: can\'t find newly created cache partition.')
        print('Creation of lvmcache probably failed. Aborting.')
        return

    # TODO do we need this tag?
    origin_lv.set_tag('ceph.cache_lv', cache_data_lv.lv_name)
    set_lvmcache_caching_mode('writeback', vg_name, origin_lv)

    return cache_lv


def add_lvmcache(vgname, origin_lv, md_partition, cache_data_partition, osdid):
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
    cachelv = _create_lvmcache(vg.name, origin_lv, cache_md_lv,
        cache_data_lv)

    return cachelv


def rm_lvmcache(vgname, osd_lv_name):
    osd_lv = api.get_lv(lv_name=osd_lv_name, vg_name=vgname)
    if not osd_lv or not osd_lv.tags.get('ceph.cache_lv', None):
        print('Can\'t find cache data LV.')
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
Manage lvmcache.

Add cache:

$> ceph-volume cache add
    --cachemetadata <metadata partition>
    --cachedata <data partition>
    --osdid <osd id>
    [--data|--db|--wal]

--data, --db and --wal indicate which partition to cache. Data is the default.

or:

$> ceph-volume cache add
    --cachemetadata <metadata partition>
    --cachedata <data partition>
    --origin <osd lvm name>
    --volumegroup <volume group>

Remove cache:

$> ceph-volume cache rm --osdid <id>

Get info:

$> ceph-volume cache info

Get cache usage numbers:

$> ceph-volume cache stats [--osdid <osd id>]

Dump LVs information:

$> ceph-volume cache dump

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
            '--origin',
            help='OSD data partition',
        )
        # other parts of the code use --osd-id
        # TODO add an --osd-fsid argument
        parser.add_argument(
            '--osdid',
            help='OSD id',
        )
        parser.add_argument(
            '--volumegroup',
            help='Volume group',
        )
        parser.add_argument(
            '--data',
            action='store_true',
            help='cache the OSD\'s data',
        )
        parser.add_argument(
            '--db',
            action='store_true',
            help='cache the OSD\'s db',
        )
        parser.add_argument(
            '--wal',
            action='store_true',
            help='cache the OSD\'s wal',
        )
        args = parser.parse_args(main_args)

        if len(self.argv) == 0:
            return parser.print_help()

        if self.argv[0] == 'dump':
            lvs = api.Volumes()
            for lv in lvs:
                pretty_lv(lv)
            return

        if args.osdid and args.origin:
            print('Can\'t have --osdid and --origin')
            return

        origin_lv = None

        # TODO think about the interface to possibly use caching "backends"
        # other than lvm. Maybe bcache, iCAS, etc. Maybe the easiest solution is
        # to rename this plugin to 'lvmcache'.
        if self.argv[0] == 'add':
            if args.osdid:
                lvs = api.Volumes()
                osdid = args.osdid
                lvs.filter(lv_tags={'ceph.osd_id': osdid})

                for lv in lvs:
                    # TODO update the cache's name accordingly to this
                    if args.data:
                        origin_lv = api.get_lv(lv_path=lv.tags.get('ceph.block_device', None))
                    elif args.db:
                        origin_lv = api.get_lv(lv_path=lv.tags.get('ceph.db_device', None))
                    elif args.wal:
                        origin_lv = api.get_lv(lv_path=lv.tags.get('ceph.wal_device', None))
                    else:
                        origin_lv = api.get_lv(lv_path=lv.tags.get('ceph.block_device', None))
                    if origin_lv:
                        break
                if not origin_lv:
                    print('Can\'t find origin LV for OSD ' + args.osdid)
                    return
                vg_name = origin_lv.vg_name
                if 'ceph.cache_lv' in origin_lv.tags:
                    print('Error: Origin LV is already cached')
                    return
            elif args.origin:
                # origin is already the LV's name, so no need to look for
                # --data, --db or --wal flags.
                vg_name = args.volumegroup
                origin_lv = api.get_lv(lv_name=args.origin, vg_name=vg_name)
                if not origin_lv:
                    print('Can\'t find origin LV ' + args.origin)
                osdid = origin_lv.tags.get('ceph.osd_id', None)

            add_lvmcache(
                vg_name,
                origin_lv,
                args.cachemetadata,
                args.cachedata,
                osdid)
        elif self.argv[0] == 'rm':
            # TODO handle when passing --origin instead of --osdid
            if args.osdid and not args.origin:
                lvs = api.Volumes()
                lvs.filter(lv_tags={'ceph.osd_id': args.osdid})
                for lv in lvs:
                    if lv.tags.get('ceph.cache_lv', None):
                        origin_lv = lv
                        break
            if not origin_lv:
                print('Can\'t find cache LV for OSD ' + args.osdid)
                return
            vg_name = origin_lv.vg_name
            rm_lvmcache(vg_name, origin_lv.name)
        elif self.argv[0] == 'info':
            print_cache_info()
            return
        elif self.argv[0] == 'stats':
            print_cache_stats(args.osdid)
            return


if __name__ == '__main__':
    main.Cache()
