import argparse
import sys
import time
import os
import json
from ceph_volume.api import lvm as api
from ceph_volume.util import disk
from ceph_volume import process, terminal

"""
The user is responsible for splitting the disk into data and metadata partitions.

If we are to partition a disk to be used as a cache layer, no partition can be
smaller than 2GB because ceph-volume creates vgs with PE = 1GB.

"""

# TODO  stderr: WARNING: Maximum supported pool metadata size is 16.00 GiB.
# TODO  stderr: WARNING: recovery of pools without pool metadata spare LV is not automated.
# TODO do we want a spare partition or not?
# TODO add cache size to `cache info`
# TODO in documentation make sure to mention the ratio of metadata to data from man lvmcache


"""
CHECKS

Q: What if the OSD ID passed with --osdid doesn't exists (ie isn't on this node)?
A: It will be filtered out when using .filter() and no origin_lv will be found.

Q: What if --db or --wal is passed and the OSD is standalone?
A: origin_lv won't be found as no LV will have the ceph.db_device or
    ceph.wal_device tag.

Q: For 'rm', what if the OSD doesn't have a cache?
A: origin_lv won't be found as no LV will have the ceph.lvmcache_lv tag.

Q: What if you run cache add twice on the same OSD?
A: This plugin makes sure the origin LV isn't cached by looking at the tag ceph.lvmcache_lv.

Q: What happens after hard reset?
A: 'ceph-volume cache info' shows that LVs are still cached as expected.

Q: What happens if the OSD is removed?
A: When removing the OSD's LV using lvremove, the cache LVs will be removed as
    well. No extra steps have to be taken. However, the user will still have
    to call pvremove on the partitions to make sure all labels have been wiped out.

Q: What happens if the OSD is zapped using ceph-volume zap?
A: Same answer as "What happens if the OSD is removed?".

Q: What happens if the origin drive fails?
A: The cache LVs still exist, and 'cache info' still shows them as being available.
    We won't be able to uncache as LVM will try to flush the cache (as it is in
    writeback mode), which will fail since the origin LV's device is gone. We're now
    in a situation where the origin LV exists, but its backing device doesn't.
    The procedure to fix this is to use the `pvs` command and locate the devices
    that are associated with the origin LV. We can then remove them using
    'pvremove /dev/<device or partition>'. The same can be done for the cache's
    partitions. This procedure should make the origin LV as well as the cache LVs
    disappear. We can also use dd if=/dev/zero of=/dev/<device or partition> to wipe them out
    before using 'pvremove'.
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


def _get_lv_info(lv):
    if not lv or not lv.name or not lv.vg_name:
        terminal.error('Missing information in LV')
        return None
    fields = 'lv_tags,lv_path,lv_name,vg_name,cache_mode,cache_policy,cache_settings'
    stdout, stderr, returncode = process.call(
        ['lvs', '--noheadings', '--readonly', '--separator=";"', '-a', '-o', fields],
        verbose_on_failure=False
    )
    items = api._output_parser(stdout, fields)
    for item in items:
        if item['lv_name'] == lv.name and item['vg_name'] == lv.vg_name:
            return item


def get_caching_mode(lv):
    item = _get_lv_info(lv)
    return item.get('cache_mode', '')


def print_cache_info(osdid=None):
    osd_id_width = 8
    rows, columns = get_terminal_size()
    column_width = min(int((columns - osd_id_width) / 5), 18)
    s = '{0:>{w}}'.format('OSD', w=osd_id_width)
    # TODO change 'partition' to something else
    s = s + '{0:>{w}}'.format('Partition', w=column_width)
    s = s + '{0:>{w}}'.format('Cache mode', w=column_width)
    s = s + '{0:>{w}}'.format('Cache policy', w=column_width)
    s = s + '{0:>{w}}'.format('Cache settings', w=column_width)
    print(s)
    for item in get_lvs_caching_info():
        # Need to test that lv_tags isn't empty because each OSD has two LVs
        # that will have cache_mode non-empty, its origin LV and the (new hidden) cache LV
        # The new hidden cache LV doesn't have tags, and we can't set them, so
        # we only want to print the info for the origin LV
        if item['cache_mode'] != "" and item['lv_tags'] != "":
            v = api.Volume(**item)
            if not osdid or (osdid and v.tags['ceph.osd_id'] == osdid):
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
    osd_id_width = 6
    column_width = 14
    h = '{0:>{w}}'.format('OSD', w=osd_id_width)
    h = h + '{0:>{w}}'.format('Read hits', w=column_width)
    h = h + '{0:>{w}}'.format('Read misses', w=column_width)
    h = h + '{0:>{w}}'.format('Rd hit rate', w=column_width)
    h = h + '{0:>{w}}'.format('Write hits', w=column_width)
    h = h + '{0:>{w}}'.format('Write misses', w=column_width)
    h = h + '{0:>{w}}'.format('Wr hit rate', w=column_width)
    h = h + '{0:>{w}}'.format('Total blocks', w=column_width)
    h = h + '{0:>{w}}'.format('Used blocks', w=column_width)
    h = h + '{0:>{w}}'.format('Dirty blocks', w=column_width)
    h = h + '{0:>{w}}'.format('Dirty %', w=column_width)
    if osdid:
        print(h)

    # TODO refactor this. Look at the mgr's iostat plugin.
    # TODO show the table's header again when it disappears
    while True:
        try:
            if not osdid:
                print(h)
            for item in get_lvs_caching_stats():
                # Need to test that lv_tags isn't empty because each OSD has two LVs
                # that will have cache_mode non-empty, its origin LV and the (new hidden) cache LV
                # The new hidden cache LV doesn't have tags, and we can't set them, so
                # we only want to print the info for the origin LV
                if item['cache_mode'] != "" and item['lv_tags'] != "":
                    v = api.Volume(**item)
                    if not osdid or (osdid and v.tags['ceph.osd_id'] == osdid):
                        rd_hits = int(item['cache_read_hits'])
                        rd_misses = int(item['cache_read_misses'])
                        wr_hits = int(item['cache_write_hits'])
                        wr_misses = int(item['cache_write_misses'])
                        total_blocks = int(item['cache_total_blocks'])
                        dirty_blocks = int(item['cache_dirty_blocks'])

                        if rd_misses + rd_hits > 0:
                            rd_hit_rate = str(int(rd_hits / (rd_hits + rd_misses) * 100)) + ' %'
                        else:
                            rd_hit_rate = '-'

                        if wr_misses + wr_hits > 0:
                            wr_hit_rate = str(int(wr_hits / (wr_hits + wr_misses) * 100)) + ' %'
                        else:
                            wr_hit_rate = '-'

                        if total_blocks > 0:
                            dirty_blocks_percentage = str(int(dirty_blocks / total_blocks * 100)) + ' %'
                        else:
                            dirty_blocks_percentage = '-'

                        s = '{0:>{w}}'.format(v.tags['ceph.osd_id'], w=osd_id_width)
                        s = s + '{0:>{w}}'.format(item['cache_read_hits'], w=column_width)
                        s = s + '{0:>{w}}'.format(item['cache_read_misses'], w=column_width)
                        s = s + '{0:>{w}}'.format(rd_hit_rate, w=column_width)
                        s = s + '{0:>{w}}'.format(item['cache_write_hits'], w=column_width)
                        s = s + '{0:>{w}}'.format(item['cache_write_misses'], w=column_width)
                        s = s + '{0:>{w}}'.format(wr_hit_rate, w=column_width)
                        s = s + '{0:>{w}}'.format(total_blocks, w=column_width)
                        s = s + '{0:>{w}}'.format(item['cache_used_blocks'], w=column_width)
                        s = s + '{0:>{w}}'.format(dirty_blocks, w=column_width)
                        s = s + '{0:>{w}}'.format(dirty_blocks_percentage, w=column_width)
                        print(s)
            if not osdid:
                print()
            time.sleep(1)
        except KeyboardInterrupt:
            print('Interrupted')
            return


def create_lvmcache_pool(data_lv, metadata_lv):
    process.run([
        'lvconvert',
        '--yes',
        '--poolmetadataspare', 'n',
        '--type', 'cache-pool',
        '--poolmetadata', metadata_lv.lv_path, data_lv.lv_path,
    ])


def create_lvmcache(cache_lv, origin_lv):
    process.run([
        'lvconvert',
        '--yes',
        '--type', 'cache',
        '--cachepool', cache_lv.lv_path, origin_lv.lv_path
    ])


def set_lvmcache_caching_mode(caching_mode, origin_lv):
    if caching_mode not in ['writeback', 'writethrough']:
        terminal.warning('Unknown caching mode: ' + caching_mode)
        return

    process.run([
        'lvchange',
        '--cachemode', caching_mode, origin_lv.lv_path
    ])


# partition sizes in GB
def _create_cache_lvs(vg_name, md_partition, data_partition, osdid, origin_lv):
    md_partition_size = disk.size_from_human_readable(disk.lsblk(md_partition)['SIZE'])
    data_partition_size = disk.size_from_human_readable(disk.lsblk(data_partition)['SIZE'])

    if md_partition_size < disk.Size(gb=2):
        raise Exception('Metadata partition is too small')
    if data_partition_size < disk.Size(gb=2):
        raise Exception('Data partition is too small')

    # ceph-volume creates volumes with extent size = 1GB
    # when a new lv is created, one extent needs to be used by LVM itself
    md_lv_size = md_partition_size - disk.Size(gb=1)
    data_lv_size = data_partition_size - disk.Size(gb=1)
    origin_type = origin_lv.tags.get('ceph.type', '')

    cache_md_lv = api.create_lv(
        name='cache_%s_md_osd.%s' % (origin_type, osdid),
        group=vg_name,
        size=str(md_lv_size._b) + 'B',
        tags={
            'ceph.osd_id': osdid,
            'ceph.type': 'lvmcache_metadata',
            'ceph.partition': md_partition,
            'ceph.cluster_fsid': origin_lv.tags.get('ceph.cluster_fsid', ''),
            'ceph.cluster_name': origin_lv.tags.get('ceph.cluster_name', '')
        },
        pv=md_partition
    )

    cache_data_lv = api.create_lv(
        name='cache_%s_osd.%s' % (origin_type, osdid),
        group=vg_name,
        size=str(data_lv_size._b) + 'B',
        tags={
            'ceph.osd_id': osdid,
            'ceph.type': 'lvmcache_data',
            'ceph.partition': data_partition,
            'ceph.cluster_fsid': origin_lv.tags.get('ceph.cluster_fsid', ''),
            'ceph.cluster_name': origin_lv.tags.get('ceph.cluster_name', '')
        },
        pv=data_partition
    )

    return cache_md_lv, cache_data_lv


def _create_lvmcache(vg_name, origin_lv, cache_metadata_lv, cache_data_lv):
    create_lvmcache_pool(cache_data_lv, cache_metadata_lv)
    create_lvmcache(cache_data_lv, origin_lv)

    # After this point, cache_metadata_lv and cache_data_lv are hidden, and
    # their names have changed from:
    #   cache_osd.5 to [cache_osd.5_cdata]
    #   cache_md_osd.5 to [cache_osd.5_cmeta]
    # these two partitions keep their tags. A new hidden partition has been created:
    #   [cache_osd.5]
    # We can't set its tags because "Operation not permitted on hidden LV [...]"

    cache_lv = api.get_lv(vg_name=vg_name, lv_name='['+cache_data_lv.name+']')
    if not cache_lv:
        raise Exception('Error: can\'t find newly created cache partition. ' +
            'Creation of lvmcache probably failed.')

    origin_lv.set_tag('ceph.lvmcache_lv', cache_data_lv.lv_name)
    set_lvmcache_caching_mode('writeback', origin_lv)

    return cache_lv


def add_lvmcache(origin_lv, md_partition, cache_data_partition, osdid):
    """
    High-level function to be called. Expects the user or orchestrator to have
    partitioned the disk used for caching.
    """
    vgname = origin_lv.vg_name
    vg = api.get_vg(vg_name=vgname)
    md_pv = api.get_pv(pv_name=md_partition)
    data_pv = api.get_pv(pv_name=cache_data_partition)
    md_pv_vgname = getattr(md_pv, 'vg_name', None)
    data_pv_vgname = getattr(data_pv, 'vg_name', None)

    # If md_partition or data_partition are already part of a different VG, fail
    if md_pv_vgname and md_pv_vgname != vg.name:
        raise Exception('Metadata PV ' + md_partition + ' is already part of a different VG.')
    if data_pv_vgname and data_pv_vgname != vg.name:
        raise Exception('Data PV ' + md_partition + ' is already part of a different VG.')
    if not md_pv_vgname:
        api.extend_vg(vg, md_partition)
    if not data_pv_vgname:
        api.extend_vg(vg, cache_data_partition)

    try:
        cache_md_lv, cache_data_lv = _create_cache_lvs(
            vg.name,
            md_partition,
            cache_data_partition,
            osdid,
            origin_lv
        )
    except Exception as e:
        terminal.error(str(e))
        terminal.error('Reverting changes...')
        api.reduce_vg(vg, [md_partition, cache_data_partition])
        return None

    try:
        cachelv = _create_lvmcache(vg.name, origin_lv, cache_md_lv,
            cache_data_lv)
    except Exception as e:
        terminal.error(str(e))
        terminal.error('Reverting changes...')
        api.remove_lv(vg.name + '/' + cache_md_lv.name)
        api.remove_lv(vg.name + '/' + cache_data_lv.name)
        api.reduce_vg(vg, [md_partition, cache_data_partition])
        return None

    return cachelv


def rm_lvmcache(origin_lv):
    if not origin_lv or not origin_lv.tags.get('ceph.lvmcache_lv', None):
        raise Exception('Can\'t find cache data LV.')
    vgname = origin_lv.vg_name
    vg = api.get_vg(vg_name=vgname)
    cache_lv_name = origin_lv.tags['ceph.lvmcache_lv']

    # get the partitions before removing the LVs
    data_lv_name = '[' + origin_lv.tags['ceph.lvmcache_lv'] + '_cdata]'
    meta_lv_name = '[' + origin_lv.tags['ceph.lvmcache_lv'] + '_cmeta]'
    data_lv = api.get_lv(lv_name=data_lv_name, vg_name=vgname)
    if not data_lv:
        raise Exception('Can\'t find cache data LV')
    meta_lv = api.get_lv(lv_name=meta_lv_name, vg_name=vgname)
    if not meta_lv:
        raise Exception('Can\'t find cache metadata LV')
    data_partition = data_lv.tags['ceph.partition']
    md_partition = meta_lv.tags['ceph.partition']

    api.remove_lv(vgname + '/' + cache_lv_name)
    api.reduce_vg(vg, [data_partition, md_partition])
    api.remove_pv(data_partition)
    api.remove_pv(md_partition)

    origin_lv.clear_tag('ceph.lvmcache_lv')


class LVMCache(object):

    help_menu = 'Manage LVM cache'
    _help = """
Manage lvmcache.

Add cache:

$> ceph-volume lvmcache add
    --cachemetadata <metadata partition>
    --cachedata <data partition>
    --osd-id <osd id>
    [--data|--db|--wal]

--data, --db and --wal indicate which partition to cache. Data is the default.

or:

$> ceph-volume lvmcache add
    --cachemetadata <metadata partition>
    --cachedata <data partition>
    --origin <OSD's LV in form vg/lv>

Remove cache:

$> ceph-volume lvmcache rm --osd-id <id>

Set cache mode:

$> ceph-volume lvmcache mode --osd-id <id>

Set cache mode:

$> ceph-volume lvmcache mode --set (writeback|writethrough) --osd-id <id>

Get info:

$> ceph-volume lvmcache info

Get cache usage numbers:

$> ceph-volume lvmcache stats [--osd-id <osd id>]

Dump LVs information:

$> ceph-volume lvmcache dump
    """
    name = 'lvmcache'

    def __init__(self, argv=sys.argv):
        self.argv = argv


    def help(self):
        return self._help


    def main(self, argv=None):
        main_args = self.argv[1:]
        parser = argparse.ArgumentParser(
            prog='lvmcache',
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
            help='OSD data partition in form VG/LV',
        )
        # TODO add an --osd-fsid argument
        parser.add_argument(
            '--osd-id',
            help='OSD id',
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
        parser.add_argument(
            '--set',
            help='Set value',
        )
        args = parser.parse_args(main_args)

        if len(self.argv) == 0:
            return parser.print_help()

        if self.argv[0] == 'dump':
            lvs = api.Volumes()
            for lv in lvs:
                pretty_lv(lv)
            return

        if args.osd_id and args.origin:
            raise Exception('Can\'t have --osdid and --origin')

        origin_lv = None

        if args.osd_id:
            lvs = api.Volumes()
            osdid = args.osd_id
            lvs.filter(lv_tags={'ceph.osd_id': osdid})

            for lv in lvs:
                # args.[data|db|wal] are for the 'add' command
                if args.data:
                    origin_lv = api.get_lv(lv_path=lv.tags.get('ceph.block_device', None))
                elif args.db:
                    origin_lv = api.get_lv(lv_path=lv.tags.get('ceph.db_device', None))
                elif args.wal:
                    origin_lv = api.get_lv(lv_path=lv.tags.get('ceph.wal_device', None))
                # this is the fallback for the commands other than 'add'
                else:
                    origin_lv = api.get_lv(lv_path=lv.tags.get('ceph.block_device', None))
                if origin_lv:
                    break
            if not origin_lv:
                raise Exception('Can\'t find origin LV for OSD ' + args.osd_id)
        elif args.origin:
            # origin is already the LV's name, so no need to look for
            # --data, --db or --wal flags.
            origin_lv = api.get_lv_from_argument(args.origin)
            if not origin_lv:
                raise Exception('Can\'t find origin LV ' + args.origin)
            osdid = origin_lv.tags.get('ceph.osd_id', None)
            if not osdid:
                raise Exception('Can\'t get OSD ID for existing LV ' +
                    args.origin)

        # TODO think about the interface to possibly use caching "backends"
        # other than lvm. Maybe bcache, iCAS, etc. Maybe the easiest solution is
        # to rename this plugin to 'lvmcache'.
        if self.argv[0] == 'add':
            if 'ceph.lvmcache_lv' in origin_lv.tags:
                raise Exception('Error: Origin LV is already cached')
            lvmcache = add_lvmcache(
                origin_lv,
                args.cachemetadata,
                args.cachedata,
                osdid)
            if lvmcache:
                terminal.success('Cache created susccessfully')
        elif self.argv[0] == 'rm':
            try:
                rm_lvmcache(origin_lv)
            except Exception as e:
                terminal.error(str(e))
                return
            else:
                terminal.success('Cache removed successfully')
        elif self.argv[0] == 'mode':
            if args.set:
                set_lvmcache_caching_mode(self.argv[2], origin_lv)
            else:
                print(get_caching_mode(origin_lv))
        elif self.argv[0] == 'info':
            print_cache_info(args.osd_id)
            return
        elif self.argv[0] == 'stats':
            print_cache_stats(args.osd_id)
            return
        elif self.argv[0] == 'help' or self.argv[0] == '-h' or self.argv[0] == '--help':
            parser.print_help()
        else:
            terminal.error('Unknown command ' + self.argv[0])


if __name__ == '__main__':
    try:
        main.LVMCache()
    except Exception as e:
        terminal.error(str(e))
