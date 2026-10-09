import argparse
import json
from textwrap import dedent

from ceph_volume import conf, terminal
from ceph_volume.util.disk import human_readable_size
from ceph_volume_zfs.util.disk import list_ceph_zpools


class List(object):

    help = 'List Ceph OSDs backed by ZFS'

    def __init__(self, argv):
        self.argv = argv

    def _matches(self, entry, wanted):
        """
        A filter argument can be an OSD id ('5'), a pool name
        ('ceph-osd-5'), or a zvol device path
        ('/dev/zvol/ceph-osd-5/osd-block-<fsid>'). Match against any
        of them so the user doesn't have to remember which form the
        tool wants.
        """
        if not wanted:
            return True
        if entry['pool'] == wanted:
            return True
        if entry['properties'].get('osd_id') == wanted:
            return True
        if entry['properties'].get('osd_fsid') == wanted:
            return True
        for zvol in entry['zvols']:
            if wanted in (zvol['device'], zvol['dataset'], zvol['name']):
                return True
        for vdev in entry.get('vdevs', []):
            # match /dev/ada0 or bare ada0
            if wanted == vdev['path'] or wanted == vdev['path'].replace('/dev/', ''):
                return True
        return False

    def plain_report(self, entries):
        if not entries:
            return 'No ZFS-backed Ceph OSDs found.\n'
        lines = []
        for entry in entries:
            props = entry['properties']
            lines.append('')
            lines.append('osd.{}  (pool: {})'.format(
                props.get('osd_id', 'unknown'), entry['pool']))
            lines.append('  osd_fsid: {}'.format(props.get('osd_fsid', 'unknown')))
            if props.get('cluster_name'):
                lines.append('  cluster: {}'.format(props.get('cluster_name')))
            if props.get('cluster_fsid'):
                lines.append('  cluster_fsid: {}'.format(props.get('cluster_fsid')))
            if entry.get('vdevs'):
                for vdev in entry['vdevs']:
                    state = vdev.get('state')
                    lines.append('  backing device: {}{}'.format(
                        vdev['path'],
                        '  [{}]'.format(state) if state else '',
                    ))
            else:
                lines.append('  backing device: unknown')
            if not entry['zvols']:
                lines.append('  zvols: none found (pool exists but has no volumes)')
            for zvol in entry['zvols']:
                zprops = zvol['properties']
                lines.append('  device: {}'.format(zvol['device']))
                try:
                    human = human_readable_size(int(zvol['size']))
                    lines.append('    size: {} bytes ({})'.format(zvol['size'], human))
                except (TypeError, ValueError):
                    lines.append('    size: {} bytes'.format(zvol['size']))
                if zprops.get('type'):
                    lines.append('    type: {}'.format(zprops.get('type')))
                if zprops.get('objectstore'):
                    lines.append('    objectstore: {}'.format(zprops.get('objectstore')))
                if zprops.get('crush_device_class'):
                    lines.append('    crush_device_class: {}'.format(
                        zprops.get('crush_device_class')))
                for link in ('db_device', 'wal_device', 'block_device'):
                    if zprops.get(link):
                        lines.append('    {}: {}'.format(link, zprops.get(link)))
        lines.append('')
        return '\n'.join(lines)

    def format_report(self, entries):
        fmt = getattr(conf, 'format', None)
        if fmt == 'json':
            print(json.dumps(entries))
        elif fmt == 'json-pretty' or getattr(conf, 'json', False):
            print(json.dumps(entries, indent=4, sort_keys=True))
        else:
            print(self.plain_report(entries))

    def main(self):
        sub_command_help = dedent("""
        List the ZFS-backed Ceph OSDs on this host.

        Only pools created by "ceph-volume zfs prepare" are shown --
        they're identified by the ceph:managed_by property that
        prepare sets. Unrelated zpools are never listed.

        An optional filter narrows the output; it can be an OSD id, an
        OSD fsid, a pool name, a zvol device path, or the backing
        physical device (e.g. /dev/ada0).

        Examples:
          ceph-volume zfs list
          ceph-volume zfs list 5
          ceph-volume zfs list ceph-osd-5
          ceph-volume zfs list /dev/ada0
          ceph-volume zfs -j list
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume zfs list',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )
        parser.add_argument(
            'filter',
            nargs='?',
            default=None,
            help='Optional OSD id, OSD fsid, pool name, or zvol device path',
        )
        self.args = parser.parse_args(self.argv)

        entries = [e for e in list_ceph_zpools() if self._matches(e, self.args.filter)]
        self.format_report(entries)

