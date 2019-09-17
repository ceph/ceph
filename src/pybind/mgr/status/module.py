
"""
High level status display commands
"""

from sys import maxint
from collections import defaultdict
from prettytable import PrettyTable
import errno
import fnmatch
import mgr_util
import prettytable
import six
import json

from mgr_module import MgrModule


class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "fs status "
                   "name=fs,type=CephString,req=false",
            "desc": "Show the status of a CephFS filesystem",
            "perm": "r"
        },
        {
            "cmd": "osd status "
                   "name=bucket,type=CephString,req=false",
            "desc": "Show the status of OSDs within a bucket, or all",
            "perm": "r"
        },
        {
            "cmd": "pg osd-df-by-pool "
                   "name=poolstr,type=CephString, ",
            "desc": "Show pg distribution by pool",
            "perm": "r"
        },
    ]

        
    def get_latest(self, daemon_type, daemon_name, stat):
        data = self.get_counter(daemon_type, daemon_name, stat)[stat]
        #self.log.error("get_latest {0} data={1}".format(stat, data))
        if data:
            return data[-1][1]
        else:
            return 0

    def get_rate(self, daemon_type, daemon_name, stat):
        data = self.get_counter(daemon_type, daemon_name, stat)[stat]

        #self.log.error("get_latest {0} data={1}".format(stat, data))
        if data and len(data) > 1 and data[-1][0] != data[-2][0]:
            return (data[-1][1] - data[-2][1]) / float(data[-1][0] - data[-2][0])
        else:
            return 0

    def handle_fs_status(self, cmd):
        output = ""

        fs_filter = cmd.get('fs', None)

        mds_versions = defaultdict(list)

        fsmap = self.get("fs_map")
        for filesystem in fsmap['filesystems']:
            if fs_filter and filesystem['mdsmap']['fs_name'] != fs_filter:
                continue

            rank_table = PrettyTable(
                ("Rank", "State", "MDS", "Activity", "dns", "inos"),
                hrules=prettytable.FRAME
            )

            mdsmap = filesystem['mdsmap']

            client_count = 0

            for rank in mdsmap["in"]:
                up = "mds_{0}".format(rank) in mdsmap["up"]
                if up:
                    gid = mdsmap['up']["mds_{0}".format(rank)]
                    info = mdsmap['info']['gid_{0}'.format(gid)]
                    dns = self.get_latest("mds", info['name'], "mds_mem.dn")
                    inos = self.get_latest("mds", info['name'], "mds_mem.ino")

                    if rank == 0:
                        client_count = self.get_latest("mds", info['name'],
                                                       "mds_sessions.session_count")
                    elif client_count == 0:
                        # In case rank 0 was down, look at another rank's
                        # sessionmap to get an indication of clients.
                        client_count = self.get_latest("mds", info['name'],
                                                       "mds_sessions.session_count")

                    laggy = "laggy_since" in info

                    state = info['state'].split(":")[1]
                    if laggy:
                        state += "(laggy)"
                    if state == "active" and not laggy:
                        c_state = mgr_util.colorize(state, mgr_util.GREEN)
                    else:
                        c_state = mgr_util.colorize(state, mgr_util.YELLOW)

                    # Populate based on context of state, e.g. client
                    # ops for an active daemon, replay progress, reconnect
                    # progress
                    activity = ""

                    if state == "active":
                        activity = "Reqs: " + mgr_util.format_dimless(
                            self.get_rate("mds", info['name'], "mds_server.handle_client_request"),
                            5
                        ) + "/s"

                    metadata = self.get_metadata('mds', info['name'])
                    mds_versions[metadata.get('ceph_version', "unknown")].append(info['name'])
                    rank_table.add_row([
                        mgr_util.bold(rank.__str__()), c_state, info['name'],
                        activity,
                        mgr_util.format_dimless(dns, 5),
                        mgr_util.format_dimless(inos, 5)
                    ])

                else:
                    rank_table.add_row([
                        rank, "failed", "", "", "", ""
                    ])

            # Find the standby replays
            for gid_str, daemon_info in six.iteritems(mdsmap['info']):
                if daemon_info['state'] != "up:standby-replay":
                    continue

                inos = self.get_latest("mds", daemon_info['name'], "mds_mem.ino")
                dns = self.get_latest("mds", daemon_info['name'], "mds_mem.dn")

                activity = "Evts: " + mgr_util.format_dimless(
                    self.get_rate("mds", daemon_info['name'], "mds_log.replayed"),
                    5
                ) + "/s"

                metadata = self.get_metadata('mds', daemon_info['name'])
                mds_versions[metadata.get('ceph_version', "unknown")].append(daemon_info['name'])

                rank_table.add_row([
                    "{0}-s".format(daemon_info['rank']), "standby-replay",
                    daemon_info['name'], activity,
                    mgr_util.format_dimless(dns, 5),
                    mgr_util.format_dimless(inos, 5)
                ])

            df = self.get("df")
            pool_stats = dict([(p['id'], p['stats']) for p in df['pools']])
            osdmap = self.get("osd_map")
            pools = dict([(p['pool'], p) for p in osdmap['pools']])
            metadata_pool_id = mdsmap['metadata_pool']
            data_pool_ids = mdsmap['data_pools']

            pools_table = PrettyTable(["Pool", "type", "used", "avail"])
            for pool_id in [metadata_pool_id] + data_pool_ids:
                pool_type = "metadata" if pool_id == metadata_pool_id else "data"
                stats = pool_stats[pool_id]
                pools_table.add_row([
                    pools[pool_id]['pool_name'], pool_type,
                    mgr_util.format_bytes(stats['bytes_used'], 5),
                    mgr_util.format_bytes(stats['max_avail'], 5)
                ])

            output += "{0} - {1} clients\n".format(
                mdsmap['fs_name'], client_count)
            output += "=" * len(mdsmap['fs_name']) + "\n"
            output += rank_table.get_string()
            output += "\n" + pools_table.get_string() + "\n"

        if not output and fs_filter is not None:
            return errno.EINVAL, "", "Invalid filesystem: " + fs_filter

        standby_table = PrettyTable(["Standby MDS"])
        for standby in fsmap['standbys']:
            metadata = self.get_metadata('mds', standby['name'])
            mds_versions[metadata.get('ceph_version', "unknown")].append(standby['name'])

            standby_table.add_row([standby['name']])

        output += "\n" + standby_table.get_string() + "\n"

        if len(mds_versions) == 1:
            output += "MDS version: {0}".format(list(mds_versions)[0])
        else:
            version_table = PrettyTable(["version", "daemons"])
            for version, daemons in six.iteritems(mds_versions):
                version_table.add_row([
                    version,
                    ", ".join(daemons)
                ])
            output += version_table.get_string() + "\n"

        return 0, output, ""

    def handle_osd_status(self, cmd):
        osd_table = PrettyTable(['id', 'host', 'used', 'avail', 'wr ops', 'wr data', 'rd ops', 'rd data', 'state'])
        osdmap = self.get("osd_map")

        filter_osds = set()
        bucket_filter = None
        if 'bucket' in cmd:
            self.log.debug("Filtering to bucket '{0}'".format(cmd['bucket']))
            bucket_filter = cmd['bucket']
            crush = self.get("osd_map_crush")
            found = False
            for bucket in crush['buckets']:
                if fnmatch.fnmatch(bucket['name'], bucket_filter):
                    found = True
                    filter_osds.update([i['id'] for i in bucket['items']])

            if not found:
                msg = "Bucket '{0}' not found".format(bucket_filter)
                return errno.ENOENT, msg, ""

        # Build dict of OSD ID to stats
        osd_stats = dict([(o['osd'], o) for o in self.get("osd_stats")['osd_stats']])

        for osd in osdmap['osds']:
            osd_id = osd['osd']
            if bucket_filter and osd_id not in filter_osds:
                continue

            hostname = ""
            kb_used = 0
            kb_avail = 0

            if osd_id in osd_stats:
                metadata = self.get_metadata('osd', "%s" % osd_id)
                stats = osd_stats[osd_id]
                hostname = metadata['hostname']
                kb_used = stats['kb_used'] * 1024
                kb_avail = stats['kb_avail'] * 1024

            osd_table.add_row([osd_id, hostname,
                               mgr_util.format_bytes(kb_used, 5),
                               mgr_util.format_bytes(kb_avail, 5),
                               mgr_util.format_dimless(self.get_rate("osd", osd_id.__str__(), "osd.op_w") +
                               self.get_rate("osd", osd_id.__str__(), "osd.op_rw"), 5),
                               mgr_util.format_bytes(self.get_rate("osd", osd_id.__str__(), "osd.op_in_bytes"), 5),
                               mgr_util.format_dimless(self.get_rate("osd", osd_id.__str__(), "osd.op_r"), 5),
                               mgr_util.format_bytes(self.get_rate("osd", osd_id.__str__(), "osd.op_out_bytes"), 5),
                               ','.join(osd['state']),
                               ])

        return 0, osd_table.get_string(), ""

    def handle_osd_df_by_pool(self, cmd):
        pool = cmd['poolstr']
        osdmap = self.get("osd_map")
        pools = osdmap.get('pools',[])
        poolid = None
        for p in pools:
            if p['pool_name'] == pool:
                poolid = p['pool']

        if poolid is None:
            return -errno.ENOENT, '', 'pool not found'

        pgs = self.get_osdmap().map_pool_pgs_up(poolid)

        osd2pgnums = {}
        for osds in pgs.values():
            for osd in osds:
                if osd in osd2pgnums:
                    osd2pgnums[int(osd)] += 1
                else:
                    osd2pgnums[int(osd)] = 1

        max = 0
        min = maxint
        sum = 0
        rows = []
        for osd, num in osd2pgnums.items():
            rows.append([osd, num])
            if num > max:
                max = num
            if num < min:
                min = num
            sum += num

        avg = float(sum) / len(osd2pgnums)

        jsformat = 'format' in cmd and cmd['format'] == 'json'
        if jsformat:
            tbl = {
                'osd_pg' : rows,
                'summary' : {
                    'MAX' : max,
                    'MIN' : min,
                    'AVG' : avg
                }
            }
            output = json.dumps(tbl)
        else:
            tbl = PrettyTable(['OSD', 'PGS'])
            for r in rows:
                tbl.add_row(r)

            sum_tbl = PrettyTable(['MAX', 'MIN', 'AVG'])
            sum_tbl.add_row([max, min, avg])

            output = tbl.get_string() + '\n' + sum_tbl.get_string()

        return 0, output, ''

    def handle_command(self, inbuf, cmd):
        self.log.error("handle_command")

        if cmd['prefix'] == "fs status":
            return self.handle_fs_status(cmd)
        elif cmd['prefix'] == "osd status":
            return self.handle_osd_status(cmd)
        elif cmd['prefix'] == "pg osd-df-by-pool":
            return self.handle_osd_df_by_pool(cmd)
        else:
            # mgr should respect our self.COMMANDS and not call us for
            # any prefix we don't advertise
            raise NotImplementedError(cmd['prefix'])
