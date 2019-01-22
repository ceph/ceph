# -*- coding: utf-8 -*-
from __future__ import absolute_import

from collections import defaultdict

import cherrypy

from . import ApiController, RESTController
from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope
from ..services.ceph_service import CephService
from ..tools import ViewCache


@ApiController('/cephfs', Scope.CEPHFS)
class CephFS(RESTController):
    def __init__(self):
        super(CephFS, self).__init__()

        # Stateful instances of CephFSClients, hold cached results.  Key to
        # dict is FSCID
        self.cephfs_clients = {}

    def list(self):
        fsmap = mgr.get("fs_map")
        return fsmap['filesystems']

    def get(self, fs_id):
        fs_id = self.fs_id_to_int(fs_id)

        return self.fs_status(fs_id)

    @RESTController.Resource('GET')
    def clients(self, fs_id):
        fs_id = self.fs_id_to_int(fs_id)

        return self._clients(fs_id)

    @RESTController.Resource('GET')
    def mds_counters(self, fs_id):
        """
        Result format: map of daemon name to map of counter to list of datapoints
        rtype: dict[str, dict[str, list]]
        """

        # Opinionated list of interesting performance counters for the GUI --
        # if you need something else just add it.  See how simple life is
        # when you don't have to write general purpose APIs?
        counters = [
            "mds_server.handle_client_request",
            "mds_log.ev",
            "mds_cache.num_strays",
            "mds.exported",
            "mds.exported_inodes",
            "mds.imported",
            "mds.imported_inodes",
            "mds.inodes",
            "mds.caps",
            "mds.subtrees"
        ]

        fs_id = self.fs_id_to_int(fs_id)

        result = {}
        mds_names = self._get_mds_names(fs_id)

        for mds_name in mds_names:
            result[mds_name] = {}
            for counter in counters:
                data = mgr.get_counter("mds", mds_name, counter)
                if data is not None:
                    result[mds_name][counter] = data[counter]
                else:
                    result[mds_name][counter] = []

        return dict(result)

    @staticmethod
    def fs_id_to_int(fs_id):
        try:
            return int(fs_id)
        except ValueError:
            raise DashboardException(code='invalid_cephfs_id',
                                     msg="Invalid cephfs ID {}".format(fs_id),
                                     component='cephfs')

    def _get_mds_names(self, filesystem_id=None):
        names = []

        fsmap = mgr.get("fs_map")
        for fs in fsmap['filesystems']:
            if filesystem_id is not None and fs['id'] != filesystem_id:
                continue
            names.extend([info['name']
                          for _, info in fs['mdsmap']['info'].items()])

        if filesystem_id is None:
            names.extend(info['name'] for info in fsmap['standbys'])

        return names

    # pylint: disable=too-many-statements,too-many-branches
    def fs_status(self, fs_id):
        mds_versions = defaultdict(list)

        fsmap = mgr.get("fs_map")
        filesystem = None
        for fs in fsmap['filesystems']:
            if fs['id'] == fs_id:
                filesystem = fs
                break

        if filesystem is None:
            raise cherrypy.HTTPError(404,
                                     "CephFS id {0} not found".format(fs_id))

        rank_table = []

        mdsmap = filesystem['mdsmap']

        client_count = 0

        for rank in mdsmap["in"]:
            up = "mds_{0}".format(rank) in mdsmap["up"]
            if up:
                gid = mdsmap['up']["mds_{0}".format(rank)]
                info = mdsmap['info']['gid_{0}'.format(gid)]
                dns = mgr.get_latest("mds", info['name'], "mds.inodes")
                inos = mgr.get_latest("mds", info['name'], "mds_mem.ino")

                if rank == 0:
                    client_count = mgr.get_latest("mds", info['name'],
                                                  "mds_sessions.session_count")
                elif client_count == 0:
                    # In case rank 0 was down, look at another rank's
                    # sessionmap to get an indication of clients.
                    client_count = mgr.get_latest("mds", info['name'],
                                                  "mds_sessions.session_count")

                laggy = "laggy_since" in info

                state = info['state'].split(":")[1]
                if laggy:
                    state += "(laggy)"

                # Populate based on context of state, e.g. client
                # ops for an active daemon, replay progress, reconnect
                # progress
                if state == "active":
                    activity = CephService.get_rate("mds",
                                                    info['name'],
                                                    "mds_server.handle_client_request")
                else:
                    activity = 0.0

                metadata = mgr.get_metadata('mds', info['name'])
                mds_versions[metadata.get('ceph_version', 'unknown')].append(
                    info['name'])
                rank_table.append(
                    {
                        "rank": rank,
                        "state": state,
                        "mds": info['name'],
                        "activity": activity,
                        "dns": dns,
                        "inos": inos
                    }
                )

            else:
                rank_table.append(
                    {
                        "rank": rank,
                        "state": "failed",
                        "mds": "",
                        "activity": 0.0,
                        "dns": 0,
                        "inos": 0
                    }
                )

        # Find the standby replays
        # pylint: disable=unused-variable
        for gid_str, daemon_info in mdsmap['info'].items():
            if daemon_info['state'] != "up:standby-replay":
                continue

            inos = mgr.get_latest("mds", daemon_info['name'], "mds_mem.ino")
            dns = mgr.get_latest("mds", daemon_info['name'], "mds.inodes")

            activity = CephService.get_rate(
                "mds", daemon_info['name'], "mds_log.replay")

            rank_table.append(
                {
                    "rank": "{0}-s".format(daemon_info['rank']),
                    "state": "standby-replay",
                    "mds": daemon_info['name'],
                    "activity": activity,
                    "dns": dns,
                    "inos": inos
                }
            )

        df = mgr.get("df")
        pool_stats = {p['id']: p['stats'] for p in df['pools']}
        osdmap = mgr.get("osd_map")
        pools = {p['pool']: p for p in osdmap['pools']}
        metadata_pool_id = mdsmap['metadata_pool']
        data_pool_ids = mdsmap['data_pools']

        pools_table = []
        for pool_id in [metadata_pool_id] + data_pool_ids:
            pool_type = "metadata" if pool_id == metadata_pool_id else "data"
            stats = pool_stats[pool_id]
            pools_table.append({
                "pool": pools[pool_id]['pool_name'],
                "type": pool_type,
                "used": stats['bytes_used'],
                "avail": stats['max_avail']
            })

        standby_table = []
        for standby in fsmap['standbys']:
            metadata = mgr.get_metadata('mds', standby['name'])
            mds_versions[metadata.get('ceph_version', 'unknown')].append(
                standby['name'])

            standby_table.append({
                'name': standby['name']
            })

        return {
            "cephfs": {
                "id": fs_id,
                "name": mdsmap['fs_name'],
                "client_count": client_count,
                "ranks": rank_table,
                "pools": pools_table
            },
            "standbys": standby_table,
            "versions": mds_versions
        }

    def _clients(self, fs_id):
        cephfs_clients = self.cephfs_clients.get(fs_id, None)
        if cephfs_clients is None:
            cephfs_clients = CephFSClients(mgr, fs_id)
            self.cephfs_clients[fs_id] = cephfs_clients

        try:
            status, clients = cephfs_clients.get()
        except AttributeError:
            raise cherrypy.HTTPError(404,
                                     "No cephfs with id {0}".format(fs_id))

        if clients is None:
            raise cherrypy.HTTPError(404,
                                     "No cephfs with id {0}".format(fs_id))

        # Decorate the metadata with some fields that will be
        # indepdendent of whether it's a kernel or userspace
        # client, so that the javascript doesn't have to grok that.
        for client in clients:
            if "ceph_version" in client['client_metadata']:
                client['type'] = "userspace"
                client['version'] = client['client_metadata']['ceph_version']
                client['hostname'] = client['client_metadata']['hostname']
            elif "kernel_version" in client['client_metadata']:
                client['type'] = "kernel"
                client['version'] = client['client_metadata']['kernel_version']
                client['hostname'] = client['client_metadata']['hostname']
            else:
                client['type'] = "unknown"
                client['version'] = ""
                client['hostname'] = ""

        return {
            'status': status,
            'data': clients
        }


class CephFSClients(object):
    def __init__(self, module_inst, fscid):
        self._module = module_inst
        self.fscid = fscid

    @ViewCache()
    def get(self):
        return CephService.send_command('mds', 'session ls', srv_spec='{0}:0'.format(self.fscid))
