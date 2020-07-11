# -*- coding: utf-8 -*-
from __future__ import absolute_import

from collections import defaultdict

import os

import cherrypy
import cephfs

from . import ApiController, ControllerDoc, RESTController, UiApiController
from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope
from ..services.cephfs import CephFS as CephFS_
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

    @RESTController.Resource('DELETE', path='/client/{client_id}')
    def evict(self, fs_id, client_id):
        fs_id = self.fs_id_to_int(fs_id)
        client_id = self.client_id_to_int(client_id)

        return self._evict(fs_id, client_id)

    @RESTController.Resource('GET')
    def mds_counters(self, fs_id, counters=None):
        fs_id = self.fs_id_to_int(fs_id)
        return self._mds_counters(fs_id, counters)

    def _mds_counters(self, fs_id, counters=None):
        """
        Result format: map of daemon name to map of counter to list of datapoints
        rtype: dict[str, dict[str, list]]
        """

        if counters is None:
            # Opinionated list of interesting performance counters for the GUI
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
                "mds.subtrees",
                "mds_mem.ino"
            ]

        result = {}  # type: dict
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

    @staticmethod
    def client_id_to_int(client_id):
        try:
            return int(client_id)
        except ValueError:
            raise DashboardException(code='invalid_cephfs_client_id',
                                     msg="Invalid cephfs client ID {}".format(client_id),
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

    def _append_mds_metadata(self, mds_versions, metadata_key):
        metadata = mgr.get_metadata('mds', metadata_key)
        if metadata is None:
            return
        mds_versions[metadata.get('ceph_version', 'unknown')].append(metadata_key)

    # pylint: disable=too-many-statements,too-many-branches
    def fs_status(self, fs_id):
        mds_versions = defaultdict(list)  # type: dict

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
                dns = mgr.get_latest("mds", info['name'], "mds_mem.dn")
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

                self._append_mds_metadata(mds_versions, info['name'])
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
            dns = mgr.get_latest("mds", daemon_info['name'], "mds_mem.dn")

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
            self._append_mds_metadata(mds_versions, standby['name'])
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

    def _evict(self, fs_id, client_id):
        clients = self._clients(fs_id)
        if not [c for c in clients['data'] if c['id'] == client_id]:
            raise cherrypy.HTTPError(404,
                                     "Client {0} does not exist in cephfs {1}".format(client_id,
                                                                                      fs_id))
        CephService.send_command('mds', 'client evict',
                                 srv_spec='{0}:0'.format(fs_id), id=client_id)

    @staticmethod
    def _cephfs_instance(fs_id):
        """
        :param fs_id: The filesystem identifier.
        :type fs_id: int | str
        :return: A instance of the CephFS class.
        """
        fs_name = CephFS_.fs_name_from_id(fs_id)
        if fs_name is None:
            raise cherrypy.HTTPError(404, "CephFS id {} not found".format(fs_id))
        return CephFS_(fs_name)

    @RESTController.Resource('GET')
    def get_root_directory(self, fs_id):
        """
        The root directory that can't be fetched using ls_dir (api).
        :param fs_id: The filesystem identifier.
        :return: The root directory
        :rtype: dict
        """
        try:
            return self._get_root_directory(self._cephfs_instance(fs_id))
        except (cephfs.PermissionError, cephfs.ObjectNotFound):
            return None

    def _get_root_directory(self, cfs):
        """
        The root directory that can't be fetched using ls_dir (api).
        It's used in ls_dir (ui-api) and in get_root_directory (api).
        :param cfs: CephFS service instance
        :type cfs: CephFS
        :return: The root directory
        :rtype: dict
        """
        return cfs.get_directory(os.sep.encode())

    @RESTController.Resource('GET')
    def ls_dir(self, fs_id, path=None, depth=1):
        """
        List directories of specified path.
        :param fs_id: The filesystem identifier.
        :param path: The path where to start listing the directory content.
          Defaults to '/' if not set.
        :type path: str | bytes
        :param depth: The number of steps to go down the directory tree.
        :type depth: int | str
        :return: The names of the directories below the specified path.
        :rtype: list
        """
        path = self._set_ls_dir_path(path)
        try:
            cfs = self._cephfs_instance(fs_id)
            paths = cfs.ls_dir(path, depth)
        except (cephfs.PermissionError, cephfs.ObjectNotFound):
            paths = []
        return paths

    def _set_ls_dir_path(self, path):
        """
        Transforms input path parameter of ls_dir methods (api and ui-api).
        :param path: The path where to start listing the directory content.
          Defaults to '/' if not set.
        :type path: str | bytes
        :return: Normalized path or root path
        :return: str
        """
        if path is None:
            path = os.sep
        else:
            path = os.path.normpath(path)
        return path

    @RESTController.Resource('POST')
    def mk_dirs(self, fs_id, path):
        """
        Create a directory.
        :param fs_id: The filesystem identifier.
        :param path: The path of the directory.
        """
        cfs = self._cephfs_instance(fs_id)
        cfs.mk_dirs(path)

    @RESTController.Resource('POST')
    def rm_dir(self, fs_id, path):
        """
        Remove a directory.
        :param fs_id: The filesystem identifier.
        :param path: The path of the directory.
        """
        cfs = self._cephfs_instance(fs_id)
        cfs.rm_dir(path)

    @RESTController.Resource('POST')
    def mk_snapshot(self, fs_id, path, name=None):
        """
        Create a snapshot.
        :param fs_id: The filesystem identifier.
        :param path: The path of the directory.
        :param name: The name of the snapshot. If not specified,
            a name using the current time in RFC3339 UTC format
            will be generated.
        :return: The name of the snapshot.
        :rtype: str
        """
        cfs = self._cephfs_instance(fs_id)
        return cfs.mk_snapshot(path, name)

    @RESTController.Resource('POST')
    def rm_snapshot(self, fs_id, path, name):
        """
        Remove a snapshot.
        :param fs_id: The filesystem identifier.
        :param path: The path of the directory.
        :param name: The name of the snapshot.
        """
        cfs = self._cephfs_instance(fs_id)
        cfs.rm_snapshot(path, name)

    @RESTController.Resource('GET')
    def get_quotas(self, fs_id, path):
        """
        Get the quotas of the specified path.
        :param fs_id: The filesystem identifier.
        :param path: The path of the directory/file.
        :return: Returns a dictionary containing 'max_bytes'
            and 'max_files'.
        :rtype: dict
        """
        cfs = self._cephfs_instance(fs_id)
        return cfs.get_quotas(path)

    @RESTController.Resource('POST')
    def set_quotas(self, fs_id, path, max_bytes=None, max_files=None):
        """
        Set the quotas of the specified path.
        :param fs_id: The filesystem identifier.
        :param path: The path of the directory/file.
        :param max_bytes: The byte limit.
        :param max_files: The file limit.
        """
        cfs = self._cephfs_instance(fs_id)
        return cfs.set_quotas(path, max_bytes, max_files)


class CephFSClients(object):
    def __init__(self, module_inst, fscid):
        self._module = module_inst
        self.fscid = fscid

    @ViewCache()
    def get(self):
        return CephService.send_command('mds', 'session ls', srv_spec='{0}:0'.format(self.fscid))


@UiApiController('/cephfs', Scope.CEPHFS)
@ControllerDoc("Dashboard UI helper function; not part of the public API", "CephFSUi")
class CephFsUi(CephFS):
    RESOURCE_ID = 'fs_id'

    @RESTController.Resource('GET')
    def tabs(self, fs_id):
        data = {}
        fs_id = self.fs_id_to_int(fs_id)

        # Needed for detail tab
        fs_status = self.fs_status(fs_id)
        for pool in fs_status['cephfs']['pools']:
            pool['size'] = pool['used'] + pool['avail']
        data['pools'] = fs_status['cephfs']['pools']
        data['ranks'] = fs_status['cephfs']['ranks']
        data['name'] = fs_status['cephfs']['name']
        data['standbys'] = ', '.join([x['name'] for x in fs_status['standbys']])
        counters = self._mds_counters(fs_id)
        for k, v in counters.items():
            v['name'] = k
        data['mds_counters'] = counters

        # Needed for client tab
        data['clients'] = self._clients(fs_id)

        return data

    @RESTController.Resource('GET')
    def ls_dir(self, fs_id, path=None, depth=1):
        """
        The difference to the API version is that the root directory will be send when listing
        the root directory.
        To only do one request this endpoint was created.
        :param fs_id: The filesystem identifier.
        :type fs_id: int | str
        :param path: The path where to start listing the directory content.
          Defaults to '/' if not set.
        :type path: str | bytes
        :param depth: The number of steps to go down the directory tree.
        :type depth: int | str
        :return: The names of the directories below the specified path.
        :rtype: list
        """
        path = self._set_ls_dir_path(path)
        try:
            cfs = self._cephfs_instance(fs_id)
            paths = cfs.ls_dir(path, depth)
            if path == os.sep:
                paths = [self._get_root_directory(cfs)] + paths
        except (cephfs.PermissionError, cephfs.ObjectNotFound):
            paths = []
        return paths
