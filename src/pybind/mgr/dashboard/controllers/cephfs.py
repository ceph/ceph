# -*- coding: utf-8 -*-
# pylint: disable=too-many-lines
import errno
import json
import os
from collections import defaultdict
from typing import Any, Dict, List

import cephfs
import cherrypy

from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope
from ..services.ceph_service import CephService
from ..services.cephfs import CephFS as CephFS_
from ..services.exception import handle_cephfs_error
from ..tools import ViewCache, str_to_bool
from . import APIDoc, APIRouter, DeletePermission, Endpoint, EndpointDoc, \
    RESTController, UIRouter, UpdatePermission, allow_empty_body

GET_QUOTAS_SCHEMA = {
    'max_bytes': (int, ''),
    'max_files': (int, '')
}
GET_STATFS_SCHEMA = {
    'bytes': (int, ''),
    'files': (int, ''),
    'subdirs': (int, '')
}


# pylint: disable=R0904
@APIRouter('/cephfs', Scope.CEPHFS)
@APIDoc("Cephfs Management API", "Cephfs")
class CephFS(RESTController):
    def __init__(self):  # pragma: no cover
        super().__init__()

        # Stateful instances of CephFSClients, hold cached results.  Key to
        # dict is FSCID
        self.cephfs_clients = {}

    def list(self):
        fsmap = mgr.get("fs_map")
        return fsmap['filesystems']

    def create(self, name: str, service_spec: Dict[str, Any]):
        service_spec_str = '1 '
        if 'labels' in service_spec['placement']:
            for label in service_spec['placement']['labels']:
                service_spec_str += f'label:{label},'
            service_spec_str = service_spec_str[:-1]
        if 'hosts' in service_spec['placement']:
            for host in service_spec['placement']['hosts']:
                service_spec_str += f'{host} '
            service_spec_str = service_spec_str[:-1]

        error_code, _, err = mgr.remote('volumes', '_cmd_fs_volume_create', None,
                                        {'name': name, 'placement': service_spec_str})
        if error_code != 0:
            raise RuntimeError(
                f'Error creating volume {name} with placement {str(service_spec)}: {err}')
        return f'Volume {name} created successfully'

    @EndpointDoc("Remove CephFS Volume",
                 parameters={
                     'name': (str, 'File System Name'),
                 })
    @allow_empty_body
    @Endpoint('DELETE')
    @DeletePermission
    def remove(self, name):
        error_code, _, err = mgr.remote('volumes', '_cmd_fs_volume_rm', None,
                                        {'vol_name': name,
                                         'yes-i-really-mean-it': "--yes-i-really-mean-it"})
        if error_code != 0:
            raise DashboardException(
                msg=f'Error deleting volume {name}: {err}',
                component='cephfs')
        return f'Volume {name} removed successfully'

    @EndpointDoc("Rename CephFS Volume",
                 parameters={
                     'name': (str, 'Existing FS Name'),
                     'new_name': (str, 'New FS Name'),
                 })
    @allow_empty_body
    @UpdatePermission
    @Endpoint('PUT')
    def rename(self, name: str, new_name: str):
        error_code, _, err = mgr.remote('volumes', '_cmd_fs_volume_rename', None,
                                        {'vol_name': name, 'new_vol_name': new_name,
                                         'yes_i_really_mean_it': True})
        if error_code != 0:
            raise DashboardException(
                msg=f'Error renaming volume {name} to {new_name}: {err}',
                component='cephfs')
        return f'Volume {name} renamed successfully to {new_name}'

    @UpdatePermission
    @Endpoint('PUT')
    @EndpointDoc("Set Ceph authentication capabilities for the specified user ID in the given path",
                 parameters={
                     'fs_name': (str, 'File system name'),
                     'client_id': (str, 'Cephx user ID'),
                     'caps': (str, 'Path and given capabilities'),
                     'root_squash': (str, 'File System Identifier'),

                 })
    def auth(self, fs_name: str, client_id: int, caps: List[str], root_squash: bool):
        if root_squash:
            caps.insert(2, 'root_squash')
        error_code, _, err = mgr.mon_command({'prefix': 'fs authorize',
                                              'filesystem': fs_name,
                                              'entity': client_id,
                                              'caps': caps})
        if error_code != 0:
            raise DashboardException(
                msg=f'Error setting authorization for {client_id} with {caps}: {err}',
                component='cephfs')
        return f'Updated {client_id} authorization successfully'

    def get(self, fs_id):
        fs_id = self.fs_id_to_int(fs_id)
        return self.fs_status(fs_id)

    @RESTController.Resource('GET')
    def clients(self, fs_id, **kwargs):
        flag = kwargs.pop('suppress_client_ls_errors', 'True')
        if flag not in ('True', 'False'):
            raise DashboardException(msg='suppress_client_ls_errors value '
                                         'needs to be either True or False '
                                         f'but provided "{flag}"',
                                     component='cephfs')

        fs_id = self.fs_id_to_int(fs_id)

        return self._clients(fs_id, suppress_client_ls_errors=flag)

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

        result: dict = {}
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

    def _find_standby_replays(self, mdsmap_info, rank_table):
        # pylint: disable=unused-variable
        for gid_str, daemon_info in mdsmap_info.items():
            if daemon_info['state'] != "up:standby-replay":
                continue

            inos = mgr.get_latest("mds", daemon_info['name'], "mds_mem.ino")
            dns = mgr.get_latest("mds", daemon_info['name'], "mds_mem.dn")
            dirs = mgr.get_latest("mds", daemon_info['name'], "mds_mem.dir")
            caps = mgr.get_latest("mds", daemon_info['name'], "mds_mem.cap")

            activity = CephService.get_rate(
                "mds", daemon_info['name'], "mds_log.replay")

            rank_table.append(
                {
                    "rank": "{0}-s".format(daemon_info['rank']),
                    "state": "standby-replay",
                    "mds": daemon_info['name'],
                    "activity": activity,
                    "dns": dns,
                    "inos": inos,
                    "dirs": dirs,
                    "caps": caps
                }
            )

    def get_standby_table(self, standbys, mds_versions):
        standby_table = []
        for standby in standbys:
            self._append_mds_metadata(mds_versions, standby['name'])
            standby_table.append({
                'name': standby['name']
            })
        return standby_table

    # pylint: disable=too-many-statements,too-many-branches
    def fs_status(self, fs_id):
        mds_versions: dict = defaultdict(list)

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
                dirs = mgr.get_latest("mds", info['name'], "mds_mem.dir")
                caps = mgr.get_latest("mds", info['name'], "mds_mem.cap")

                # In case rank 0 was down, look at another rank's
                # sessionmap to get an indication of clients.
                if rank == 0 or client_count == 0:
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
                    activity = 0.0  # pragma: no cover

                self._append_mds_metadata(mds_versions, info['name'])
                rank_table.append(
                    {
                        "rank": rank,
                        "state": state,
                        "mds": info['name'],
                        "activity": activity,
                        "dns": dns,
                        "inos": inos,
                        "dirs": dirs,
                        "caps": caps
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
                        "inos": 0,
                        "dirs": 0,
                        "caps": 0
                    }
                )

        self._find_standby_replays(mdsmap['info'], rank_table)

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
                "used": stats['stored'],
                "avail": stats['max_avail']
            })

        standby_table = self.get_standby_table(fsmap['standbys'], mds_versions)

        flags = mdsmap['flags_state']

        return {
            "cephfs": {
                "id": fs_id,
                "name": mdsmap['fs_name'],
                "client_count": client_count,
                "ranks": rank_table,
                "pools": pools_table,
                "flags": flags,
            },
            "standbys": standby_table,
            "versions": mds_versions
        }

    def _clients(self, fs_id, **kwargs):
        suppress_get_errors = kwargs.pop('suppress_client_ls_errors', 'True')
        cephfs_clients = self.cephfs_clients.get(fs_id, None)
        if cephfs_clients is None:
            cephfs_clients = CephFSClients(mgr, fs_id)
            self.cephfs_clients[fs_id] = cephfs_clients

        try:
            status, clients = cephfs_clients.get(suppress_get_errors)
        except AttributeError:
            raise cherrypy.HTTPError(404,
                                     "No cephfs with id {0}".format(fs_id))
        except RuntimeError:
            raise cherrypy.HTTPError(500,
                                     f"Could not fetch client(s), maybe there "
                                     f"is no active MDS on CephFS {fs_id} or "
                                     "the FS is in failed state.")

        if clients is None:
            raise cherrypy.HTTPError(404,
                                     "No cephfs with id {0}".format(fs_id))

        # Decorate the metadata with some fields that will be
        # independent of whether it's a kernel or userspace
        # client, so that the javascript doesn't have to grok that.
        for client in clients:
            if "ceph_version" in client['client_metadata']:  # pragma: no cover - no complexity
                client['type'] = "userspace"
                client['version'] = client['client_metadata']['ceph_version']
                client['hostname'] = client['client_metadata']['hostname']
                client['root'] = client['client_metadata']['root']
            elif "kernel_version" in client['client_metadata']:  # pragma: no cover - no complexity
                client['type'] = "kernel"
                client['version'] = client['client_metadata']['kernel_version']
                client['hostname'] = client['client_metadata']['hostname']
                client['root'] = client['client_metadata']['root']
            else:  # pragma: no cover - no complexity there
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
        filters = [f'id={client_id}']
        CephService.send_command('mds', 'client evict',
                                 srv_spec='{0}:0'.format(fs_id), filters=filters)

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
        except (cephfs.PermissionError, cephfs.ObjectNotFound):  # pragma: no cover
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

    @handle_cephfs_error()
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
        except (cephfs.PermissionError, cephfs.ObjectNotFound):  # pragma: no cover
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

    @RESTController.Resource('POST', path='/tree')
    @allow_empty_body
    def mk_tree(self, fs_id, path):
        """
        Create a directory.
        :param fs_id: The filesystem identifier.
        :param path: The path of the directory.
        """
        cfs = self._cephfs_instance(fs_id)
        cfs.mk_dirs(path)

    @RESTController.Resource('DELETE', path='/tree')
    def rm_tree(self, fs_id, path):
        """
        Remove a directory.
        :param fs_id: The filesystem identifier.
        :param path: The path of the directory.
        """
        cfs = self._cephfs_instance(fs_id)
        cfs.rm_dir(path)

    @RESTController.Resource('PUT', path='/quota')
    @allow_empty_body
    def quota(self, fs_id, path, max_bytes=None, max_files=None):
        """
        Set the quotas of the specified path.
        :param fs_id: The filesystem identifier.
        :param path: The path of the directory/file.
        :param max_bytes: The byte limit.
        :param max_files: The file limit.
        """
        cfs = self._cephfs_instance(fs_id)
        return cfs.set_quotas(path, max_bytes, max_files)

    @RESTController.Resource('GET', path='/quota')
    @EndpointDoc("Get Cephfs Quotas of the specified path",
                 parameters={
                     'fs_id': (str, 'File System Identifier'),
                     'path': (str, 'File System Path'),
                 },
                 responses={200: GET_QUOTAS_SCHEMA})
    def get_quota(self, fs_id, path):
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

    @RESTController.Resource('POST', path='/write_to_file')
    @allow_empty_body
    def write_to_file(self, fs_id, path, buf) -> None:
        """
        Write some data to the specified path.
        :param fs_id: The filesystem identifier.
        :param path: The path of the file to write.
        :param buf: The str to write to the buf.
        """
        cfs = self._cephfs_instance(fs_id)
        cfs.write_to_file(path, buf)

    @RESTController.Resource('DELETE', path='/unlink')
    def unlink(self, fs_id, path) -> None:
        """
        Removes a file, link, or symbolic link.
        :param fs_id: The filesystem identifier.
        :param path: The path of the file or link to unlink.
        """
        cfs = self._cephfs_instance(fs_id)
        cfs.unlink(path)

    @RESTController.Resource('GET', path='/statfs')
    @EndpointDoc("Get Cephfs statfs of the specified path",
                 parameters={
                     'fs_id': (str, 'File System Identifier'),
                     'path': (str, 'File System Path'),
                 },
                 responses={200: GET_STATFS_SCHEMA})
    def statfs(self, fs_id, path) -> dict:
        """
        Get the statfs of the specified path.
        :param fs_id: The filesystem identifier.
        :param path: The path of the directory/file.
        :return: Returns a dictionary containing 'bytes',
        'files' and 'subdirs'.
        :rtype: dict
        """
        cfs = self._cephfs_instance(fs_id)
        return cfs.statfs(path)

    @RESTController.Resource('POST', path='/snapshot')
    @allow_empty_body
    def snapshot(self, fs_id, path, name=None):
        """
        Create a snapshot.
        :param fs_id: The filesystem identifier.
        :param path: The path of the directory.
        :param name: The name of the snapshot. If not specified, a name using the
        current time in RFC3339 UTC format will be generated.
        :return: The name of the snapshot.
        :rtype: str
        """
        cfs = self._cephfs_instance(fs_id)
        list_snaps = cfs.ls_snapshots(path)
        for snap in list_snaps:
            if name == snap['name']:
                raise DashboardException(code='Snapshot name already in use',
                                         msg='Snapshot name {} is already in use.'
                                         'Please use another name'.format(name),
                                         component='cephfs')

        return cfs.mk_snapshot(path, name)

    @RESTController.Resource('DELETE', path='/snapshot')
    def rm_snapshot(self, fs_id, path, name):
        """
        Remove a snapshot.
        :param fs_id: The filesystem identifier.
        :param path: The path of the directory.
        :param name: The name of the snapshot.
        """
        cfs = self._cephfs_instance(fs_id)
        cfs.rm_snapshot(path, name)

    @RESTController.Resource('PUT', path='/rename-path')
    def rename_path(self, fs_id, src_path, dst_path) -> None:
        """
        Rename a file or directory.
        :param fs_id: The filesystem identifier.
        :param src_path: The path to the existing file or directory.
        :param dst_path: The new name of the file or directory.
        """
        cfs = self._cephfs_instance(fs_id)
        cfs.rename_path(src_path, dst_path)


class CephFSClients(object):
    def __init__(self, module_inst, fscid):
        self._module = module_inst
        self.fscid = fscid

    @ViewCache()
    def get(self, suppress_errors='True'):
        try:
            ret = CephService.send_command('mds', 'session ls', srv_spec='{0}:0'.format(self.fscid))
        except RuntimeError:
            if suppress_errors == 'True':
                ret = []
            else:
                raise
        return ret


@UIRouter('/cephfs', Scope.CEPHFS)
@APIDoc("Dashboard UI helper function; not part of the public API", "CephFSUi")
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

    @handle_cephfs_error()
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
        except (cephfs.PermissionError, cephfs.ObjectNotFound):  # pragma: no cover
            paths = []
        return paths


@APIRouter('/cephfs/subvolume', Scope.CEPHFS)
@APIDoc('CephFS Subvolume Management API', 'CephFSSubvolume')
class CephFSSubvolume(RESTController):

    def get(self, vol_name: str, group_name: str = "", info=True):
        params = {'vol_name': vol_name}
        if group_name:
            params['group_name'] = group_name
        error_code, out, err = mgr.remote(
            'volumes', '_cmd_fs_subvolume_ls', None, params)
        if error_code != 0:
            raise DashboardException(
                f'Failed to list subvolumes for volume {vol_name}: {err}'
            )
        subvolumes = json.loads(out)

        if info:
            for subvolume in subvolumes:
                params['sub_name'] = subvolume['name']
                error_code, out, err = mgr.remote('volumes', '_cmd_fs_subvolume_info', None,
                                                  params)
                # just ignore this error for now so the subvolumes page will load.
                # the ideal solution is to implement a status page where clone status
                # can be displayed
                if error_code == -errno.EAGAIN:
                    pass
                elif error_code != 0:
                    raise DashboardException(
                        f'Failed to get info for subvolume {subvolume["name"]}: {err}'
                    )
                if out:
                    subvolume['info'] = json.loads(out)
        return subvolumes

    @RESTController.Resource('GET')
    def info(self, vol_name: str, subvol_name: str, group_name: str = ""):
        params = {'vol_name': vol_name, 'sub_name': subvol_name}
        if group_name:
            params['group_name'] = group_name
        error_code, out, err = mgr.remote('volumes', '_cmd_fs_subvolume_info', None,
                                          params)
        if error_code != 0:
            raise DashboardException(
                f'Failed to get info for subvolume {subvol_name}: {err}'
            )
        return json.loads(out)

    def create(self, vol_name: str, subvol_name: str, **kwargs):
        error_code, _, err = mgr.remote('volumes', '_cmd_fs_subvolume_create', None, {
            'vol_name': vol_name, 'sub_name': subvol_name, **kwargs})
        if error_code != 0:
            raise DashboardException(
                f'Failed to create subvolume {subvol_name}: {err}'
            )

        return f'Subvolume {subvol_name} created successfully'

    def set(self, vol_name: str, subvol_name: str, size: str, group_name: str = ""):
        params = {'vol_name': vol_name, 'sub_name': subvol_name}
        if size:
            params['new_size'] = size
            if group_name:
                params['group_name'] = group_name
            error_code, _, err = mgr.remote('volumes', '_cmd_fs_subvolume_resize', None,
                                            params)
            if error_code != 0:
                raise DashboardException(
                    f'Failed to update subvolume {subvol_name}: {err}'
                )

        return f'Subvolume {subvol_name} updated successfully'

    def delete(self, vol_name: str, subvol_name: str, group_name: str = "",
               retain_snapshots: bool = False):
        params = {'vol_name': vol_name, 'sub_name': subvol_name}
        if group_name:
            params['group_name'] = group_name
        retain_snapshots = str_to_bool(retain_snapshots)
        if retain_snapshots:
            params['retain_snapshots'] = 'True'
        error_code, _, err = mgr.remote(
            'volumes', '_cmd_fs_subvolume_rm', None, params)
        if error_code != 0:
            raise DashboardException(
                msg=f'Failed to remove subvolume {subvol_name}: {err}',
                component='cephfs')
        return f'Subvolume {subvol_name} removed successfully'

    @RESTController.Resource('GET')
    def exists(self, vol_name: str, group_name=''):
        params = {'vol_name': vol_name}
        if group_name:
            params['group_name'] = group_name
        error_code, out, err = mgr.remote(
            'volumes', '_cmd_fs_subvolume_exist', None, params)
        if error_code != 0:
            raise DashboardException(
                f'Failed to check if subvolume exists: {err}'
            )
        if out == 'no subvolume exists':
            return False
        return True


@APIRouter('/cephfs/subvolume/group', Scope.CEPHFS)
@APIDoc("Cephfs Subvolume Group Management API", "CephfsSubvolumeGroup")
class CephFSSubvolumeGroups(RESTController):

    def get(self, vol_name, info=True):
        if not vol_name:
            raise DashboardException(
                f'Error listing subvolume groups for {vol_name}')
        error_code, out, err = mgr.remote('volumes', '_cmd_fs_subvolumegroup_ls',
                                          None, {'vol_name': vol_name})
        if error_code != 0:
            raise DashboardException(
                f'Error listing subvolume groups for {vol_name}')
        subvolume_groups = json.loads(out)

        if info:
            for group in subvolume_groups:
                error_code, out, err = mgr.remote('volumes', '_cmd_fs_subvolumegroup_info',
                                                  None, {'vol_name': vol_name,
                                                         'group_name': group['name']})
                if error_code != 0:
                    raise DashboardException(
                        f'Failed to get info for subvolume group {group["name"]}: {err}'
                    )
                group['info'] = json.loads(out)

                error_code, out, err = mgr.remote('volumes', '_cmd_fs_subvolumegroup_getpath',
                                                  None, {'vol_name': vol_name,
                                                         'group_name': group['name']})
                if error_code != 0:
                    raise DashboardException(
                        f'Failed to get path for subvolume group {group["name"]}: {err}'
                    )
                group['info']['path'] = out
        return subvolume_groups

    @RESTController.Resource('GET')
    def info(self, vol_name: str, group_name: str):
        error_code, out, err = mgr.remote('volumes', '_cmd_fs_subvolumegroup_info', None, {
            'vol_name': vol_name, 'group_name': group_name})
        if error_code != 0:
            raise DashboardException(
                f'Failed to get info for subvolume group {group_name}: {err}'

            )
        group = json.loads(out)
        error_code, out, err = mgr.remote('volumes', '_cmd_fs_subvolumegroup_getpath', None, {
            'vol_name': vol_name, 'group_name': group_name})
        if error_code != 0:
            raise DashboardException(
                f'Failed to get path for subvolume group {group_name}: {err}'
            )
        group['path'] = out
        return group

    def create(self, vol_name: str, group_name: str, **kwargs):
        error_code, _, err = mgr.remote('volumes', '_cmd_fs_subvolumegroup_create', None, {
            'vol_name': vol_name, 'group_name': group_name, **kwargs})
        if error_code != 0:
            raise DashboardException(
                f'Failed to create subvolume group {group_name}: {err}'
            )

    def set(self, vol_name: str, group_name: str, size: str):
        if not size:
            return f'Failed to update subvolume group {group_name}, size was not provided'
        error_code, _, err = mgr.remote('volumes', '_cmd_fs_subvolumegroup_resize', None, {
            'vol_name': vol_name, 'group_name': group_name, 'new_size': size})
        if error_code != 0:
            raise DashboardException(
                f'Failed to update subvolume group {group_name}: {err}'
            )
        return f'Subvolume group {group_name} updated successfully'

    def delete(self, vol_name: str, group_name: str):
        error_code, _, err = mgr.remote(
            'volumes', '_cmd_fs_subvolumegroup_rm', None, {
                'vol_name': vol_name, 'group_name': group_name})
        if error_code != 0:
            raise DashboardException(
                f'Failed to delete subvolume group {group_name}: {err}'
            )
        return f'Subvolume group {group_name} removed successfully'


@APIRouter('/cephfs/subvolume/snapshot', Scope.CEPHFS)
@APIDoc("Cephfs Subvolume Snapshot Management API", "CephfsSubvolumeSnapshot")
class CephFSSubvolumeSnapshots(RESTController):
    def get(self, vol_name: str, subvol_name, group_name: str = '', info=True):
        params = {'vol_name': vol_name, 'sub_name': subvol_name}
        if group_name:
            params['group_name'] = group_name
        error_code, out, err = mgr.remote('volumes', '_cmd_fs_subvolume_snapshot_ls', None,
                                          params)
        if error_code != 0:
            raise DashboardException(
                f'Failed to list subvolume snapshots for subvolume {subvol_name}: {err}'
            )
        snapshots = json.loads(out)

        if info:
            for snapshot in snapshots:
                params['snap_name'] = snapshot['name']
                error_code, out, err = mgr.remote('volumes', '_cmd_fs_subvolume_snapshot_info',
                                                  None, params)
                # just ignore this error for now so the subvolumes page will load.
                # the ideal solution is to implement a status page where clone status
                # can be displayed
                if error_code == -errno.EAGAIN:
                    pass
                elif error_code != 0:
                    raise DashboardException(
                        f'Failed to get info for subvolume snapshot {snapshot["name"]}: {err}'
                    )
                if out:
                    snapshot['info'] = json.loads(out)
        return snapshots

    @RESTController.Resource('GET')
    def info(self, vol_name: str, subvol_name: str, snap_name: str, group_name: str = ''):
        params = {'vol_name': vol_name, 'sub_name': subvol_name, 'snap_name': snap_name}
        if group_name:
            params['group_name'] = group_name
        error_code, out, err = mgr.remote('volumes', '_cmd_fs_subvolume_snapshot_info', None,
                                          params)
        if error_code != 0:
            raise DashboardException(
                f'Failed to get info for subvolume snapshot {snap_name}: {err}'
            )
        return json.loads(out)

    def create(self, vol_name: str, subvol_name: str, snap_name: str, group_name=''):
        params = {'vol_name': vol_name, 'sub_name': subvol_name, 'snap_name': snap_name}
        if group_name:
            params['group_name'] = group_name

        error_code, _, err = mgr.remote('volumes', '_cmd_fs_subvolume_snapshot_create', None,
                                        params)

        if error_code != 0:
            raise DashboardException(
                f'Failed to create subvolume snapshot {snap_name}: {err}'
            )
        return f'Subvolume snapshot {snap_name} created successfully'

    def delete(self, vol_name: str, subvol_name: str, snap_name: str, group_name='', force=True):
        params = {'vol_name': vol_name, 'sub_name': subvol_name, 'snap_name': snap_name}
        if group_name:
            params['group_name'] = group_name
        params['force'] = str_to_bool(force)
        error_code, _, err = mgr.remote('volumes', '_cmd_fs_subvolume_snapshot_rm', None,
                                        params)
        if error_code != 0:
            raise DashboardException(
                f'Failed to delete subvolume snapshot {snap_name}: {err}'
            )
        return f'Subvolume snapshot {snap_name} removed successfully'


@APIRouter('/cephfs/subvolume/snapshot/clone', Scope.CEPHFS)
@APIDoc("Cephfs Snapshot Clone Management API", "CephfsSnapshotClone")
class CephFsSnapshotClone(RESTController):
    @EndpointDoc("Create a clone of a subvolume snapshot")
    def create(self, vol_name: str, subvol_name: str, snap_name: str, clone_name: str,
               group_name='', target_group_name=''):
        params = {'vol_name': vol_name, 'sub_name': subvol_name, 'snap_name': snap_name,
                  'target_sub_name': clone_name}
        if group_name:
            params['group_name'] = group_name

        if target_group_name:
            params['target_group_name'] = target_group_name

        error_code, _, err = mgr.remote('volumes', '_cmd_fs_subvolume_snapshot_clone', None,
                                        params)
        if error_code != 0:
            raise DashboardException(
                f'Failed to create clone {clone_name}: {err}'
            )
        return f'Clone {clone_name} created successfully'


@APIRouter('/cephfs/snapshot/schedule', Scope.CEPHFS)
@APIDoc("Cephfs Snapshot Scheduling API", "CephFSSnapshotSchedule")
class CephFSSnapshotSchedule(RESTController):

    def list(self, fs: str, path: str = '/', recursive: bool = True):
        error_code, out, err = mgr.remote('snap_schedule', 'snap_schedule_list',
                                          path, recursive, fs, None, None, 'plain')
        if len(out) == 0:
            return []

        snapshot_schedule_list = out.split('\n')
        output: List[Any] = []

        for snap in snapshot_schedule_list:
            current_path = snap.strip().split(' ')[0]
            error_code, status_out, err = mgr.remote('snap_schedule', 'snap_schedule_get',
                                                     current_path, fs, None, None, 'json')
            output = output + json.loads(status_out)

        output_json = json.dumps(output)

        if error_code != 0:
            raise DashboardException(
                f'Failed to get list of snapshot schedules for path {path}: {err}'
            )
        return json.loads(output_json)

    def create(self, fs: str, path: str, snap_schedule: str, start: str, retention_policy=None,
               subvol=None, group=None):
        error_code, _, err = mgr.remote('snap_schedule',
                                        'snap_schedule_add',
                                        path,
                                        snap_schedule,
                                        start,
                                        fs,
                                        subvol,
                                        group)

        if retention_policy:
            retention_policies = retention_policy.split('|')
            for retention in retention_policies:
                retention_count = retention.split('-')[0]
                retention_spec_or_period = retention.split('-')[1]
                error_code_retention, _, err_retention = mgr.remote('snap_schedule',
                                                                    'snap_schedule_retention_add',
                                                                    path,
                                                                    retention_spec_or_period,
                                                                    retention_count,
                                                                    fs,
                                                                    subvol,
                                                                    group)
                if error_code_retention != 0:
                    raise DashboardException(
                        f'Failed to add retention policy for path {path}: {err_retention}'
                    )
        if error_code != 0:
            raise DashboardException(
                f'Failed to create snapshot schedule for path {path}: {err}'
            )

        return f'Snapshot schedule for path {path} created successfully'

    def set(self, fs: str, path: str, retention_to_add=None, retention_to_remove=None,
            subvol=None, group=None):
        def editRetentionPolicies(method, retention_policy):
            if not retention_policy:
                return

            retention_policies = retention_policy.split('|')
            for retention in retention_policies:
                retention_count = retention.split('-')[0]
                retention_spec_or_period = retention.split('-')[1]
                error_code_retention, _, err_retention = mgr.remote('snap_schedule',
                                                                    method,
                                                                    path,
                                                                    retention_spec_or_period,
                                                                    retention_count,
                                                                    fs,
                                                                    subvol,
                                                                    group)
                if error_code_retention != 0:
                    raise DashboardException(
                        f'Failed to add/remove retention policy for path {path}: {err_retention}'
                    )

        editRetentionPolicies('snap_schedule_retention_rm', retention_to_remove)
        editRetentionPolicies('snap_schedule_retention_add', retention_to_add)

        return f'Retention policies for snapshot schedule on path {path} updated successfully'

    @RESTController.Resource('DELETE')
    def delete_snapshot(self, fs: str, path: str, schedule: str, start: str,
                        retention_policy=None, subvol=None, group=None):
        if retention_policy:
            # check if there are other snap schedules for this exact same path
            error_code, out, err = mgr.remote('snap_schedule', 'snap_schedule_list',
                                              path, False, fs, subvol, group, 'plain')

            if error_code != 0:
                raise DashboardException(
                    f'Failed to get snapshot schedule list for path {path}: {err}'
                )
            # only remove the retention policies if there no other snap schedules for this path
            snapshot_schedule_list = out.split('\n')
            if len(snapshot_schedule_list) <= 1:
                retention_policies = retention_policy.split('|')
                for retention in retention_policies:
                    retention_count = retention.split('-')[0]
                    retention_spec_or_period = retention.split('-')[1]
                    error_code, _, err = mgr.remote('snap_schedule',
                                                    'snap_schedule_retention_rm',
                                                    path,
                                                    retention_spec_or_period,
                                                    retention_count,
                                                    fs,
                                                    subvol,
                                                    group)
                    if error_code != 0:
                        raise DashboardException(
                            f'Failed to remove retention policy for path {path}: {err}'
                        )
        # remove snap schedule
        error_code, _, err = mgr.remote('snap_schedule',
                                        'snap_schedule_rm',
                                        path,
                                        schedule,
                                        start,
                                        fs,
                                        subvol,
                                        group)
        if error_code != 0:
            raise DashboardException(
                f'Failed to delete snapshot schedule for path {path}: {err}'
            )

        return f'Snapshot schedule for path {path} deleted successfully'

    @RESTController.Resource('POST')
    def deactivate(self, fs: str, path: str, schedule: str, start: str, subvol=None, group=None):
        error_code, _, err = mgr.remote('snap_schedule',
                                        'snap_schedule_deactivate',
                                        path,
                                        schedule,
                                        start,
                                        fs,
                                        subvol,
                                        group)
        if error_code != 0:
            raise DashboardException(
                f'Failed to deactivate snapshot schedule for path {path}: {err}'
            )

        return f'Snapshot schedule for path {path} deactivated successfully'

    @RESTController.Resource('POST')
    def activate(self, fs: str, path: str, schedule: str, start: str, subvol=None, group=None):
        error_code, _, err = mgr.remote('snap_schedule',
                                        'snap_schedule_activate',
                                        path,
                                        schedule,
                                        start,
                                        fs,
                                        subvol,
                                        group)
        if error_code != 0:
            raise DashboardException(
                f'Failed to activate snapshot schedule for path {path}: {err}'
            )

        return f'Snapshot schedule for path {path} activated successfully'
