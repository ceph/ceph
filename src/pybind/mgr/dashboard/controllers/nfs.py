# -*- coding: utf-8 -*-

import json
import logging
import os
from functools import partial
from typing import Any, Dict, List, Optional

import cephfs
from mgr_module import NFS_GANESHA_SUPPORTED_FSALS

from .. import mgr
from ..security import Scope
from ..services.cephfs import CephFS
from ..services.exception import DashboardException, handle_cephfs_error, \
    serialize_dashboard_exception
from . import APIDoc, APIRouter, BaseController, Endpoint, EndpointDoc, \
    ReadPermission, RESTController, Task, UIRouter
from ._version import APIVersion

logger = logging.getLogger('controllers.nfs')


class NFSException(DashboardException):
    def __init__(self, msg):
        super(NFSException, self).__init__(component="nfs", msg=msg)


# documentation helpers
EXPORT_SCHEMA = {
    'export_id': (int, 'Export ID'),
    'path': (str, 'Export path'),
    'cluster_id': (str, 'Cluster identifier'),
    'pseudo': (str, 'Pseudo FS path'),
    'access_type': (str, 'Export access type'),
    'squash': (str, 'Export squash policy'),
    'security_label': (str, 'Security label'),
    'protocols': ([int], 'List of protocol types'),
    'transports': ([str], 'List of transport types'),
    'fsal': ({
        'name': (str, 'name of FSAL'),
        'fs_name': (str, 'CephFS filesystem name', True),
        'sec_label_xattr': (str, 'Name of xattr for security label', True),
        'user_id': (str, 'User id', True)
    }, 'FSAL configuration'),
    'clients': ([{
        'addresses': ([str], 'list of IP addresses'),
        'access_type': (str, 'Client access type'),
        'squash': (str, 'Client squash policy')
    }], 'List of client configurations'),
}


CREATE_EXPORT_SCHEMA = {
    'path': (str, 'Export path'),
    'cluster_id': (str, 'Cluster identifier'),
    'pseudo': (str, 'Pseudo FS path'),
    'access_type': (str, 'Export access type'),
    'squash': (str, 'Export squash policy'),
    'security_label': (str, 'Security label'),
    'protocols': ([int], 'List of protocol types'),
    'transports': ([str], 'List of transport types'),
    'fsal': ({
        'name': (str, 'name of FSAL'),
        'fs_name': (str, 'CephFS filesystem name', True),
        'sec_label_xattr': (str, 'Name of xattr for security label', True)
    }, 'FSAL configuration'),
    'clients': ([{
        'addresses': ([str], 'list of IP addresses'),
        'access_type': (str, 'Client access type'),
        'squash': (str, 'Client squash policy')
    }], 'List of client configurations')
}


# pylint: disable=not-callable
def NfsTask(name, metadata, wait_for):  # noqa: N802
    def composed_decorator(func):
        return Task("nfs/{}".format(name), metadata, wait_for,
                    partial(serialize_dashboard_exception,
                            include_http_status=True))(func)
    return composed_decorator


@APIRouter('/nfs-ganesha/cluster', Scope.NFS_GANESHA)
@APIDoc("NFS-Ganesha Cluster Management API", "NFS-Ganesha")
class NFSGaneshaCluster(RESTController):
    @ReadPermission
    @RESTController.MethodMap(version=APIVersion.EXPERIMENTAL)
    def list(self):
        return mgr.remote('nfs', 'cluster_ls')


@APIRouter('/nfs-ganesha/export', Scope.NFS_GANESHA)
@APIDoc(group="NFS-Ganesha")
class NFSGaneshaExports(RESTController):
    RESOURCE_ID = "cluster_id/export_id"

    @staticmethod
    def _get_schema_export(export: Dict[str, Any]) -> Dict[str, Any]:
        """
        Method that avoids returning export info not exposed in the export schema
        e.g., rgw user access/secret keys.
        """
        schema_fsal_info = {}
        for key in export['fsal'].keys():
            if key in EXPORT_SCHEMA['fsal'][0].keys():  # type: ignore
                schema_fsal_info[key] = export['fsal'][key]
        export['fsal'] = schema_fsal_info
        return export

    @EndpointDoc("List all NFS-Ganesha exports",
                 responses={200: [EXPORT_SCHEMA]})
    def list(self) -> List[Dict[str, Any]]:
        exports = []
        for export in mgr.remote('nfs', 'export_ls'):
            exports.append(self._get_schema_export(export))

        return exports

    @handle_cephfs_error()
    @NfsTask('create', {'path': '{path}', 'fsal': '{fsal.name}',
                        'cluster_id': '{cluster_id}'}, 2.0)
    @EndpointDoc("Creates a new NFS-Ganesha export",
                 parameters=CREATE_EXPORT_SCHEMA,
                 responses={201: EXPORT_SCHEMA})
    @RESTController.MethodMap(version=APIVersion(2, 0))  # type: ignore
    def create(self, path, cluster_id, pseudo, access_type,
               squash, security_label, protocols, transports, fsal, clients) -> Dict[str, Any]:
        export_mgr = mgr.remote('nfs', 'fetch_nfs_export_obj')
        if export_mgr.get_export_by_pseudo(cluster_id, pseudo):
            raise DashboardException(msg=f'Pseudo {pseudo} is already in use.',
                                     component='nfs')
        if hasattr(fsal, 'user_id'):
            fsal.pop('user_id')  # mgr/nfs does not let you customize user_id
        raw_ex = {
            'path': path,
            'pseudo': pseudo,
            'cluster_id': cluster_id,
            'access_type': access_type,
            'squash': squash,
            'security_label': security_label,
            'protocols': protocols,
            'transports': transports,
            'fsal': fsal,
            'clients': clients
        }
        applied_exports = export_mgr.apply_export(cluster_id, json.dumps(raw_ex))
        if not applied_exports.has_error:
            return self._get_schema_export(
                export_mgr.get_export_by_pseudo(cluster_id, pseudo))
        raise NFSException(f"Export creation failed {applied_exports.changes[0].msg}")

    @EndpointDoc("Get an NFS-Ganesha export",
                 parameters={
                     'cluster_id': (str, 'Cluster identifier'),
                     'export_id': (str, "Export ID")
                 },
                 responses={200: EXPORT_SCHEMA})
    def get(self, cluster_id, export_id) -> Optional[Dict[str, Any]]:
        export_id = int(export_id)
        export = mgr.remote('nfs', 'export_get', cluster_id, export_id)
        if export:
            export = self._get_schema_export(export)

        return export

    @NfsTask('edit', {'cluster_id': '{cluster_id}', 'export_id': '{export_id}'},
             2.0)
    @EndpointDoc("Updates an NFS-Ganesha export",
                 parameters=dict(export_id=(int, "Export ID"),
                                 **CREATE_EXPORT_SCHEMA),
                 responses={200: EXPORT_SCHEMA})
    @RESTController.MethodMap(version=APIVersion(2, 0))  # type: ignore
    def set(self, cluster_id, export_id, path, pseudo, access_type,
            squash, security_label, protocols, transports, fsal, clients) -> Dict[str, Any]:

        if hasattr(fsal, 'user_id'):
            fsal.pop('user_id')  # mgr/nfs does not let you customize user_id
        raw_ex = {
            'path': path,
            'pseudo': pseudo,
            'cluster_id': cluster_id,
            'export_id': export_id,
            'access_type': access_type,
            'squash': squash,
            'security_label': security_label,
            'protocols': protocols,
            'transports': transports,
            'fsal': fsal,
            'clients': clients
        }

        export_mgr = mgr.remote('nfs', 'fetch_nfs_export_obj')
        applied_exports = export_mgr.apply_export(cluster_id, json.dumps(raw_ex))
        if not applied_exports.has_error:
            return self._get_schema_export(
                export_mgr.get_export_by_pseudo(cluster_id, pseudo))
        raise NFSException(f"Export creation failed {applied_exports.changes[0].msg}")

    @NfsTask('delete', {'cluster_id': '{cluster_id}',
                        'export_id': '{export_id}'}, 2.0)
    @EndpointDoc("Deletes an NFS-Ganesha export",
                 parameters={
                     'cluster_id': (str, 'Cluster identifier'),
                     'export_id': (int, "Export ID")
                 })
    @RESTController.MethodMap(version=APIVersion(2, 0))  # type: ignore
    def delete(self, cluster_id, export_id):
        export_id = int(export_id)

        export = mgr.remote('nfs', 'export_get', cluster_id, export_id)
        if not export:
            raise DashboardException(
                http_status_code=404,
                msg=f'Export with id {export_id} not found.',
                component='nfs')
        mgr.remote('nfs', 'export_rm', cluster_id, export['pseudo'])


@UIRouter('/nfs-ganesha', Scope.NFS_GANESHA)
class NFSGaneshaUi(BaseController):
    @Endpoint('GET', '/fsals')
    @ReadPermission
    def fsals(self):
        return NFS_GANESHA_SUPPORTED_FSALS

    @Endpoint('GET', '/lsdir')
    @ReadPermission
    def lsdir(self, fs_name, root_dir=None, depth=1):  # pragma: no cover
        if root_dir is None:
            root_dir = "/"
        if not root_dir.startswith('/'):
            root_dir = '/{}'.format(root_dir)
        root_dir = os.path.normpath(root_dir)

        try:
            depth = int(depth)
            error_msg = ''
            if depth < 0:
                error_msg = '`depth` must be greater or equal to 0.'
            if depth > 5:
                logger.warning("Limiting depth to maximum value of 5: "
                               "input depth=%s", depth)
                depth = 5
        except ValueError:
            error_msg = '`depth` must be an integer.'
        finally:
            if error_msg:
                raise DashboardException(code=400,
                                         component='nfs',
                                         msg=error_msg)

        try:
            cfs = CephFS(fs_name)
            paths = [root_dir]
            paths.extend([p['path'].rstrip('/')
                          for p in cfs.ls_dir(root_dir, depth)])
        except (cephfs.ObjectNotFound, cephfs.PermissionError):
            paths = []
        return {'paths': paths}

    @Endpoint('GET', '/cephfs/filesystems')
    @ReadPermission
    def filesystems(self):
        return CephFS.list_filesystems()

    @Endpoint()
    @ReadPermission
    def status(self):
        status = {'available': True, 'message': None}
        try:
            mgr.remote('nfs', 'cluster_ls')
        except (ImportError, RuntimeError) as error:
            logger.exception(error)
            status['available'] = False
            status['message'] = str(error)  # type: ignore

        return status
