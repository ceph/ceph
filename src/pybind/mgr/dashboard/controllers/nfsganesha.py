# -*- coding: utf-8 -*-

import logging
import os
import json
from functools import partial

import cephfs
import cherrypy
# Importing from nfs module throws Attribute Error
# https://gist.github.com/varshar16/61ac26426bbe5f5f562ebb14bcd0f548
#from nfs.export_utils import NFS_GANESHA_SUPPORTED_FSALS
#from nfs.utils import available_clusters

from .. import mgr
from ..security import Scope
from ..services.cephfs import CephFS
from ..services.exception import DashboardException, serialize_dashboard_exception
from ..services.rgw_client import NoCredentialsException, \
    NoRgwDaemonsException, RequestException, RgwClient
from . import APIDoc, APIRouter, BaseController, Endpoint, EndpointDoc, \
    ReadPermission, RESTController, Task, UIRouter

logger = logging.getLogger('controllers.nfs')


class NFSException(DashboardException):
    def __init__(self, msg):
        super(NFSException, self).__init__(component="nfs", msg=msg)

# Remove this once attribute error is fixed
NFS_GANESHA_SUPPORTED_FSALS = ['CEPH', 'RGW']

# documentation helpers
EXPORT_SCHEMA = {
    'export_id': (int, 'Export ID'),
    'path': (str, 'Export path'),
    'cluster_id': (str, 'Cluster identifier'),
    'daemons': ([str], 'List of NFS Ganesha daemons identifiers'),
    'pseudo': (str, 'Pseudo FS path'),
    'access_type': (str, 'Export access type'),
    'squash': (str, 'Export squash policy'),
    'security_label': (str, 'Security label'),
    'protocols': ([int], 'List of protocol types'),
    'transports': ([str], 'List of transport types'),
    'fsal': ({
        'name': (str, 'name of FSAL'),
        'user_id': (str, 'CephX user id', True),
        'filesystem': (str, 'CephFS filesystem ID', True),
        'sec_label_xattr': (str, 'Name of xattr for security label', True),
        'rgw_user_id': (str, 'RGW user id', True)
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
    'daemons': ([str], 'List of NFS Ganesha daemons identifiers'),
    'pseudo': (str, 'Pseudo FS path'),
    'access_type': (str, 'Export access type'),
    'squash': (str, 'Export squash policy'),
    'security_label': (str, 'Security label'),
    'protocols': ([int], 'List of protocol types'),
    'transports': ([str], 'List of transport types'),
    'fsal': ({
        'name': (str, 'name of FSAL'),
        'user_id': (str, 'CephX user id', True),
        'filesystem': (str, 'CephFS filesystem ID', True),
        'sec_label_xattr': (str, 'Name of xattr for security label', True),
        'rgw_user_id': (str, 'RGW user id', True)
    }, 'FSAL configuration'),
    'clients': ([{
        'addresses': ([str], 'list of IP addresses'),
        'access_type': (str, 'Client access type'),
        'squash': (str, 'Client squash policy')
    }], 'List of client configurations'),
    'reload_daemons': (bool,
                       'Trigger reload of NFS-Ganesha daemons configuration',
                       True)
}


# pylint: disable=not-callable
def NfsTask(name, metadata, wait_for):  # noqa: N802
    def composed_decorator(func):
        return Task("nfs/{}".format(name), metadata, wait_for,
                    partial(serialize_dashboard_exception,
                            include_http_status=True))(func)
    return composed_decorator


@APIRouter('/nfs-ganesha', Scope.NFS_GANESHA)
@APIDoc("NFS-Ganesha Management API", "NFS-Ganesha")
class NFSGanesha(RESTController):

    @EndpointDoc("Status of NFS-Ganesha management feature",
                 responses={200: {
                     'available': (bool, "Is API available?"),
                     'message': (str, "Error message")
                 }})
    @Endpoint()
    @ReadPermission
    def status(self):
        '''
        FIXME: update this to check if any nfs cluster is available. Otherwise this endpoint can be safely removed too.
        As it was introduced to check dashboard pool and namespace configuration.
        try:
            cluster_ls = available_clusters(mgr)
            if not cluster_ls:
                raise NFSException('Please deploy a cluster using `nfs cluster create ... or orch apply nfs ..')
        except (NameError, ImportError) as e:
            status['message'] = str(e)  # type: ignore
            status['available'] = False
        return status
        '''
        return {'available': True, 'message': None}


@APIRouter('/nfs-ganesha/export', Scope.NFS_GANESHA)
@APIDoc(group="NFS-Ganesha")
class NFSGaneshaExports(RESTController):
    RESOURCE_ID = "cluster_id/export_id"

    @EndpointDoc("List all NFS-Ganesha exports",
                 responses={200: [EXPORT_SCHEMA]})
    def list(self):
        '''
        list exports based on cluster_id ?
        export_mgr = mgr.remote('nfs', 'fetch_nfs_export_obj')
        ret, out, err = export_mgr.list_exports(cluster_id=cluster_id, detailed=True)
        if ret == 0:
            return json.loads(out)
        raise NFSException(f"Failed to list exports: {err}")
        '''
        return mgr.remote('nfs', 'export_ls')

    @NfsTask('create', {'path': '{path}', 'fsal': '{fsal.name}',
                        'cluster_id': '{cluster_id}'}, 2.0)
    @EndpointDoc("Creates a new NFS-Ganesha export",
                 parameters=CREATE_EXPORT_SCHEMA,
                 responses={201: EXPORT_SCHEMA})
    def create(self, path, cluster_id, daemons, pseudo, access_type,
               squash, security_label, protocols, transports, fsal, clients,
               reload_daemons=True):
        fsal.pop('user_id')  # mgr/nfs does not let you customize user_id
        raw_ex = {
            'path': path,
            'pseudo': pseudo,
            'cluster_id': cluster_id,
            'daemons': daemons,
            'access_type': access_type,
            'squash': squash,
            'security_label': security_label,
            'protocols': protocols,
            'transports': transports,
            'fsal': fsal,
            'clients': clients
        }
        export_mgr = mgr.remote('nfs', 'fetch_nfs_export_obj')
        ret, out, err = export_mgr.apply_export(cluster_id, json.dumps(raw_ex))
        if ret == 0:
            return export_mgr._get_export_dict(cluster_id, pseudo)
        raise NFSException(f"Export creation failed {err}")

    @EndpointDoc("Get an NFS-Ganesha export",
                 parameters={
                     'cluster_id': (str, 'Cluster identifier'),
                     'export_id': (int, "Export ID")
                 },
                 responses={200: EXPORT_SCHEMA})
    def get(self, cluster_id, export_id):
        '''
         Get export by pseudo path?
         export_mgr = mgr.remote('nfs', 'fetch_nfs_export_obj')
        return export_mgr._get_export_dict(cluster_id, pseudo)

         Get export by id
         export_mgr = mgr.remote('nfs', 'fetch_nfs_export_obj')
         return export_mgr.get_export_by_id(cluster_id, export_id)
        '''
        return mgr.remote('nfs', 'export_get', cluster_id, export_id)

    @NfsTask('edit', {'cluster_id': '{cluster_id}', 'export_id': '{export_id}'},
             2.0)
    @EndpointDoc("Updates an NFS-Ganesha export",
                 parameters=dict(export_id=(int, "Export ID"),
                                 **CREATE_EXPORT_SCHEMA),
                 responses={200: EXPORT_SCHEMA})
    def set(self, cluster_id, export_id, path, daemons, pseudo, access_type,
            squash, security_label, protocols, transports, fsal, clients,
            reload_daemons=True):

        fsal.pop('user_id')  # mgr/nfs does not let you customize user_id
        raw_ex = {
            'path': path,
            'pseudo': pseudo,
            'cluster_id': cluster_id,
            'daemons': daemons,
            'access_type': access_type,
            'squash': squash,
            'security_label': security_label,
            'protocols': protocols,
            'transports': transports,
            'fsal': fsal,
            'clients': clients
        }

        export_mgr = mgr.remote('nfs', 'fetch_nfs_export_obj')
        ret, out, err = export_mgr.apply_export(cluster_id, json.dumps(raw_ex))
        if ret == 0:
            return export_mgr._get_export_dict(cluster_id, pseudo)
        raise NFSException(f"Failed to update export: {err}")

    @NfsTask('delete', {'cluster_id': '{cluster_id}',
                        'export_id': '{export_id}'}, 2.0)
    @EndpointDoc("Deletes an NFS-Ganesha export",
                 parameters={
                     'cluster_id': (str, 'Cluster identifier'),
                     'export_id': (int, "Export ID"),
                     'reload_daemons': (bool,
                                        'Trigger reload of NFS-Ganesha daemons'
                                        ' configuration',
                                        True)
                 })
    def delete(self, cluster_id, export_id, reload_daemons=True):
        '''
         Delete by pseudo path
         export_mgr = mgr.remote('nfs', 'fetch_nfs_export_obj')
         export_mgr.delete_export(cluster_id, pseudo)

         if deleting by export id
         export_mgr = mgr.remote('nfs', 'fetch_nfs_export_obj')
         export = export_mgr.get_export_by_id(cluster_id, export_id)
         ret, out, err = export_mgr.delete_export(cluster_id=cluster_id, pseudo_path=export['pseudo'])
         if ret != 0:
            raise NFSException(err)
        '''
        export_id = int(export_id)

        export = mgr.remote('nfs', 'export_get', cluster_id, export_id)
        if not export:
            raise cherrypy.HTTPError(404)  # pragma: no cover - the handling is too obvious
        mgr.remote('nfs', 'export_rm', cluster_id, export['pseudo'])


# FIXME: remove this; dashboard should only care about clusters.
@APIRouter('/nfs-ganesha/daemon', Scope.NFS_GANESHA)
@APIDoc(group="NFS-Ganesha")
class NFSGaneshaService(RESTController):

    @EndpointDoc("List NFS-Ganesha daemons information",
                 responses={200: [{
                     'daemon_id': (str, 'Daemon identifier'),
                     'cluster_id': (str, 'Cluster identifier'),
                     'cluster_type': (str, 'Cluster type'),   # FIXME: remove this property
                     'status': (int, 'Status of daemon', True),
                     'desc': (str, 'Status description', True)
                 }]})
    def list(self):
        return mgr.remote('nfs', 'daemon_ls')


@UIRouter('/nfs-ganesha', Scope.NFS_GANESHA)
class NFSGaneshaUi(BaseController):
    @Endpoint('GET', '/cephx/clients')
    @ReadPermission
    def cephx_clients(self):
        # FIXME: remove this; cephx users/creds are managed by mgr/nfs
        return ['admin']

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
                                         component='nfsganesha',
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

    @Endpoint('GET', '/rgw/buckets')
    @ReadPermission
    def buckets(self, user_id=None):
        try:
            return RgwClient.instance(user_id).get_buckets()
        except (DashboardException, NoCredentialsException, RequestException,
                NoRgwDaemonsException):
            return []

    @Endpoint('GET', '/clusters')
    @ReadPermission
    def clusters(self):
        '''
        Remove this remote call instead directly use available_cluster() method. It returns list of cluster names: ['vstart']
        The current dashboard api needs to changed from following to simply list of strings
              [
                {
                     'pool': 'nfs-ganesha',
                     'namespace': cluster_id,
                     'type': 'orchestrator',
                     'daemon_conf': None
                 } for cluster_id in available_clusters()
               ]
        As pool, namespace, cluster type and daemon_conf are not required for listing cluster by mgr/nfs module
        return available_cluster(mgr)
        '''
        return mgr.remote('nfs', 'cluster_ls')
