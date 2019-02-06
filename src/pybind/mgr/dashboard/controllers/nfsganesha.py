# -*- coding: utf-8 -*-
from __future__ import absolute_import

from functools import partial

import cherrypy
import cephfs

from . import ApiController, RESTController, UiApiController, BaseController, \
              Endpoint, Task
from .. import logger
from ..security import Scope
from ..services.cephfs import CephFS
from ..services.cephx import CephX
from ..services.exception import serialize_dashboard_exception
from ..services.ganesha import Ganesha, GaneshaConf, NFSException
from ..services.rgw_client import RgwClient


# pylint: disable=not-callable
def NfsTask(name, metadata, wait_for):
    def composed_decorator(func):
        return Task("nfs/{}".format(name), metadata, wait_for,
                    partial(serialize_dashboard_exception,
                            include_http_status=True))(func)
    return composed_decorator


@ApiController('/nfs-ganesha/export', Scope.NFS_GANESHA)
class NFSGaneshaExports(RESTController):
    RESOURCE_ID = "cluster_id/export_id"

    def list(self):
        result = []
        for cluster_id in Ganesha.get_ganesha_clusters():
            result.extend(
                [export.to_dict()
                 for export in GaneshaConf.instance(cluster_id).list_exports()])
        return result

    @NfsTask('create', {'path': '{path}', 'fsal': '{fsal.name}',
                        'cluster_id': '{cluster_id}'}, 2.0)
    def create(self, path, cluster_id, daemons, pseudo, tag, access_type,
               squash, security_label, protocols, transports, fsal, clients,
               reload_daemons=True):
        if fsal['name'] not in Ganesha.fsals_available():
            raise NFSException("Cannot create this export. "
                               "FSAL '{}' cannot be managed by the dashboard."
                               .format(fsal['name']))

        ganesha_conf = GaneshaConf.instance(cluster_id)
        ex_id = ganesha_conf.create_export({
            'path': path,
            'pseudo': pseudo,
            'cluster_id': cluster_id,
            'daemons': daemons,
            'tag': tag,
            'access_type': access_type,
            'squash': squash,
            'security_label': security_label,
            'protocols': protocols,
            'transports': transports,
            'fsal': fsal,
            'clients': clients
        })
        if reload_daemons:
            ganesha_conf.reload_daemons(daemons)
        return ganesha_conf.get_export(ex_id).to_dict()

    def get(self, cluster_id, export_id):
        export_id = int(export_id)
        ganesha_conf = GaneshaConf.instance(cluster_id)
        if not ganesha_conf.has_export(export_id):
            raise cherrypy.HTTPError(404)
        return ganesha_conf.get_export(export_id).to_dict()

    @NfsTask('edit', {'cluster_id': '{cluster_id}', 'export_id': '{export_id}'},
             2.0)
    def set(self, cluster_id, export_id, path, daemons, pseudo, tag, access_type,
            squash, security_label, protocols, transports, fsal, clients,
            reload_daemons=True):
        export_id = int(export_id)
        ganesha_conf = GaneshaConf.instance(cluster_id)

        if not ganesha_conf.has_export(export_id):
            raise cherrypy.HTTPError(404)

        if fsal['name'] not in Ganesha.fsals_available():
            raise NFSException("Cannot make modifications to this export. "
                               "FSAL '{}' cannot be managed by the dashboard."
                               .format(fsal['name']))

        old_export = ganesha_conf.update_export({
            'export_id': export_id,
            'path': path,
            'cluster_id': cluster_id,
            'daemons': daemons,
            'pseudo': pseudo,
            'tag': tag,
            'access_type': access_type,
            'squash': squash,
            'security_label': security_label,
            'protocols': protocols,
            'transports': transports,
            'fsal': fsal,
            'clients': clients
        })
        daemons = list(daemons)
        for d_id in old_export.daemons:
            if d_id not in daemons:
                daemons.append(d_id)
        if reload_daemons:
            ganesha_conf.reload_daemons(daemons)
        return ganesha_conf.get_export(export_id).to_dict()

    @NfsTask('delete', {'cluster_id': '{cluster_id}',
                        'export_id': '{export_id}'}, 2.0)
    def delete(self, cluster_id, export_id, reload_daemons=True):
        export_id = int(export_id)
        ganesha_conf = GaneshaConf.instance(cluster_id)

        if not ganesha_conf.has_export(export_id):
            raise cherrypy.HTTPError(404)

        export = ganesha_conf.remove_export(export_id)
        if reload_daemons:
            ganesha_conf.reload_daemons(export.daemons)


@ApiController('/nfs-ganesha/daemon')
class NFSGaneshaService(RESTController):

    def list(self):
        status_dict = Ganesha.get_daemons_status()
        if status_dict:
            return [
                {
                    'daemon_id': daemon_id,
                    'cluster_id': cluster_id,
                    'status': status_dict[cluster_id][daemon_id]['status'],
                    'desc': status_dict[cluster_id][daemon_id]['desc']
                }
                for daemon_id in status_dict[cluster_id]
                for cluster_id in status_dict
            ]

        result = []
        for cluster_id in Ganesha.get_ganesha_clusters():
            result.extend(
                [{'daemon_id': daemon_id, 'cluster_id': cluster_id}
                 for daemon_id in GaneshaConf.instance(cluster_id).list_daemons()])
        return result


@UiApiController('/nfs-ganesha')
class NFSGaneshaUi(BaseController):
    @Endpoint('GET', '/cephx/clients')
    def cephx_clients(self):
        return [client for client in CephX.list_clients()]

    @Endpoint('GET', '/fsals')
    def fsals(self):
        return Ganesha.fsals_available()

    @Endpoint('GET', '/lsdir')
    def lsdir(self, root_dir=None, depth=1):
        if root_dir is None:
            root_dir = "/"
        depth = int(depth)
        if depth > 5:
            logger.warning("[NFS] Limiting depth to maximum value of 5: "
                           "input depth=%s", depth)
            depth = 5
        root_dir = '{}/'.format(root_dir) \
                   if not root_dir.endswith('/') else root_dir

        try:
            cfs = CephFS()
            paths = cfs.get_dir_list(root_dir, depth)
            paths = [p[:-1] for p in paths if p != root_dir]
            return {'paths': paths}
        except (cephfs.ObjectNotFound, cephfs.PermissionError):
            return {'paths': []}

    @Endpoint('GET', '/cephfs/filesystems')
    def filesystems(self):
        return CephFS.list_filesystems()

    @Endpoint('GET', '/rgw/buckets')
    def buckets(self, user_id=None):
        return RgwClient.instance(user_id).get_buckets()

    @Endpoint('GET', '/clusters')
    def clusters(self):
        return Ganesha.get_ganesha_clusters()
