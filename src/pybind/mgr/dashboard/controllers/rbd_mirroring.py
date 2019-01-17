# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
import re

from functools import partial

import cherrypy

import rbd

from . import ApiController, Endpoint, Task, BaseController, ReadPermission, \
    RESTController
from .. import logger, mgr
from ..security import Scope
from ..services.ceph_service import CephService
from ..tools import ViewCache
from ..services.exception import handle_rados_error, handle_rbd_error, \
    serialize_dashboard_exception


# pylint: disable=not-callable
def handle_rbd_mirror_error():
    def composed_decorator(func):
        func = handle_rados_error('rbd-mirroring')(func)
        return handle_rbd_error()(func)
    return composed_decorator


# pylint: disable=not-callable
def RbdMirroringTask(name, metadata, wait_for):
    def composed_decorator(func):
        func = handle_rbd_mirror_error()(func)
        return Task("rbd/mirroring/{}".format(name), metadata, wait_for,
                    partial(serialize_dashboard_exception, include_http_status=True))(func)
    return composed_decorator


def _rbd_call(pool_name, func, *args, **kwargs):
    with mgr.rados.open_ioctx(pool_name) as ioctx:
        func(ioctx, *args, **kwargs)


@ViewCache()
def get_daemons_and_pools():  # pylint: disable=R0915
    def get_daemons():
        daemons = []
        for hostname, server in CephService.get_service_map('rbd-mirror').items():
            for service in server['services']:
                id = service['id']  # pylint: disable=W0622
                metadata = service['metadata']
                status = service['status']

                try:
                    status = json.loads(status['json'])
                except (ValueError, KeyError) as _:
                    status = {}

                instance_id = metadata['instance_id']
                if id == instance_id:
                    # new version that supports per-cluster leader elections
                    id = metadata['id']

                # extract per-daemon service data and health
                daemon = {
                    'id': id,
                    'instance_id': instance_id,
                    'version': metadata['ceph_version'],
                    'server_hostname': hostname,
                    'service': service,
                    'server': server,
                    'metadata': metadata,
                    'status': status
                }
                daemon = dict(daemon, **get_daemon_health(daemon))
                daemons.append(daemon)

        return sorted(daemons, key=lambda k: k['instance_id'])

    def get_daemon_health(daemon):
        health = {
            'health_color': 'info',
            'health': 'Unknown'
        }
        for _, pool_data in daemon['status'].items():
            if (health['health'] != 'error'
                    and [k for k, v in pool_data.get('callouts', {}).items()
                         if v['level'] == 'error']):
                health = {
                    'health_color': 'error',
                    'health': 'Error'
                }
            elif (health['health'] != 'error'
                  and [k for k, v in pool_data.get('callouts', {}).items()
                       if v['level'] == 'warning']):
                health = {
                    'health_color': 'warning',
                    'health': 'Warning'
                }
            elif health['health_color'] == 'info':
                health = {
                    'health_color': 'success',
                    'health': 'OK'
                }
        return health

    def get_pools(daemons):  # pylint: disable=R0912, R0915
        pool_names = [pool['pool_name'] for pool in CephService.get_pool_list('rbd')
                      if pool.get('type', 1) == 1]
        pool_stats = {}
        rbdctx = rbd.RBD()
        for pool_name in pool_names:
            logger.debug("Constructing IOCtx %s", pool_name)
            try:
                ioctx = mgr.rados.open_ioctx(pool_name)
            except TypeError:
                logger.exception("Failed to open pool %s", pool_name)
                continue

            try:
                mirror_mode = rbdctx.mirror_mode_get(ioctx)
                peer_uuids = [x['uuid'] for x in rbdctx.mirror_peer_list(ioctx)]
            except:  # noqa pylint: disable=W0702
                logger.exception("Failed to query mirror settings %s", pool_name)
                mirror_mode = None
                peer_uuids = []

            stats = {}
            if mirror_mode == rbd.RBD_MIRROR_MODE_DISABLED:
                mirror_mode = "disabled"
                stats['health_color'] = "info"
                stats['health'] = "Disabled"
            elif mirror_mode == rbd.RBD_MIRROR_MODE_IMAGE:
                mirror_mode = "image"
            elif mirror_mode == rbd.RBD_MIRROR_MODE_POOL:
                mirror_mode = "pool"
            else:
                mirror_mode = "unknown"
                stats['health_color'] = "warning"
                stats['health'] = "Warning"

            pool_stats[pool_name] = dict(stats, **{
                'mirror_mode': mirror_mode,
                'peer_uuids': peer_uuids
            })

        for daemon in daemons:
            for _, pool_data in daemon['status'].items():
                stats = pool_stats.get(pool_data['name'], None)
                if stats is None:
                    continue

                if pool_data.get('leader', False):
                    # leader instance stores image counts
                    stats['leader_id'] = daemon['metadata']['instance_id']
                    stats['image_local_count'] = pool_data.get('image_local_count', 0)
                    stats['image_remote_count'] = pool_data.get('image_remote_count', 0)

                if (stats.get('health_color', '') != 'error'
                        and pool_data.get('image_error_count', 0) > 0):
                    stats['health_color'] = 'error'
                    stats['health'] = 'Error'
                elif (stats.get('health_color', '') != 'error'
                      and pool_data.get('image_warning_count', 0) > 0):
                    stats['health_color'] = 'warning'
                    stats['health'] = 'Warning'
                elif stats.get('health', None) is None:
                    stats['health_color'] = 'success'
                    stats['health'] = 'OK'

        for _, stats in pool_stats.items():
            if stats['mirror_mode'] == 'disabled':
                continue
            if stats.get('health', None) is None:
                # daemon doesn't know about pool
                stats['health_color'] = 'error'
                stats['health'] = 'Error'
            elif stats.get('leader_id', None) is None:
                # no daemons are managing the pool as leader instance
                stats['health_color'] = 'warning'
                stats['health'] = 'Warning'
        return pool_stats

    daemons = get_daemons()
    return {
        'daemons': daemons,
        'pools': get_pools(daemons)
    }


@ViewCache()
def _get_pool_datum(pool_name):
    data = {}
    logger.debug("Constructing IOCtx %s", pool_name)
    try:
        ioctx = mgr.rados.open_ioctx(pool_name)
    except TypeError:
        logger.exception("Failed to open pool %s", pool_name)
        return None

    mirror_state = {
        'down': {
            'health': 'issue',
            'state_color': 'warning',
            'state': 'Unknown',
            'description': None
        },
        rbd.MIRROR_IMAGE_STATUS_STATE_UNKNOWN: {
            'health': 'issue',
            'state_color': 'warning',
            'state': 'Unknown'
        },
        rbd.MIRROR_IMAGE_STATUS_STATE_ERROR: {
            'health': 'issue',
            'state_color': 'error',
            'state': 'Error'
        },
        rbd.MIRROR_IMAGE_STATUS_STATE_SYNCING: {
            'health': 'syncing'
        },
        rbd.MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY: {
            'health': 'ok',
            'state_color': 'success',
            'state': 'Starting'
        },
        rbd.MIRROR_IMAGE_STATUS_STATE_REPLAYING: {
            'health': 'ok',
            'state_color': 'success',
            'state': 'Replaying'
        },
        rbd.MIRROR_IMAGE_STATUS_STATE_STOPPING_REPLAY: {
            'health': 'ok',
            'state_color': 'success',
            'state': 'Stopping'
        },
        rbd.MIRROR_IMAGE_STATUS_STATE_STOPPED: {
            'health': 'ok',
            'state_color': 'info',
            'state': 'Primary'
        }
    }

    rbdctx = rbd.RBD()
    try:
        mirror_image_status = rbdctx.mirror_image_status_list(ioctx)
        data['mirror_images'] = sorted([
            dict({
                'name': image['name'],
                'description': image['description']
            }, **mirror_state['down' if not image['up'] else image['state']])
            for image in mirror_image_status
        ], key=lambda k: k['name'])
    except rbd.ImageNotFound:
        pass
    except:  # noqa pylint: disable=W0702
        logger.exception("Failed to list mirror image status %s", pool_name)
        raise

    return data


@ViewCache()
def _get_content_data():  # pylint: disable=R0914
    pool_names = [pool['pool_name'] for pool in CephService.get_pool_list('rbd')
                  if pool.get('type', 1) == 1]
    _, data = get_daemons_and_pools()
    daemons = data.get('daemons', [])
    pool_stats = data.get('pools', {})

    pools = []
    image_error = []
    image_syncing = []
    image_ready = []
    for pool_name in pool_names:
        _, pool = _get_pool_datum(pool_name)
        if not pool:
            pool = {}

        stats = pool_stats.get(pool_name, {})
        if stats.get('mirror_mode', None) is None:
            continue

        mirror_images = pool.get('mirror_images', [])
        for mirror_image in mirror_images:
            image = {
                'pool_name': pool_name,
                'name': mirror_image['name']
            }

            if mirror_image['health'] == 'ok':
                image.update({
                    'state_color': mirror_image['state_color'],
                    'state': mirror_image['state'],
                    'description': mirror_image['description']
                })
                image_ready.append(image)
            elif mirror_image['health'] == 'syncing':
                p = re.compile("bootstrapping, IMAGE_COPY/COPY_OBJECT (.*)%")
                image.update({
                    'progress': (p.findall(mirror_image['description']) or [0])[0]
                })
                image_syncing.append(image)
            else:
                image.update({
                    'state_color': mirror_image['state_color'],
                    'state': mirror_image['state'],
                    'description': mirror_image['description']
                })
                image_error.append(image)

        pools.append(dict({
            'name': pool_name
        }, **stats))

    return {
        'daemons': daemons,
        'pools': pools,
        'image_error': image_error,
        'image_syncing': image_syncing,
        'image_ready': image_ready
    }


def _reset_view_cache():
    get_daemons_and_pools.reset()
    _get_pool_datum.reset()
    _get_content_data.reset()


@ApiController('/block/mirroring/summary', Scope.RBD_MIRRORING)
class RbdMirroringSummary(BaseController):

    @Endpoint()
    @handle_rbd_mirror_error()
    @ReadPermission
    def __call__(self):
        status, content_data = _get_content_data()
        return {'status': status, 'content_data': content_data}


@ApiController('/block/mirroring/pool', Scope.RBD_MIRRORING)
class RbdMirroringPoolMode(RESTController):

    RESOURCE_ID = "pool_name"
    MIRROR_MODES = {
        rbd.RBD_MIRROR_MODE_DISABLED: 'disabled',
        rbd.RBD_MIRROR_MODE_IMAGE: 'image',
        rbd.RBD_MIRROR_MODE_POOL: 'pool'
    }

    @handle_rbd_mirror_error()
    def get(self, pool_name):
        ioctx = mgr.rados.open_ioctx(pool_name)
        mode = rbd.RBD().mirror_mode_get(ioctx)
        data = {
            'mirror_mode': self.MIRROR_MODES.get(mode, 'unknown')
        }
        return data

    @RbdMirroringTask('pool/edit', {'pool_name': '{pool_name}'}, 5.0)
    def set(self, pool_name, mirror_mode=None):
        def _edit(ioctx, mirror_mode=None):
            if mirror_mode:
                mode_enum = {x[1]: x[0] for x in
                             self.MIRROR_MODES.items()}.get(mirror_mode, None)
                if mode_enum is None:
                    raise rbd.Error('invalid mirror mode "{}"'.format(mirror_mode))

                current_mode_enum = rbd.RBD().mirror_mode_get(ioctx)
                if mode_enum != current_mode_enum:
                    rbd.RBD().mirror_mode_set(ioctx, mode_enum)
                _reset_view_cache()

        return _rbd_call(pool_name, _edit, mirror_mode)


@ApiController('/block/mirroring/pool/{pool_name}/peer', Scope.RBD_MIRRORING)
class RbdMirroringPoolPeer(RESTController):

    RESOURCE_ID = "peer_uuid"

    @handle_rbd_mirror_error()
    def list(self, pool_name):
        ioctx = mgr.rados.open_ioctx(pool_name)
        peer_list = rbd.RBD().mirror_peer_list(ioctx)
        return [x['uuid'] for x in peer_list]

    @handle_rbd_mirror_error()
    def create(self, pool_name, cluster_name, client_id, mon_host=None,
               key=None):
        ioctx = mgr.rados.open_ioctx(pool_name)
        mode = rbd.RBD().mirror_mode_get(ioctx)
        if mode == rbd.RBD_MIRROR_MODE_DISABLED:
            raise rbd.Error('mirroring must be enabled')

        uuid = rbd.RBD().mirror_peer_add(ioctx, cluster_name,
                                         'client.{}'.format(client_id))

        attributes = {}
        if mon_host is not None:
            attributes[rbd.RBD_MIRROR_PEER_ATTRIBUTE_NAME_MON_HOST] = mon_host
        if key is not None:
            attributes[rbd.RBD_MIRROR_PEER_ATTRIBUTE_NAME_KEY] = key
        if attributes:
            rbd.RBD().mirror_peer_set_attributes(ioctx, uuid, attributes)

        _reset_view_cache()
        return {'uuid': uuid}

    @handle_rbd_mirror_error()
    def get(self, pool_name, peer_uuid):
        ioctx = mgr.rados.open_ioctx(pool_name)
        peer_list = rbd.RBD().mirror_peer_list(ioctx)
        peer = next((x for x in peer_list if x['uuid'] == peer_uuid), None)
        if not peer:
            raise cherrypy.HTTPError(404)

        # convert full client name to just the client id
        peer['client_id'] = peer['client_name'].split('.', 1)[-1]
        del peer['client_name']

        try:
            attributes = rbd.RBD().mirror_peer_get_attributes(ioctx, peer_uuid)
        except rbd.ImageNotFound:
            attributes = {}

        peer['mon_host'] = attributes.get(rbd.RBD_MIRROR_PEER_ATTRIBUTE_NAME_MON_HOST, '')
        peer['key'] = attributes.get(rbd.RBD_MIRROR_PEER_ATTRIBUTE_NAME_KEY, '')
        return peer

    @handle_rbd_mirror_error()
    def delete(self, pool_name, peer_uuid):
        ioctx = mgr.rados.open_ioctx(pool_name)
        rbd.RBD().mirror_peer_remove(ioctx, peer_uuid)
        _reset_view_cache()

    @handle_rbd_mirror_error()
    def set(self, pool_name, peer_uuid, cluster_name=None, client_id=None,
            mon_host=None, key=None):
        ioctx = mgr.rados.open_ioctx(pool_name)
        if cluster_name:
            rbd.RBD().mirror_peer_set_cluster(ioctx, peer_uuid, cluster_name)
        if client_id:
            rbd.RBD().mirror_peer_set_client(ioctx, peer_uuid,
                                             'client.{}'.format(client_id))

        if mon_host is not None or key is not None:
            try:
                attributes = rbd.RBD().mirror_peer_get_attributes(ioctx, peer_uuid)
            except rbd.ImageNotFound:
                attributes = {}

            if mon_host is not None:
                attributes[rbd.RBD_MIRROR_PEER_ATTRIBUTE_NAME_MON_HOST] = mon_host
            if key is not None:
                attributes[rbd.RBD_MIRROR_PEER_ATTRIBUTE_NAME_KEY] = key
            rbd.RBD().mirror_peer_set_attributes(ioctx, peer_uuid, attributes)

        _reset_view_cache()
