# -*- coding: utf-8 -*-

import json
import logging
import re
from enum import IntEnum
from functools import partial
from typing import Any, Dict, NamedTuple, Optional, no_type_check

import cherrypy
import rbd

from .. import mgr
from ..controllers.service import Service
from ..security import Scope
from ..services.ceph_service import CephService
from ..services.exception import handle_rados_error, handle_rbd_error, serialize_dashboard_exception
from ..services.orchestrator import OrchClient
from ..services.rbd import rbd_call
from ..tools import ViewCache
from . import APIDoc, APIRouter, BaseController, CreatePermission, Endpoint, \
    EndpointDoc, ReadPermission, RESTController, Task, UIRouter, \
    UpdatePermission, allow_empty_body

logger = logging.getLogger('controllers.rbd_mirror')


class MirrorHealth(IntEnum):
    # RBD defined mirroring health states in in src/tools/rbd/action/MirrorPool.cc where the order
    # is relevant.
    MIRROR_HEALTH_OK = 0
    MIRROR_HEALTH_UNKNOWN = 1
    MIRROR_HEALTH_WARNING = 2
    MIRROR_HEALTH_ERROR = 3

    # extra states for the dashboard
    MIRROR_HEALTH_DISABLED = 4
    MIRROR_HEALTH_INFO = 5

# pylint: disable=not-callable


def handle_rbd_mirror_error():
    def composed_decorator(func):
        func = handle_rados_error('rbd-mirroring')(func)
        return handle_rbd_error()(func)
    return composed_decorator


# pylint: disable=not-callable
def RbdMirroringTask(name, metadata, wait_for):  # noqa: N802
    def composed_decorator(func):
        func = handle_rbd_mirror_error()(func)
        return Task("rbd/mirroring/{}".format(name), metadata, wait_for,
                    partial(serialize_dashboard_exception, include_http_status=True))(func)
    return composed_decorator


def get_daemons():
    daemons = []
    for hostname, server in CephService.get_service_map('rbd-mirror').items():
        for service in server['services']:
            id = service['id']  # pylint: disable=W0622
            metadata = service['metadata']
            status = service['status'] or {}

            try:
                status = json.loads(status['json'])
            except (ValueError, KeyError):
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
        'health': MirrorHealth.MIRROR_HEALTH_DISABLED
    }
    for _, pool_data in daemon['status'].items():
        if (health['health'] != MirrorHealth.MIRROR_HEALTH_ERROR
                and [k for k, v in pool_data.get('callouts', {}).items()
                     if v['level'] == 'error']):
            health = {
                'health': MirrorHealth.MIRROR_HEALTH_ERROR
            }
        elif (health['health'] != MirrorHealth.MIRROR_HEALTH_ERROR
                and [k for k, v in pool_data.get('callouts', {}).items()
                     if v['level'] == 'warning']):
            health = {
                'health': MirrorHealth.MIRROR_HEALTH_WARNING
            }
        elif health['health'] == MirrorHealth.MIRROR_HEALTH_DISABLED:
            health = {
                'health': MirrorHealth.MIRROR_HEALTH_OK
            }
    return health


def get_pools(daemons):  # pylint: disable=R0912, R0915
    pool_names = [pool['pool_name'] for pool in CephService.get_pool_list('rbd')
                  if pool.get('type', 1) == 1]
    pool_stats = _get_pool_stats(pool_names)
    _update_pool_stats(daemons, pool_stats)
    return pool_stats


def transform_mirror_health(stat):
    health = 'OK'
    health_color = 'success'
    if stat['health'] == MirrorHealth.MIRROR_HEALTH_ERROR:
        health = 'Error'
        health_color = 'error'
    elif stat['health'] == MirrorHealth.MIRROR_HEALTH_WARNING:
        health = 'Warning'
        health_color = 'warning'
    elif stat['health'] == MirrorHealth.MIRROR_HEALTH_UNKNOWN:
        health = 'Unknown'
        health_color = 'warning'
    elif stat['health'] == MirrorHealth.MIRROR_HEALTH_OK:
        health = 'OK'
        health_color = 'success'
    elif stat['health'] == MirrorHealth.MIRROR_HEALTH_DISABLED:
        health = 'Disabled'
        health_color = 'info'
    stat['health'] = health
    stat['health_color'] = health_color


def _update_pool_stats(daemons, pool_stats):
    _update_pool_stats_with_daemons(daemons, pool_stats)
    for pool_stat in pool_stats.values():
        transform_mirror_health(pool_stat)


def _update_pool_stats_with_daemons(daemons, pool_stats):
    for daemon in daemons:
        for _, pool_data in daemon['status'].items():
            pool_stat = pool_stats.get(pool_data['name'], None)  # type: ignore
            if pool_stat is None:
                continue

            if pool_data.get('leader', False):
                # leader instance stores image counts
                pool_stat['leader_id'] = daemon['metadata']['instance_id']
                pool_stat['image_local_count'] = pool_data.get('image_local_count', 0)
                pool_stat['image_remote_count'] = pool_data.get('image_remote_count', 0)

            pool_stat['health'] = max(pool_stat['health'], daemon['health'])


def _get_pool_stats(pool_names):
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
            stats['health'] = MirrorHealth.MIRROR_HEALTH_DISABLED
        elif mirror_mode == rbd.RBD_MIRROR_MODE_IMAGE:
            mirror_mode = "image"
        elif mirror_mode == rbd.RBD_MIRROR_MODE_POOL:
            mirror_mode = "pool"
        else:
            mirror_mode = "unknown"

        if mirror_mode != "disabled":
            # In case of a pool being enabled we will infer the health like the RBD cli tool does
            # in src/tools/rbd/action/MirrorPool.cc::execute_status
            mirror_image_health: MirrorHealth = MirrorHealth.MIRROR_HEALTH_OK
            for status, _ in rbdctx.mirror_image_status_summary(ioctx):
                if (mirror_image_health < MirrorHealth.MIRROR_HEALTH_WARNING
                    and status != rbd.MIRROR_IMAGE_STATUS_STATE_REPLAYING
                        and status != rbd.MIRROR_IMAGE_STATUS_STATE_STOPPED):
                    mirror_image_health = MirrorHealth.MIRROR_HEALTH_WARNING
                if (mirror_image_health < MirrorHealth.MIRROR_HEALTH_ERROR
                        and status == rbd.MIRROR_IMAGE_STATUS_STATE_ERROR):
                    mirror_image_health = MirrorHealth.MIRROR_HEALTH_ERROR
            stats['health'] = mirror_image_health

        pool_stats[pool_name] = dict(stats, **{
            'mirror_mode': mirror_mode,
            'peer_uuids': peer_uuids
        })
    return pool_stats


@ViewCache()
def get_daemons_and_pools():  # pylint: disable=R0915
    daemons = get_daemons()
    daemons_and_pools = {
        'daemons': daemons,
        'pools': get_pools(daemons)
    }
    for daemon in daemons:
        transform_mirror_health(daemon)
    return daemons_and_pools


class ReplayingData(NamedTuple):
    bytes_per_second: Optional[int] = None
    seconds_until_synced: Optional[int] = None
    syncing_percent: Optional[float] = None
    entries_behind_primary: Optional[int] = None


def _get_mirror_mode(ioctx, image_name):
    with rbd.Image(ioctx, image_name) as img:
        mirror_mode = img.mirror_image_get_mode()
        mirror_mode_str = 'Disabled'
        if mirror_mode == rbd.RBD_MIRROR_IMAGE_MODE_JOURNAL:
            mirror_mode_str = 'journal'
        elif mirror_mode == rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT:
            mirror_mode_str = 'snapshot'
        return mirror_mode_str


@ViewCache()
@no_type_check
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
            'health': 'syncing',
            'state_color': 'success',
            'state': 'Syncing'
        },
        rbd.MIRROR_IMAGE_STATUS_STATE_STARTING_REPLAY: {
            'health': 'syncing',
            'state_color': 'success',
            'state': 'Starting'
        },
        rbd.MIRROR_IMAGE_STATUS_STATE_REPLAYING: {
            'health': 'syncing',
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
            'state': 'Stopped'
        }

    }

    rbdctx = rbd.RBD()
    try:
        mirror_image_status = rbdctx.mirror_image_status_list(ioctx)
        data['mirror_images'] = sorted([
            dict({
                'name': image['name'],
                'description': image['description'],
                'mirror_mode': _get_mirror_mode(ioctx, image['name'])
            }, **mirror_state['down' if not image['up'] else image['state']])
            for image in mirror_image_status
        ], key=lambda k: k['name'])
    except rbd.ImageNotFound:
        pass
    except:  # noqa pylint: disable=W0702
        logger.exception("Failed to list mirror image status %s", pool_name)
        raise

    return data


def _update_syncing_image_data(mirror_image, image):
    if mirror_image['state'] == 'Replaying':
        p = re.compile("replaying, ({.*})")
        replaying_data = p.findall(mirror_image['description'])
        assert len(replaying_data) == 1
        replaying_data = json.loads(replaying_data[0])
        if 'replay_state' in replaying_data and replaying_data['replay_state'] == 'idle':
            image.update({
                'state_color': 'info',
                'state': 'Idle'
            })
        for field in ReplayingData._fields:
            try:
                image[field] = replaying_data[field]
            except KeyError:
                pass
    else:
        p = re.compile("bootstrapping, IMAGE_COPY/COPY_OBJECT (.*)%")
        image.update({
            'progress': (p.findall(mirror_image['description']) or [0])[0]
        })


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
                'name': mirror_image['name'],
                'state_color': mirror_image['state_color'],
                'state': mirror_image['state'],
                'mirror_mode': mirror_image['mirror_mode']
            }

            if mirror_image['health'] == 'ok':
                image.update({
                    'description': mirror_image['description']
                })
                image_ready.append(image)
            elif mirror_image['health'] == 'syncing':
                _update_syncing_image_data(mirror_image, image)
                image_syncing.append(image)
            else:
                image.update({
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


RBD_MIRROR_SCHEMA = {
    "site_name": (str, "Site Name")
}

RBDM_POOL_SCHEMA = {
    "mirror_mode": (str, "Mirror Mode")
}

RBDM_SUMMARY_SCHEMA = {
    "site_name": (str, "site name"),
    "status": (int, ""),
    "content_data": ({
        "daemons": ([str], ""),
        "pools": ([{
            "name": (str, "Pool name"),
            "health_color": (str, ""),
            "health": (str, "pool health"),
            "mirror_mode": (str, "status"),
            "peer_uuids": ([str], "")
        }], "Pools"),
        "image_error": ([str], ""),
        "image_syncing": ([str], ""),
        "image_ready": ([str], "")
    }, "")
}


@APIRouter('/block/mirroring', Scope.RBD_MIRRORING)
@APIDoc("RBD Mirroring Management API", "RbdMirroring")
class RbdMirroring(BaseController):

    @Endpoint(method='GET', path='site_name')
    @handle_rbd_mirror_error()
    @ReadPermission
    @EndpointDoc("Display Rbd Mirroring sitename",
                 responses={200: RBD_MIRROR_SCHEMA})
    def get(self):
        return self._get_site_name()

    @Endpoint(method='PUT', path='site_name')
    @handle_rbd_mirror_error()
    @UpdatePermission
    def set(self, site_name):
        rbd.RBD().mirror_site_name_set(mgr.rados, site_name)
        return self._get_site_name()

    def _get_site_name(self):
        return {'site_name': rbd.RBD().mirror_site_name_get(mgr.rados)}


@APIRouter('/block/mirroring/summary', Scope.RBD_MIRRORING)
@APIDoc("RBD Mirroring Summary Management API", "RbdMirroringSummary")
class RbdMirroringSummary(BaseController):

    @Endpoint()
    @handle_rbd_mirror_error()
    @ReadPermission
    @EndpointDoc("Display Rbd Mirroring Summary",
                 responses={200: RBDM_SUMMARY_SCHEMA})
    def __call__(self):
        site_name = rbd.RBD().mirror_site_name_get(mgr.rados)

        status, content_data = _get_content_data()
        return {'site_name': site_name,
                'status': status,
                'content_data': content_data}


@APIRouter('/block/mirroring/pool', Scope.RBD_MIRRORING)
@APIDoc("RBD Mirroring Pool Mode Management API", "RbdMirroringPoolMode")
class RbdMirroringPoolMode(RESTController):

    RESOURCE_ID = "pool_name"
    MIRROR_MODES = {
        rbd.RBD_MIRROR_MODE_DISABLED: 'disabled',
        rbd.RBD_MIRROR_MODE_IMAGE: 'image',
        rbd.RBD_MIRROR_MODE_POOL: 'pool'
    }

    @handle_rbd_mirror_error()
    @EndpointDoc("Display Rbd Mirroring Summary",
                 parameters={
                     'pool_name': (str, 'Pool Name'),
                 },
                 responses={200: RBDM_POOL_SCHEMA})
    def get(self, pool_name):
        ioctx = mgr.rados.open_ioctx(pool_name)
        mode = rbd.RBD().mirror_mode_get(ioctx)
        data = {
            'mirror_mode': self.MIRROR_MODES.get(mode, 'unknown')
        }
        return data

    @RbdMirroringTask('pool/edit', {'pool_name': '{pool_name}'}, 5.0)
    def set(self, pool_name, mirror_mode=None):
        return self.set_pool_mirror_mode(pool_name, mirror_mode)

    def set_pool_mirror_mode(self, pool_name, mirror_mode):
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

        return rbd_call(pool_name, None, _edit, mirror_mode)


@APIRouter('/block/mirroring/pool/{pool_name}/bootstrap', Scope.RBD_MIRRORING)
@APIDoc("RBD Mirroring Pool Bootstrap Management API", "RbdMirroringPoolBootstrap")
class RbdMirroringPoolBootstrap(BaseController):

    @Endpoint(method='POST', path='token')
    @handle_rbd_mirror_error()
    @UpdatePermission
    @allow_empty_body
    def create_token(self, pool_name):
        ioctx = mgr.rados.open_ioctx(pool_name)
        token = rbd.RBD().mirror_peer_bootstrap_create(ioctx)
        return {'token': token}

    @Endpoint(method='POST', path='peer')
    @handle_rbd_mirror_error()
    @UpdatePermission
    @allow_empty_body
    def import_token(self, pool_name, direction, token):
        ioctx = mgr.rados.open_ioctx(pool_name)

        directions = {
            'rx': rbd.RBD_MIRROR_PEER_DIRECTION_RX,
            'rx-tx': rbd.RBD_MIRROR_PEER_DIRECTION_RX_TX
        }

        direction_enum = directions.get(direction)
        if direction_enum is None:
            raise rbd.Error('invalid direction "{}"'.format(direction))

        rbd.RBD().mirror_peer_bootstrap_import(ioctx, direction_enum, token)
        return {}


@APIRouter('/block/mirroring/pool/{pool_name}/peer', Scope.RBD_MIRRORING)
@APIDoc("RBD Mirroring Pool Peer Management API", "RbdMirroringPoolPeer")
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

        # convert direction enum to string
        directions = {
            rbd.RBD_MIRROR_PEER_DIRECTION_RX: 'rx',
            rbd.RBD_MIRROR_PEER_DIRECTION_TX: 'tx',
            rbd.RBD_MIRROR_PEER_DIRECTION_RX_TX: 'rx-tx'
        }
        peer['direction'] = directions[peer.get('direction', rbd.RBD_MIRROR_PEER_DIRECTION_RX)]

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


@UIRouter('/block/mirroring', Scope.RBD_MIRRORING)
class RbdMirroringStatus(BaseController):
    @EndpointDoc('Display RBD Mirroring Status')
    @Endpoint()
    @ReadPermission
    def status(self):
        status: Dict[str, Any] = {'available': True, 'message': None}
        orch_status = OrchClient.instance().status()

        # if the orch is not available we can't create the service
        # using dashboard.
        if not orch_status['available']:
            return status
        if not CephService.get_service_list('rbd-mirror') and not CephService.get_pool_list('rbd'):
            status['available'] = False
            status['message'] = 'No default "rbd" pool or "rbd-mirror" service ' \
                                'in the cluster. Please click on ' \
                                '"Configure Block Mirroring" ' \
                                'button to get started.'  # type: ignore
        return status

    @Endpoint('POST')
    @EndpointDoc('Configure RBD Mirroring')
    @CreatePermission
    def configure(self):
        from ..controllers.pool import RBDPool  # to avoid circular import

        rbd_pool = RBDPool()
        service = Service()

        service_spec = {
            'service_type': 'rbd-mirror',
            'placement': {},
            'unmanaged': False
        }

        if not CephService.get_service_list('rbd-mirror'):
            service.create(service_spec, 'rbd-mirror')

        if not CephService.get_pool_list('rbd'):
            rbd_pool.create()
