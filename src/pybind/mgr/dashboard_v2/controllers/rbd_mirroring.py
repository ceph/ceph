# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
import re

from functools import partial

import cherrypy
import rbd

from .. import logger, mgr
from ..services.ceph_service import CephService
from ..tools import ApiController, AuthRequired, BaseController, ViewCache


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
        for _, pool_data in daemon['status'].items():  # TODO: simplify
            if (health['health'] != 'error' and
                    [k for k, v in pool_data.get('callouts', {}).items()
                     if v['level'] == 'error']):
                health = {
                    'health_color': 'error',
                    'health': 'Error'
                }
            elif (health['health'] != 'error' and
                  [k for k, v in pool_data.get('callouts', {}).items()
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
        pool_names = [pool['pool_name'] for pool in CephService.get_pool_list('rbd')]
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
            except:  # noqa pylint: disable=W0702
                logger.exception("Failed to query mirror mode %s", pool_name)

            stats = {}
            if mirror_mode == rbd.RBD_MIRROR_MODE_DISABLED:
                continue
            elif mirror_mode == rbd.RBD_MIRROR_MODE_IMAGE:
                mirror_mode = "image"
            elif mirror_mode == rbd.RBD_MIRROR_MODE_POOL:
                mirror_mode = "pool"
            else:
                mirror_mode = "unknown"
                stats['health_color'] = "warning"
                stats['health'] = "Warning"

            pool_stats[pool_name] = dict(stats, **{
                'mirror_mode': mirror_mode
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

                if (stats.get('health_color', '') != 'error' and
                        pool_data.get('image_error_count', 0) > 0):
                    stats['health_color'] = 'error'
                    stats['health'] = 'Error'
                elif (stats.get('health_color', '') != 'error' and
                      pool_data.get('image_warning_count', 0) > 0):
                    stats['health_color'] = 'warning'
                    stats['health'] = 'Warning'
                elif stats.get('health', None) is None:
                    stats['health_color'] = 'success'
                    stats['health'] = 'OK'

        for _, stats in pool_stats.items():
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


@ApiController('rbdmirror')
@AuthRequired()
class RbdMirror(BaseController):

    def __init__(self):
        self.pool_data = {}

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def default(self):
        status, content_data = self._get_content_data()
        return {'status': status, 'content_data': content_data}

    @ViewCache()
    def _get_pool_datum(self, pool_name):
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

        return data

    @ViewCache()
    def _get_content_data(self):  # pylint: disable=R0914

        def get_pool_datum(pool_name):
            pool_datum = self.pool_data.get(pool_name, None)
            if pool_datum is None:
                pool_datum = partial(self._get_pool_datum, pool_name)
                self.pool_data[pool_name] = pool_datum

            _, value = pool_datum()
            return value

        pool_names = [pool['pool_name'] for pool in CephService.get_pool_list('rbd')]
        _, data = get_daemons_and_pools()
        if isinstance(data, Exception):
            logger.exception("Failed to get rbd-mirror daemons list")
            raise type(data)(str(data))
        daemons = data.get('daemons', [])
        pool_stats = data.get('pools', {})

        pools = []
        image_error = []
        image_syncing = []
        image_ready = []
        for pool_name in pool_names:
            pool = get_pool_datum(pool_name) or {}
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
