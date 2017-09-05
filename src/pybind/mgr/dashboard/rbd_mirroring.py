
import json
import re
import rados
import rbd
from remote_view_cache import RemoteViewCache

class DaemonsAndPools(RemoteViewCache):
    def _get(self):
        daemons = self.get_daemons()
        return {
            'daemons': daemons,
            'pools': self.get_pools(daemons)
        }

    def get_daemons(self):
        daemons = []
        for server in self._module.list_servers():
            for service in server['services']:
                if service['type'] == 'rbd-mirror':
                    metadata = self._module.get_metadata('rbd-mirror',
                                                         service['id'])
                    status = self._module.get_daemon_status('rbd-mirror',
                                                            service['id'])
                    try:
                        status = json.loads(status['json'])
                    except:
                        status = {}

                    # extract per-daemon service data and health
                    daemon = {
                        'id': service['id'],
                        'instance_id': metadata['instance_id'],
                        'version': metadata['ceph_version'],
                        'server_hostname': server['hostname'],
                        'service': service,
                        'server': server,
                        'metadata': metadata,
                        'status': status
                    }
                    daemon = dict(daemon, **self.get_daemon_health(daemon))
                    daemons.append(daemon)

        return sorted(daemons, key=lambda k: k['id'])

    def get_daemon_health(self, daemon):
        health = {
            'health_color': 'info',
            'health' : 'Unknown'
        }
        for pool_id, pool_data in daemon['status'].items():
            if (health['health'] != 'error' and
                [k for k,v in pool_data.get('callouts', {}).items() if v['level'] == 'error']):
                health = {
                    'health_color': 'error',
                    'health': 'Error'
                }
            elif (health['health'] != 'error' and
                  [k for k,v in pool_data.get('callouts', {}).items() if v['level'] == 'warning']):
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

    def get_pools(self, daemons):
        status, pool_names = self._module.rbd_pool_ls.get()
        if pool_names is None:
            self.log.warning("Failed to get RBD pool list")
            return {}

        pool_stats = {}
        rbdctx = rbd.RBD()
        for pool_name in pool_names:
            self.log.debug("Constructing IOCtx " + pool_name)
            try:
                ioctx = self._module.rados.open_ioctx(pool_name)
            except:
                self.log.exception("Failed to open pool " + pool_name)
                continue

            try:
                mirror_mode = rbdctx.mirror_mode_get(ioctx)
            except:
                self.log.exception("Failed to query mirror mode " + pool_name)

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
            for pool_id, pool_data in daemon['status'].items():
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

        for name, stats in pool_stats.items():
            if stats.get('health', None) is None:
                # daemon doesn't know about pool
                stats['health_color'] = 'error'
                stats['health'] = 'Error'
            elif stats.get('leader_id', None) is None:
                # no daemons are managing the pool as leader instance
                stats['health_color'] = 'warning'
                stats['health'] = 'Warning'
        return pool_stats


class PoolDatum(RemoteViewCache):
    def __init__(self, module_inst, pool_name):
        super(PoolDatum, self).__init__(module_inst)
        self.pool_name = pool_name

    def _get(self):
        data = {}
        self.log.debug("Constructing IOCtx " + self.pool_name)
        try:
            ioctx = self._module.rados.open_ioctx(self.pool_name)
        except:
            self.log.exception("Failed to open pool " + pool_name)
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
        except:
            self.log.exception("Failed to list mirror image status " + self.pool_name)

        return data

class Toplevel(RemoteViewCache):
    def __init__(self, module_inst, daemons_and_pools):
        super(Toplevel, self).__init__(module_inst)
        self.daemons_and_pools = daemons_and_pools

    def _get(self):
        status, data = self.daemons_and_pools.get()
        if data is None:
            self.log.warning("Failed to get rbd-mirror daemons and pools")
            daemons = {}
        daemons = data.get('daemons', [])
        pools = data.get('pools', {})

        warnings = 0
        errors = 0
        for daemon in daemons:
            if daemon['health_color'] == 'error':
                errors += 1
            elif daemon['health_color'] == 'warning':
                warnings += 1
        for pool_name, pool in pools.items():
            if pool['health_color'] == 'error':
                errors += 1
            elif pool['health_color'] == 'warning':
                warnings += 1
        return {'warnings': warnings, 'errors': errors}


class ContentData(RemoteViewCache):
    def __init__(self, module_inst, daemons_and_pools, pool_data):
        super(ContentData, self).__init__(module_inst)

        self.daemons_and_pools = daemons_and_pools
        self.pool_data = pool_data

    def _get(self):
        status, pool_names = self._module.rbd_pool_ls.get()
        if pool_names is None:
            self.log.warning("Failed to get RBD pool list")
            return None

        status, data = self.daemons_and_pools.get()
        if data is None:
            self.log.warning("Failed to get rbd-mirror daemons list")
            data = {}
        daemons = data.get('daemons', [])
        pool_stats = data.get('pools', {})

        pools = []
        image_error = []
        image_syncing = []
        image_ready = []
        for pool_name in pool_names:
            pool = self.get_pool_datum(pool_name) or {}
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
            'pools' : pools,
            'image_error': image_error,
            'image_syncing': image_syncing,
            'image_ready': image_ready
        }

    def get_pool_datum(self, pool_name):
        pool_datum = self.pool_data.get(pool_name, None)
        if pool_datum is None:
            pool_datum = PoolDatum(self._module, pool_name)
            self.pool_data[pool_name] = pool_datum

        status, value = pool_datum.get()
        return value

class Controller:
    def __init__(self, module_inst):
        self.daemons_and_pools = DaemonsAndPools(module_inst)
        self.pool_data = {}
        self.toplevel = Toplevel(module_inst, self.daemons_and_pools)
        self.content_data = ContentData(module_inst, self.daemons_and_pools,
                                        self.pool_data)

