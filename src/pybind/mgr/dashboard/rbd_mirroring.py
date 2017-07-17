
import json
import re
import rados
import rbd
from remote_view_cache import RemoteViewCache

class Daemons(RemoteViewCache):
    def _get(self):
        daemons = []
        for server in self._module.list_servers():
            for service in server['services']:
                if service['type'] == 'rbd-mirror':
                    metadata = self._module.get_metadata('rbd-mirror',
                                                         service['id'])
                    try:
                        status = JSON.parse(metadata['json'])
                    except:
                        status = {}
                    daemons.append({
                        'service': service,
                        'server': server,
                        'metadata': metadata,
                        'status': status
                    })
        return daemons

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

        rbdctx = rbd.RBD()
        try:
            mirror_mode = rbdctx.mirror_mode_get(ioctx)
        except:
            self.log.exception("Failed to query mirror mode " + pool_name)
        if mirror_mode == rbd.RBD_MIRROR_MODE_IMAGE:
            mirror_mode = "image"
        elif mirror_mode == rbd.RBD_MIRROR_MODE_POOL:
            mirror_mode = "pool"
        else:
            mirror_mode = None
        data['mirror_mode'] = mirror_mode

        mirror_state = {
            rbd.MIRROR_IMAGE_STATUS_STATE_UNKNOWN: {
                'health': 'issue',
                'state_color': 'warning',
                'state': 'Unknown',
                'description': None
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

        try:
            mirror_image_status = rbdctx.mirror_image_status_list(ioctx)
            data['mirror_images'] = sorted([
                dict({
                    'name': image['name'],
                    'description': image['description']
                }, **mirror_state[rbd.MIRROR_IMAGE_STATUS_STATE_UNKNOWN if not image['up'] else image['state']])
                for image in mirror_image_status
            ], key=lambda k: k['name'])
        except:
            self.log.exception("Failed to list mirror image status " + self.pool_name)
            return None

        return data

class Toplevel(RemoteViewCache):
    def __init__(self, module_inst, daemons):
        super(Toplevel, self).__init__(module_inst)
        self.daemons = daemons

    def _get(self):
        return {'warnings': 2, 'errors': 1}

class ContentData(RemoteViewCache):
    def __init__(self, module_inst, daemons, pool_data):
        super(ContentData, self).__init__(module_inst)

        self.daemons = daemons
        self.pool_data = pool_data

    def _get(self):
        status, pool_names = self._module.rbd_pool_ls.get()
        if pool_names is None:
            log.warning("Failed to get RBD pool list")
            return None

        status, daemons = self.daemons.get()
        if daemons is None:
            log.warning("Failed to get rbd-mirror daemons list")
            daemons = []

        daemons = sorted([
            {
                'id': daemon['service']['id'],
                'instance_id': daemon['metadata']['instance_id'],
                'version': daemon['metadata']['ceph_version'],
                'server_hostname': daemon['server']['hostname'],
                'health_color': 'warning',
                'health': 'Warning'
            }
            for daemon in daemons
        ], key=lambda k: k['id'])

        pools = []
        image_error = []
        image_syncing = []
        image_ready = []
        for pool_name in pool_names:
            pool = self.get_pool_datum(pool_name)
            if pool is None:
                continue

            for mirror_image in pool['mirror_images']:
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

            pools.append({
                'name': pool_name,
                'mirror_mode': pool['mirror_mode'],
                'health_color': 'error',
                'health': 'Error'
            })


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
        self.daemons = Daemons(module_inst)
        self.pool_data = {}
        self.toplevel = Toplevel(module_inst, self.daemons)
        self.content_data = ContentData(module_inst, self.daemons,
                                        self.pool_data)

