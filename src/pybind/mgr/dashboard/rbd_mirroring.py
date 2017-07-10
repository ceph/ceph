
import json
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

        try:
            rbdctx = rbd.RBD()
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


        pools = sorted([
            {
                'name': pool_name,
                'mirror_mode': self.get_pool_mirror_mode(pool_name),
                'health_color': 'error',
                'health': 'Error'
            }
            for pool_name in pool_names
        ], key=lambda k: k['name'])

        return {
            'daemons': daemons,
            'pools' : pools,
            'image_error': [
                {'pool': 'images', 'name': 'foo', 'description': "couldn't blah"},
                {'pool': 'images', 'name': 'goo', 'description': "failed XYZ"}
            ],
            'image_syncing': [
                {'pool': 'images', 'name': 'foo', 'progress': 93},
                {'pool': 'images', 'name': 'goo', 'progress': 0}
            ],
            'image_ready': [
                {'pool': 'images', 'name': 'foo', 'state_color': 'info', 'state': 'Primary'},
                {'pool': 'images', 'name': 'goo', 'state_color': 'success', 'state': 'Mirroring'}
            ]
        }

    def get_pool_mirror_mode(self, pool_name):
        pool_datum = self.pool_data.get(pool_name, None)
        if pool_datum is None:
            pool_datum = PoolDatum(self._module, pool_name)
            self.pool_data[pool_name] = pool_datum

        status, value = pool_datum.get()
        if value is None:
            return None
        return value.get('mirror_mode', None)

class Controller:
    def __init__(self, module_inst):
        self.daemons = Daemons(module_inst)
        self.pool_data = {}
        self.toplevel = Toplevel(module_inst, self.daemons)
        self.content_data = ContentData(module_inst, self.daemons,
                                        self.pool_data)

