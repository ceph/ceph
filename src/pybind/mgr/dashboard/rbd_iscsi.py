
import rados
import rbd
from remote_view_cache import RemoteViewCache

SERVICE_TYPE = 'tcmu-runner'

class DaemonsAndImages(RemoteViewCache):
    def _get(self):
        daemons = {}
        images = {}
        for server in self._module.list_servers():
            for service in server['services']:
                if service['type'] == SERVICE_TYPE:
                    metadata = self._module.get_metadata(SERVICE_TYPE,
                                                         service['id'])
                    status = self._module.get_daemon_status(SERVICE_TYPE,
                                                            service['id'])

                    daemon = daemons.get(server['hostname'], None)
                    if daemon is None:
                        daemon = {
                            'server_hostname': server['hostname'],
                            'version': metadata['ceph_version'],
                            'optimized_paths': 0,
                            'non_optimized_paths': 0
                        }
                        daemons[server['hostname']] = daemon

                    image = images.get(service['id'])
                    if image is None:
                        image = {
                            'id': service['id'],
                            'pool_name': metadata['pool_name'],
                            'name': metadata['image_name'],
                            'optimized_paths': [],
                            'non_optimized_paths': []
                        }
                        if status.get('lock_owner', 'false') == 'true':
                            daemon['optimized_paths'] += 1
                            image['optimized_paths'].append(server['hostname'])
                        else:
                            daemon['non_optimized_paths'] += 1
                            image['non_optimized_paths'].append(server['hostname'])
                        images[service['id']] = image

        return {
            'daemons': [daemons[k] for k in sorted(daemons, key=daemons.get)],
            'images': [images[k] for k in sorted(images, key=images.get)]
        }

class Controller:
    def __init__(self, module_inst):
        self.content_data = DaemonsAndImages(module_inst)
