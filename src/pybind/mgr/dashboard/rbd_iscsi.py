from __future__ import absolute_import

import rados
import rbd
from .remote_view_cache import RemoteViewCache

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

                    service_id = service['id']
                    device_id = service_id.split(':')[-1]
                    image = images.get(device_id)
                    if image is None:
                        image = {
                            'device_id': device_id,
                            'pool_name': metadata['pool_name'],
                            'name': metadata['image_name'],
                            'id': metadata.get('image_id', None),
                            'optimized_paths': [],
                            'non_optimized_paths': []
                        }
                        images[device_id] = image
                    if status.get('lock_owner', 'false') == 'true':
                        daemon['optimized_paths'] += 1
                        image['optimized_paths'].append(server['hostname'])

                        perf_key_prefix = "librbd-{id}-{pool}-{name}.".format(
                            id=metadata.get('image_id', ''),
                            pool=metadata['pool_name'],
                            name=metadata['image_name'])
                        perf_key = "{}lock_acquired_time".format(perf_key_prefix)
                        lock_acquired_time = (self._module.get_counter(
                          'tcmu-runner', service_id, perf_key)[perf_key] or
                            [[0,0]])[-1][1] / 1000000000
                        if lock_acquired_time > image.get('optimized_since', None):
                            image['optimized_since'] = lock_acquired_time
                            image['stats'] = {}
                            image['stats_history'] = {}
                            for s in ['rd', 'wr', 'rd_bytes', 'wr_bytes']:
                                perf_key = "{}{}".format(perf_key_prefix, s)
                                image['stats'][s] = self._module.get_rate(
                                    'tcmu-runner', service_id, perf_key)
                                image['stats_history'][s] = self._module.get_counter(
                                    'tcmu-runner', service_id, perf_key)[perf_key]
                    else:
                        daemon['non_optimized_paths'] += 1
                        image['non_optimized_paths'].append(server['hostname'])

        return {
            'daemons': [daemons[k] for k in sorted(daemons, key=daemons.get)],
            'images': [images[k] for k in sorted(images, key=images.get)]
        }

class Controller:
    def __init__(self, module_inst):
        self.content_data = DaemonsAndImages(module_inst)
