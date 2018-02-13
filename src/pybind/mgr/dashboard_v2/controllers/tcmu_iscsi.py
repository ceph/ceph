# -*- coding: utf-8 -*-
from __future__ import absolute_import

from ..tools import ApiController, AuthRequired, RESTController

SERVICE_TYPE = 'tcmu-runner'


@ApiController('tcmuiscsi')
@AuthRequired()
class TcmuIscsi(RESTController):
    # pylint: disable=too-many-locals,too-many-nested-blocks
    def list(self):  # pylint: disable=unused-argument
        daemons = {}
        images = {}
        for server in self.mgr.list_servers():
            for service in server['services']:
                if service['type'] == SERVICE_TYPE:
                    metadata = self.mgr.get_metadata(SERVICE_TYPE,
                                                     service['id'])
                    status = self.mgr.get_daemon_status(SERVICE_TYPE,
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
                        lock_acquired_time = (self.mgr.get_counter(
                            'tcmu-runner', service_id, perf_key)[perf_key] or
                                              [[0, 0]])[-1][1] / 1000000000
                        if lock_acquired_time > image.get('optimized_since', 0):
                            image['optimized_since'] = lock_acquired_time
                            image['stats'] = {}
                            image['stats_history'] = {}
                            for s in ['rd', 'wr', 'rd_bytes', 'wr_bytes']:
                                perf_key = "{}{}".format(perf_key_prefix, s)
                                image['stats'][s] = self.mgr.get_rate(
                                    'tcmu-runner', service_id, perf_key)
                                image['stats_history'][s] = self.mgr.get_counter(
                                    'tcmu-runner', service_id, perf_key)[perf_key]
                    else:
                        daemon['non_optimized_paths'] += 1
                        image['non_optimized_paths'].append(server['hostname'])

        return {
            'daemons': sorted(daemons.values(), key=lambda d: d['server_hostname']),
            'images': sorted(images.values(), key=lambda i: ['id']),
        }
