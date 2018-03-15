# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .. import mgr
from ..services.ceph_service import CephService
from ..tools import ApiController, AuthRequired, RESTController

SERVICE_TYPE = 'tcmu-runner'


@ApiController('tcmuiscsi')
@AuthRequired()
class TcmuIscsi(RESTController):
    def _get_rate(self, daemon_type, daemon_name, stat):
        data = mgr.get_counter(daemon_type, daemon_name, stat)[stat]

        if data and len(data) > 1:
            return (data[-1][1] - data[-2][1]) / float(data[-1][0] - data[-2][0])
        return 0

    # pylint: disable=too-many-locals,too-many-nested-blocks
    def list(self):  # pylint: disable=unused-argument
        daemons = {}
        images = {}
        for service in CephService.get_service_list(SERVICE_TYPE):
            metadata = service['metadata']
            status = service['status']
            hostname = service['hostname']

            daemon = daemons.get(hostname, None)
            if daemon is None:
                daemon = {
                    'server_hostname': hostname,
                    'version': metadata['ceph_version'],
                    'optimized_paths': 0,
                    'non_optimized_paths': 0
                }
                daemons[hostname] = daemon

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
                image['optimized_paths'].append(hostname)

                perf_key_prefix = "librbd-{id}-{pool}-{name}.".format(
                    id=metadata.get('image_id', ''),
                    pool=metadata['pool_name'],
                    name=metadata['image_name'])
                perf_key = "{}lock_acquired_time".format(perf_key_prefix)
                lock_acquired_time = (mgr.get_counter(
                    'tcmu-runner', service_id, perf_key)[perf_key] or
                                      [[0, 0]])[-1][1] / 1000000000
                if lock_acquired_time > image.get('optimized_since', 0):
                    image['optimized_since'] = lock_acquired_time
                    image['stats'] = {}
                    image['stats_history'] = {}
                    for s in ['rd', 'wr', 'rd_bytes', 'wr_bytes']:
                        perf_key = "{}{}".format(perf_key_prefix, s)
                        image['stats'][s] = self._get_rate(
                            'tcmu-runner', service_id, perf_key)
                        image['stats_history'][s] = mgr.get_counter(
                            'tcmu-runner', service_id, perf_key)[perf_key]
            else:
                daemon['non_optimized_paths'] += 1
                image['non_optimized_paths'].append(hostname)

        return {
            'daemons': sorted(daemons.values(), key=lambda d: d['server_hostname']),
            'images': sorted(images.values(), key=lambda i: ['id']),
        }
