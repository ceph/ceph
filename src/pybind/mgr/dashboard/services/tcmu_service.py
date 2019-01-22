from dashboard.services.ceph_service import CephService
from .. import mgr

SERVICE_TYPE = 'tcmu-runner'


class TcmuService(object):
    # pylint: disable=too-many-nested-blocks
    @staticmethod
    def get_iscsi_info():
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
                    'tcmu-runner', service_id, perf_key)[perf_key]
                                      or [[0, 0]])[-1][1] / 1000000000
                if lock_acquired_time > image.get('optimized_since', 0):
                    image['optimized_daemon'] = hostname
                    image['optimized_since'] = lock_acquired_time
                    image['stats'] = {}
                    image['stats_history'] = {}
                    for s in ['rd', 'wr', 'rd_bytes', 'wr_bytes']:
                        perf_key = "{}{}".format(perf_key_prefix, s)
                        image['stats'][s] = CephService.get_rate(
                            'tcmu-runner', service_id, perf_key)
                        image['stats_history'][s] = CephService.get_rates(
                            'tcmu-runner', service_id, perf_key)
            else:
                daemon['non_optimized_paths'] += 1
                image['non_optimized_paths'].append(hostname)

        # clear up races w/ tcmu-runner clients that haven't detected
        # loss of optimized path
        for image in images.values():
            optimized_daemon = image.get('optimized_daemon', None)
            if optimized_daemon:
                for daemon_name in image['optimized_paths']:
                    if daemon_name != optimized_daemon:
                        daemon = daemons[daemon_name]
                        daemon['optimized_paths'] -= 1
                        daemon['non_optimized_paths'] += 1
                        image['non_optimized_paths'].append(daemon_name)
                image['optimized_paths'] = [optimized_daemon]

        return {
            'daemons': sorted(daemons.values(),
                              key=lambda d: d['server_hostname']),
            'images': sorted(images.values(), key=lambda i: ['id']),
        }

    @staticmethod
    def get_iscsi_daemons_amount():
        daemons = {}
        for service in CephService.get_service_list(SERVICE_TYPE):
            hostname = service['hostname']

            daemon = daemons.get(hostname, None)
            if daemon is None:
                daemons[hostname] = True

        return len(daemons)
