# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json
import logging

import rados

from mgr_module import CommandResult
from mgr_util import get_time_series_rates, get_most_recent_rate

from .. import mgr
from ..exceptions import DashboardException

try:
    from typing import Dict, Any, Union  # pylint: disable=unused-import
except ImportError:
    pass  # For typing only

logger = logging.getLogger('ceph_service')


class SendCommandError(rados.Error):
    def __init__(self, err, prefix, argdict, errno):
        self.prefix = prefix
        self.argdict = argdict
        super(SendCommandError, self).__init__(err, errno)


class CephService(object):

    OSD_FLAG_NO_SCRUB = 'noscrub'
    OSD_FLAG_NO_DEEP_SCRUB = 'nodeep-scrub'

    PG_STATUS_SCRUBBING = 'scrubbing'
    PG_STATUS_DEEP_SCRUBBING = 'deep'

    SCRUB_STATUS_DISABLED = 'Disabled'
    SCRUB_STATUS_ACTIVE = 'Active'
    SCRUB_STATUS_INACTIVE = 'Inactive'

    @classmethod
    def get_service_map(cls, service_name):
        service_map = {}  # type: Dict[str, dict]
        for server in mgr.list_servers():
            for service in server['services']:
                if service['type'] == service_name:
                    if server['hostname'] not in service_map:
                        service_map[server['hostname']] = {
                            'server': server,
                            'services': []
                        }
                    inst_id = service['id']
                    metadata = mgr.get_metadata(service_name, inst_id)
                    status = mgr.get_daemon_status(service_name, inst_id)
                    service_map[server['hostname']]['services'].append({
                        'id': inst_id,
                        'type': service_name,
                        'hostname': server['hostname'],
                        'metadata': metadata,
                        'status': status
                    })
        return service_map

    @classmethod
    def get_service_list(cls, service_name):
        service_map = cls.get_service_map(service_name)
        return [svc for _, svcs in service_map.items() for svc in svcs['services']]

    @classmethod
    def get_service(cls, service_name, service_id):
        for server in mgr.list_servers():
            for service in server['services']:
                if service['type'] == service_name:
                    inst_id = service['id']
                    if inst_id == service_id:
                        metadata = mgr.get_metadata(service_name, inst_id)
                        status = mgr.get_daemon_status(service_name, inst_id)
                        return {
                            'id': inst_id,
                            'type': service_name,
                            'hostname': server['hostname'],
                            'metadata': metadata,
                            'status': status
                        }
        return None

    @classmethod
    def get_pool_list(cls, application=None):
        osd_map = mgr.get('osd_map')
        if not application:
            return osd_map['pools']
        return [pool for pool in osd_map['pools']
                if application in pool.get('application_metadata', {})]

    @classmethod
    def get_pool_list_with_stats(cls, application=None):
        # pylint: disable=too-many-locals
        pools = cls.get_pool_list(application)

        pools_w_stats = []

        pg_summary = mgr.get("pg_summary")
        pool_stats = mgr.get_updated_pool_stats()

        for pool in pools:
            pool['pg_status'] = pg_summary['by_pool'][pool['pool'].__str__()]
            stats = pool_stats[pool['pool']]
            s = {}

            for stat_name, stat_series in stats.items():
                rates = get_time_series_rates(stat_series)
                s[stat_name] = {
                    'latest': stat_series[0][1],
                    'rate': get_most_recent_rate(rates),
                    'rates': rates
                }
            pool['stats'] = s
            pools_w_stats.append(pool)
        return pools_w_stats

    @classmethod
    def get_erasure_code_profiles(cls):
        def _serialize_ecp(name, ecp):
            def serialize_numbers(key):
                value = ecp.get(key)
                if value is not None:
                    ecp[key] = int(value)

            ecp['name'] = name
            serialize_numbers('k')
            serialize_numbers('m')
            return ecp

        ret = []
        for name, ecp in mgr.get('osd_map').get('erasure_code_profiles', {}).items():
            ret.append(_serialize_ecp(name, ecp))
        return ret

    @classmethod
    def get_pool_name_from_id(cls, pool_id):
        # type: (int) -> Union[str, None]
        return mgr.rados.pool_reverse_lookup(pool_id)

    @classmethod
    def get_pool_by_attribute(cls, attribute, value):
        # type: (str, Any) -> Union[dict, None]
        pool_list = cls.get_pool_list()
        for pool in pool_list:
            if attribute in pool and pool[attribute] == value:
                return pool
        return None

    @classmethod
    def get_pool_pg_status(cls, pool_name):
        # type: (str) -> dict
        pool = cls.get_pool_by_attribute('pool_name', pool_name)
        if pool is None:
            return {}
        return mgr.get("pg_summary")['by_pool'][pool['pool'].__str__()]

    @classmethod
    def send_command(cls, srv_type, prefix, srv_spec='', **kwargs):
        """
        :type prefix: str
        :param srv_type: mon |
        :param kwargs: will be added to argdict
        :param srv_spec: typically empty. or something like "<fs_id>:0"

        :raises PermissionError: See rados.make_ex
        :raises ObjectNotFound: See rados.make_ex
        :raises IOError: See rados.make_ex
        :raises NoSpace: See rados.make_ex
        :raises ObjectExists: See rados.make_ex
        :raises ObjectBusy: See rados.make_ex
        :raises NoData: See rados.make_ex
        :raises InterruptedOrTimeoutError: See rados.make_ex
        :raises TimedOut: See rados.make_ex
        :raises ValueError: return code != 0
        """
        argdict = {
            "prefix": prefix,
            "format": "json",
        }
        argdict.update({k: v for k, v in kwargs.items() if v is not None})
        result = CommandResult("")
        mgr.send_command(result, srv_type, srv_spec, json.dumps(argdict), "")
        r, outb, outs = result.wait()
        if r != 0:
            logger.error("send_command '%s' failed. (r=%s, outs=\"%s\", kwargs=%s)", prefix, r,
                         outs, kwargs)

            raise SendCommandError(outs, prefix, argdict, r)

        try:
            return json.loads(outb or outs)
        except Exception:  # pylint: disable=broad-except
            return outb

    @staticmethod
    def _get_smart_data_by_device(device):
        # type: (dict) -> Dict[str, dict]
        # Check whether the device is associated with daemons.
        if 'daemons' in device and device['daemons']:
            dev_smart_data = None

            # The daemons associated with the device. Note, the list may
            # contain daemons that are 'down' or 'destroyed'.
            daemons = device.get('daemons')

            # Get a list of all OSD daemons on all hosts that are 'up'
            # because SMART data can not be retrieved from daemons that
            # are 'down' or 'destroyed'.
            osd_tree = CephService.send_command('mon', 'osd tree')
            osd_daemons_up = [
                node['name'] for node in osd_tree.get('nodes', {})
                if node.get('status') == 'up'
            ]

            # Finally get the daemons on the host of the given device
            # that are 'up'. All daemons on the same host can deliver
            # SMART data, thus it is not relevant for us which daemon
            # we are using.
            daemons = list(set(daemons) & set(osd_daemons_up))  # type: ignore

            for daemon in daemons:
                svc_type, svc_id = daemon.split('.')
                try:
                    dev_smart_data = CephService.send_command(
                        svc_type, 'smart', svc_id, devid=device['devid'])
                except SendCommandError:
                    # Try to retrieve SMART data from another daemon.
                    continue
                for dev_id, dev_data in dev_smart_data.items():
                    if 'error' in dev_data:
                        logger.warning(
                            '[SMART] Error retrieving smartctl data for device ID "%s": %s',
                            dev_id, dev_data)
                break
            if dev_smart_data is None:
                raise DashboardException(
                    'Failed to retrieve SMART data for device ID "{}"'.format(
                        device['devid']))
            return dev_smart_data
        logger.warning('[SMART] No daemons associated with device ID "%s"',
                       device['devid'])
        return {}

    @staticmethod
    def get_devices_by_host(hostname):
        # (str) -> dict
        return CephService.send_command('mon',
                                        'device ls-by-host',
                                        host=hostname)

    @staticmethod
    def get_devices_by_daemon(daemon_type, daemon_id):
        # (str, str) -> dict
        return CephService.send_command('mon',
                                        'device ls-by-daemon',
                                        who='{}.{}'.format(
                                            daemon_type, daemon_id))

    @staticmethod
    def get_smart_data_by_host(hostname):
        # type: (str) -> dict
        """
        Get the SMART data of all devices on the given host, regardless
        of the daemon (osd, mon, ...).
        :param hostname: The name of the host.
        :return: A dictionary containing the SMART data of every device
          on the given host. The device name is used as the key in the
          dictionary.
        """
        devices = CephService.get_devices_by_host(hostname)
        smart_data = {}  # type: dict
        if devices:
            for device in devices:
                if device['devid'] not in smart_data:
                    smart_data.update(
                        CephService._get_smart_data_by_device(device))
        return smart_data

    @staticmethod
    def get_smart_data_by_daemon(daemon_type, daemon_id):
        # type: (str, str) -> Dict[str, dict]
        """
        Get the SMART data of the devices associated with the given daemon.
        :param daemon_type: The daemon type, e.g. 'osd' or 'mon'.
        :param daemon_id: The daemon identifier.
        :return: A dictionary containing the SMART data of every device
          associated with the given daemon. The device name is used as the
          key in the dictionary.
        """
        devices = CephService.get_devices_by_daemon(daemon_type, daemon_id)
        smart_data = {}  # type: Dict[str, dict]
        if devices:
            for device in devices:
                if device['devid'] not in smart_data:
                    smart_data.update(
                        CephService._get_smart_data_by_device(device))
        return smart_data

    @classmethod
    def get_rates(cls, svc_type, svc_name, path):
        """
        :return: the derivative of mgr.get_counter()
        :rtype: list[tuple[int, float]]"""
        data = mgr.get_counter(svc_type, svc_name, path)[path]
        return get_time_series_rates(data)

    @classmethod
    def get_rate(cls, svc_type, svc_name, path):
        """returns most recent rate"""
        return get_most_recent_rate(cls.get_rates(svc_type, svc_name, path))

    @classmethod
    def get_client_perf(cls):
        pools_stats = mgr.get('osd_pool_stats')['pool_stats']

        io_stats = {
            'read_bytes_sec': 0,
            'read_op_per_sec': 0,
            'write_bytes_sec': 0,
            'write_op_per_sec': 0,
        }
        recovery_stats = {'recovering_bytes_per_sec': 0}

        for pool_stats in pools_stats:
            client_io = pool_stats['client_io_rate']
            for stat in list(io_stats.keys()):
                if stat in client_io:
                    io_stats[stat] += client_io[stat]

            client_recovery = pool_stats['recovery_rate']
            for stat in list(recovery_stats.keys()):
                if stat in client_recovery:
                    recovery_stats[stat] += client_recovery[stat]

        client_perf = io_stats.copy()
        client_perf.update(recovery_stats)

        return client_perf

    @classmethod
    def get_scrub_status(cls):
        enabled_flags = mgr.get('osd_map')['flags_set']
        if cls.OSD_FLAG_NO_SCRUB in enabled_flags or cls.OSD_FLAG_NO_DEEP_SCRUB in enabled_flags:
            return cls.SCRUB_STATUS_DISABLED

        grouped_pg_statuses = mgr.get('pg_summary')['all']
        for grouped_pg_status in grouped_pg_statuses.keys():
            if len(grouped_pg_status.split(cls.PG_STATUS_SCRUBBING)) > 1 \
                    or len(grouped_pg_status.split(cls.PG_STATUS_DEEP_SCRUBBING)) > 1:
                return cls.SCRUB_STATUS_ACTIVE

        return cls.SCRUB_STATUS_INACTIVE

    @classmethod
    def get_pg_info(cls):
        pg_summary = mgr.get('pg_summary')
        object_stats = {stat: pg_summary['pg_stats_sum']['stat_sum'][stat] for stat in [
            'num_objects', 'num_object_copies', 'num_objects_degraded',
            'num_objects_misplaced', 'num_objects_unfound']}

        pgs_per_osd = 0.0
        total_osds = len(pg_summary['by_osd'])
        if total_osds > 0:
            total_pgs = 0.0
            for _, osd_pg_statuses in pg_summary['by_osd'].items():
                for _, pg_amount in osd_pg_statuses.items():
                    total_pgs += pg_amount

            pgs_per_osd = total_pgs / total_osds

        return {
            'object_stats': object_stats,
            'statuses': pg_summary['all'],
            'pgs_per_osd': pgs_per_osd,
        }
