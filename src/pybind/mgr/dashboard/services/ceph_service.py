# -*- coding: utf-8 -*-
from __future__ import absolute_import

import time
import collections
from collections import defaultdict
import json

import rados

from mgr_module import CommandResult

try:
    from more_itertools import pairwise
except ImportError:
    def pairwise(iterable):
        from itertools import tee
        a, b = tee(iterable)
        next(b, None)
        return zip(a, b)

from .. import logger, mgr


class SendCommandError(rados.Error):
    def __init__(self, err, prefix, argdict, errno):
        self.prefix = prefix
        self.argdict = argdict
        super(SendCommandError, self).__init__(err, errno)


class CephService(object):

    OSD_FLAG_NO_SCRUB = 'noscrub'
    OSD_FLAG_NO_DEEP_SCRUB = 'nodeep-scrub'

    POOL_STAT_CLIENT_IO_RATE = 'client_io_rate'
    POOL_STAT_READ_BYTES_PER_SEC = 'read_bytes_sec'
    POOL_STAT_READ_OP_PER_SEC = 'read_op_per_sec'
    POOL_STAT_RECOVERY_BYTES_PER_SEC = 'recovering_bytes_per_sec'
    POOL_STAT_RECOVERY_RATE = 'recovery_rate'
    POOL_STAT_WRITE_BYTES_PER_SEC = 'write_bytes_sec'
    POOL_STAT_WRITE_OP_PER_SEC = 'write_op_per_sec'

    PG_STATUS_SCRUBBING = 'scrubbing'
    PG_STATUS_DEEP_SCRUBBING = 'deep'

    SCRUB_STATUS_DISABLED = 'Disabled'
    SCRUB_STATUS_ACTIVE = 'Active'
    SCRUB_STATUS_INACTIVE = 'Inactive'

    @classmethod
    def get_service_map(cls, service_name):
        service_map = {}
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
        pool_stats = defaultdict(lambda: defaultdict(
            lambda: collections.deque(maxlen=10)))

        df = mgr.get("df")
        pool_stats_dict = dict([(p['id'], p['stats']) for p in df['pools']])
        now = time.time()
        for pool_id, stats in pool_stats_dict.items():
            for stat_name, stat_val in stats.items():
                pool_stats[pool_id][stat_name].appendleft((now, stat_val))

        for pool in pools:
            pool['pg_status'] = pg_summary['by_pool'][pool['pool'].__str__()]
            stats = pool_stats[pool['pool']]
            s = {}

            def get_rate(series):
                if len(series) >= 2:
                    return differentiate(*series[0:1])
                return 0

            for stat_name, stat_series in stats.items():
                s[stat_name] = {
                    'latest': stat_series[0][1],
                    'rate': get_rate(stat_series),
                    'series': [i for i in stat_series]
                }
            pool['stats'] = s
            pools_w_stats.append(pool)
        return pools_w_stats

    @classmethod
    def get_pool_name_from_id(cls, pool_id):
        pool_list = cls.get_pool_list()
        for pool in pool_list:
            if pool['pool'] == pool_id:
                return pool['pool_name']
        return None

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
            msg = "send_command '{}' failed. (r={}, outs=\"{}\", kwargs={})".format(prefix, r, outs,
                                                                                    kwargs)
            logger.error(msg)
            raise SendCommandError(outs, prefix, argdict, r)
        else:
            try:
                return json.loads(outb)
            except Exception:  # pylint: disable=broad-except
                return outb

    @classmethod
    def get_rates(cls, svc_type, svc_name, path):
        """
        :return: the derivative of mgr.get_counter()
        :rtype: list[tuple[int, float]]"""
        data = mgr.get_counter(svc_type, svc_name, path)[path]
        if not data:
            return [(0, 0.0)]
        elif len(data) == 1:
            return [(data[0][0], 0.0)]
        return [(data2[0], differentiate(data1, data2)) for data1, data2 in pairwise(data)]

    @classmethod
    def get_rate(cls, svc_type, svc_name, path):
        """returns most recent rate"""
        data = mgr.get_counter(svc_type, svc_name, path)[path]

        if data and len(data) > 1:
            return differentiate(*data[-2:])
        return 0.0

    @classmethod
    def get_client_perf(cls):
        pools_stats = mgr.get('osd_pool_stats')['pool_stats']

        client_perf = {
            cls.POOL_STAT_READ_BYTES_PER_SEC: 0,
            cls.POOL_STAT_READ_OP_PER_SEC: 0,
            cls.POOL_STAT_RECOVERY_BYTES_PER_SEC: 0,
            cls.POOL_STAT_WRITE_BYTES_PER_SEC: 0,
            cls.POOL_STAT_WRITE_OP_PER_SEC: 0,
        }

        for pool_stats in pools_stats:
            client_io = pool_stats[cls.POOL_STAT_CLIENT_IO_RATE]
            if cls.POOL_STAT_READ_BYTES_PER_SEC in client_io:
                client_perf[cls.POOL_STAT_READ_BYTES_PER_SEC] +=\
                    client_io[cls.POOL_STAT_READ_BYTES_PER_SEC]
                client_perf[cls.POOL_STAT_READ_OP_PER_SEC] +=\
                    client_io[cls.POOL_STAT_READ_OP_PER_SEC]
                client_perf[cls.POOL_STAT_WRITE_BYTES_PER_SEC] +=\
                    client_io[cls.POOL_STAT_WRITE_BYTES_PER_SEC]
                client_perf[cls.POOL_STAT_WRITE_OP_PER_SEC] +=\
                    client_io[cls.POOL_STAT_WRITE_OP_PER_SEC]

            client_recovery = pool_stats[cls.POOL_STAT_RECOVERY_RATE]
            if cls.POOL_STAT_RECOVERY_BYTES_PER_SEC in client_recovery:
                client_perf[cls.POOL_STAT_RECOVERY_BYTES_PER_SEC] +=\
                    client_recovery[cls.POOL_STAT_RECOVERY_BYTES_PER_SEC]

        return client_perf

    @classmethod
    def get_scrub_status(cls):
        enabled_flags = mgr.get('osd_map')['flags'].split(',')
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

        pgs_per_osd = 0
        total_osds = len(pg_summary['by_osd'])
        if total_osds > 0:
            total_pgs = 0
            for _, osd_pg_statuses in pg_summary['by_osd'].items():
                for _, pg_amount in osd_pg_statuses.items():
                    total_pgs += pg_amount

            pgs_per_osd = total_pgs / total_osds

        return {
            'statuses': pg_summary['all'],
            'pgs_per_osd': pgs_per_osd,
        }


def differentiate(data1, data2):
    """
    >>> times = [0, 2]
    >>> values = [100, 101]
    >>> differentiate(*zip(times, values))
    0.5
    """
    return (data2[1] - data1[1]) / float(data2[0] - data1[0])
