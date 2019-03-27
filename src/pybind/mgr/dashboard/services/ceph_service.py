# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

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


class CephService(object):
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
        pool_stats = mgr.get_updated_pool_stats()

        for pool in pools:
            pool['pg_status'] = pg_summary['by_pool'][pool['pool'].__str__()]
            stats = pool_stats[pool['pool']]
            s = {}

            def get_rate(series):
                if len(series) >= 2:
                    return differentiate(*list(series)[-2:])
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
        argdict.update({k: v for k, v in kwargs.items() if v})

        result = CommandResult("")
        mgr.send_command(result, srv_type, srv_spec, json.dumps(argdict), "")
        r, outb, outs = result.wait()
        if r != 0:
            msg = "send_command '{}' failed. (r={}, outs=\"{}\", kwargs={})".format(prefix, r, outs,
                                                                                    kwargs)
            logger.error(msg)
            raise ValueError(msg)
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


def differentiate(data1, data2):
    """
    >>> times = [0, 2]
    >>> values = [100, 101]
    >>> differentiate(*zip(times, values))
    0.5
    """
    return (data2[1] - data1[1]) / float(data2[0] - data1[0])
