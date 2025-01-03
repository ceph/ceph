# -*- coding: utf-8 -*-

import json

from .. import mgr
from ..rest_client import RequestException
from ..security import Permission, Scope
from ..services.ceph_service import CephService
from ..services.cluster import ClusterModel
from ..services.iscsi_cli import IscsiGatewaysConfig
from ..services.iscsi_client import IscsiClient
from ..tools import partial_dict
from . import APIDoc, APIRouter, BaseController, Endpoint, EndpointDoc
from .host import get_hosts

HEALTH_MINIMAL_SCHEMA = ({
    'client_perf': ({
        'read_bytes_sec': (int, ''),
        'read_op_per_sec': (int, ''),
        'recovering_bytes_per_sec': (int, ''),
        'write_bytes_sec': (int, ''),
        'write_op_per_sec': (int, ''),
    }, ''),
    'df': ({
        'stats': ({
            'total_avail_bytes': (int, ''),
            'total_bytes': (int, ''),
            'total_used_raw_bytes': (int, ''),
        }, '')
    }, ''),
    'fs_map': ({
        'filesystems': ([{
            'mdsmap': ({
                'session_autoclose': (int, ''),
                'balancer': (str, ''),
                'up': (str, ''),
                'last_failure_osd_epoch': (int, ''),
                'in': ([int], ''),
                'last_failure': (int, ''),
                'max_file_size': (int, ''),
                'explicitly_allowed_features': (int, ''),
                'damaged': ([int], ''),
                'tableserver': (int, ''),
                'failed': ([int], ''),
                'metadata_pool': (int, ''),
                'epoch': (int, ''),
                'btime': (str, ''),
                'stopped': ([int], ''),
                'max_mds': (int, ''),
                'compat': ({
                    'compat': (str, ''),
                    'ro_compat': (str, ''),
                    'incompat': (str, ''),
                }, ''),
                'required_client_features': (str, ''),
                'data_pools': ([int], ''),
                'info': (str, ''),
                'fs_name': (str, ''),
                'created': (str, ''),
                'standby_count_wanted': (int, ''),
                'enabled': (bool, ''),
                'modified': (str, ''),
                'session_timeout': (int, ''),
                'flags': (int, ''),
                'ever_allowed_features': (int, ''),
                'root': (int, ''),
            }, ''),
            'standbys': (str, ''),
        }], ''),
    }, ''),
    'health': ({
        'checks': (str, ''),
        'mutes': (str, ''),
        'status': (str, ''),
    }, ''),
    'hosts': (int, ''),
    'iscsi_daemons': ({
        'up': (int, ''),
        'down': (int, '')
    }, ''),
    'mgr_map': ({
        'active_name': (str, ''),
        'standbys': (str, '')
    }, ''),
    'mon_status': ({
        'monmap': ({
            'mons': (str, ''),
        }, ''),
        'quorum': ([int], '')
    }, ''),
    'osd_map': ({
        'osds': ([{
            'in': (int, ''),
            'up': (int, ''),
        }], '')
    }, ''),
    'pg_info': ({
        'object_stats': ({
            'num_objects': (int, ''),
            'num_object_copies': (int, ''),
            'num_objects_degraded': (int, ''),
            'num_objects_misplaced': (int, ''),
            'num_objects_unfound': (int, ''),
        }, ''),
        'pgs_per_osd': (int, ''),
        'statuses': (str, '')
    }, ''),
    'pools': (str, ''),
    'rgw': (int, ''),
    'scrub_status': (str, '')
})


class HealthData(object):
    """
    A class to be used in combination with BaseController to allow either
    "full" or "minimal" sets of health data to be collected.

    To function properly, it needs BaseCollector._has_permissions to be passed
    in as ``auth_callback``.
    """

    def __init__(self, auth_callback, minimal=True):
        self._has_permissions = auth_callback
        self._minimal = minimal

    def all_health(self):
        result = {
            "health": self.basic_health(),
        }

        if self._has_permissions(Permission.READ, Scope.MONITOR):
            result['mon_status'] = self.mon_status()

        if self._has_permissions(Permission.READ, Scope.CEPHFS):
            result['fs_map'] = self.fs_map()

        if self._has_permissions(Permission.READ, Scope.OSD):
            result['osd_map'] = self.osd_map()
            result['scrub_status'] = self.scrub_status()
            result['pg_info'] = self.pg_info()

        if self._has_permissions(Permission.READ, Scope.MANAGER):
            result['mgr_map'] = self.mgr_map()

        if self._has_permissions(Permission.READ, Scope.POOL):
            result['pools'] = self.pools()
            result['df'] = self.df()
            result['client_perf'] = self.client_perf()

        if self._has_permissions(Permission.READ, Scope.HOSTS):
            result['hosts'] = self.host_count()

        if self._has_permissions(Permission.READ, Scope.RGW):
            result['rgw'] = self.rgw_count()

        if self._has_permissions(Permission.READ, Scope.ISCSI):
            result['iscsi_daemons'] = self.iscsi_daemons()

        return result

    def basic_health(self):
        health_data = mgr.get("health")
        health = json.loads(health_data['json'])

        # Transform the `checks` dict into a list for the convenience
        # of rendering from javascript.
        checks = []
        for k, v in health['checks'].items():
            v['type'] = k
            checks.append(v)

        checks = sorted(checks, key=lambda c: c['severity'])
        health['checks'] = checks
        return health

    def client_perf(self):
        result = CephService.get_client_perf()
        if self._minimal:
            result = partial_dict(
                result,
                ['read_bytes_sec', 'read_op_per_sec',
                 'recovering_bytes_per_sec', 'write_bytes_sec',
                 'write_op_per_sec']
            )
        return result

    def df(self):
        df = mgr.get('df')

        del df['stats_by_class']

        if self._minimal:
            df = dict(stats=partial_dict(
                df['stats'],
                ['total_avail_bytes', 'total_bytes',
                 'total_used_raw_bytes']
            ))
        return df

    def fs_map(self):
        fs_map = mgr.get('fs_map')
        if self._minimal:
            fs_map = partial_dict(fs_map, ['filesystems', 'standbys'])
            fs_map['filesystems'] = [partial_dict(item, ['mdsmap']) for
                                     item in fs_map['filesystems']]
            for fs in fs_map['filesystems']:
                mdsmap_info = fs['mdsmap']['info']
                min_mdsmap_info = dict()
                for k, v in mdsmap_info.items():
                    min_mdsmap_info[k] = partial_dict(v, ['state'])
        return fs_map

    def host_count(self):
        return len(get_hosts())

    def iscsi_daemons(self):
        up_counter = 0
        down_counter = 0
        for gateway_name in IscsiGatewaysConfig.get_gateways_config()['gateways']:
            try:
                IscsiClient.instance(gateway_name=gateway_name).ping()
                up_counter += 1
            except RequestException:
                down_counter += 1
        return {'up': up_counter, 'down': down_counter}

    def mgr_map(self):
        mgr_map = mgr.get('mgr_map')
        if self._minimal:
            mgr_map = partial_dict(mgr_map, ['active_name', 'standbys'])
        return mgr_map

    def mon_status(self):
        mon_status = json.loads(mgr.get('mon_status')['json'])
        if self._minimal:
            mon_status = partial_dict(mon_status, ['monmap', 'quorum'])
            mon_status['monmap'] = partial_dict(
                mon_status['monmap'], ['mons']
            )
            mon_status['monmap']['mons'] = [{}] * \
                len(mon_status['monmap']['mons'])
        return mon_status

    def osd_map(self):
        osd_map = mgr.get('osd_map')
        assert osd_map is not None
        # Not needed, skip the effort of transmitting this to UI
        del osd_map['pg_temp']
        if self._minimal:
            osd_map = partial_dict(osd_map, ['osds'])
            osd_map['osds'] = [
                partial_dict(item, ['in', 'up', 'state'])
                for item in osd_map['osds']
            ]
        else:
            osd_map['tree'] = mgr.get('osd_map_tree')
            osd_map['crush'] = mgr.get('osd_map_crush')
            osd_map['crush_map_text'] = mgr.get('osd_map_crush_map_text')
            osd_map['osd_metadata'] = mgr.get('osd_metadata')
        return osd_map

    def pg_info(self):
        return CephService.get_pg_info()

    def pools(self):
        pools = CephService.get_pool_list_with_stats()
        if self._minimal:
            pools = [{}] * len(pools)
        return pools

    def rgw_count(self):
        return len(CephService.get_service_list('rgw'))

    def scrub_status(self):
        return CephService.get_scrub_status()


@APIRouter('/health')
@APIDoc("Display Detailed Cluster health Status", "Health")
class Health(BaseController):
    def __init__(self):
        super().__init__()
        self.health_full = HealthData(self._has_permissions, minimal=False)
        self.health_minimal = HealthData(self._has_permissions, minimal=True)

    @Endpoint()
    def full(self):
        return self.health_full.all_health()

    @Endpoint()
    @EndpointDoc("Get Cluster's minimal health report",
                 responses={200: HEALTH_MINIMAL_SCHEMA})
    def minimal(self):
        return self.health_minimal.all_health()

    @Endpoint()
    def get_cluster_capacity(self):
        return ClusterModel.get_capacity()

    @Endpoint()
    def get_cluster_fsid(self):
        return mgr.get('config')['fsid']

    @Endpoint()
    def get_telemetry_status(self):
        return mgr.get_module_option_ex('telemetry', 'enabled', False)
