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

HEALTH_SNAPSHOT_SCHEMA = ({
    'fsid': (str, 'Cluster filesystem ID'),
    'health': ({
        'status': (str, 'Overall health status'),
        'checks': ({
            '<check_name>': ({
                'severity': (str, 'Health severity level'),
                'summary': ({
                    'message': (str, 'Human-readable summary'),
                    'count': (int, 'Occurrence count')
                }, 'Summary details'),
                'muted': (bool, 'Whether the check is muted')
            }, 'Individual health check object')
        }, 'Health checks keyed by name'),
        'mutes': ([str], 'List of muted check names')
    }, 'Cluster health overview'),
    'monmap': ({
        'num_mons': (int, 'Number of monitors')
    }, 'Monitor map details'),
    'osdmap': ({
        'in': (int, 'Number of OSDs in'),
        'up': (int, 'Number of OSDs up'),
        'num_osds': (int, 'Total OSD count')
    }, 'OSD map details'),
    'pgmap': ({
        'pgs_by_state': ([{
            'state_name': (str, 'Placement group state'),
            'count': (int, 'Count of PGs in this state')
        }], 'List of PG counts by state'),
        'num_pools': (int, 'Number of pools'),
        'num_pgs': (int, 'Total PG count'),
        'bytes_used': (int, 'Used capacity in bytes'),
        'bytes_total': (int, 'Total capacity in bytes'),
    }, 'Placement group map details'),
    'mgrmap': ({
        'num_active': (int, 'Number of active managers'),
        'num_standbys': (int, 'Standby manager count')
    }, 'Manager map details'),
    'fsmap': ({
        'num_active': (int, 'Number of active mds'),
        'num_standbys': (int, 'Standby MDS count'),
    }, 'Filesystem map details'),
    'num_rgw_gateways': (int, 'Count of RGW gateway daemons running'),
    'num_iscsi_gateways': ({
        'up': (int, 'Count of iSCSI gateways running'),
        'down': (int, 'Count of iSCSI gateways not running')
    }, 'Iscsi gateways status'),
    'num_hosts': (int, 'Count of hosts')
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
        self._health_full = None
        self._health_minimal = None

    @property
    def health_full(self):
        if self._health_full is None:
            self._health_full = HealthData(self._has_permissions, minimal=False)
        return self._health_full

    @property
    def health_minimal(self):
        if self._health_minimal is None:
            self._health_minimal = HealthData(self._has_permissions, minimal=True)
        return self._health_minimal

    @Endpoint()
    @EndpointDoc("Get Cluster's detailed health report")
    def full(self):
        return self.health_full.all_health()

    @Endpoint()
    @EndpointDoc("Get Cluster's health report with lesser details",
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

    @Endpoint()
    @EndpointDoc(
        "Get a quick overview of cluster health at a moment, analogous to "
        "the ceph status command in CLI.",
        responses={200: HEALTH_SNAPSHOT_SCHEMA})
    def snapshot(self):
        data = CephService.send_command('mon', 'status')

        summary = {
            'fsid': data.get('fsid'),
            'health': {
                'status': data.get('health', {}).get('status'),
                'checks': data.get('health', {}).get('checks', {}),
                'mutes': data.get('health', {}).get('mutes', []),
            },
        }

        if self._has_permissions(Permission.READ, Scope.MONITOR):
            summary['monmap'] = {
                'num_mons': data.get('monmap', {}).get('num_mons'),
            }

        if self._has_permissions(Permission.READ, Scope.OSD):
            summary['osdmap'] = {
                'in': data.get('osdmap', {}).get('num_in_osds'),
                'up': data.get('osdmap', {}).get('num_up_osds'),
                'num_osds': data.get('osdmap', {}).get('num_osds'),
            }
            summary['pgmap'] = {
                'pgs_by_state': data.get('pgmap', {}).get('pgs_by_state', []),
                'num_pools': data.get('pgmap', {}).get('num_pools'),
                'num_pgs': data.get('pgmap', {}).get('num_pgs'),
                'bytes_used': data.get('pgmap', {}).get('bytes_used'),
                'bytes_total': data.get('pgmap', {}).get('bytes_total'),
            }

        if self._has_permissions(Permission.READ, Scope.MANAGER):
            mgrmap = data.get('mgrmap', {})
            available = mgrmap.get('available', False)
            num_standbys = mgrmap.get('num_standbys')
            num_active = 1 if available else 0
            summary['mgrmap'] = {
                'num_active': num_active,
                'num_standbys': num_standbys,
            }

        if self._has_permissions(Permission.READ, Scope.CEPHFS):
            fsmap = data.get('fsmap', {})
            by_rank = fsmap.get('by_rank', [])

            active_count = 0
            standby_replay_count = 0

            for mds in by_rank:
                state = mds.get('status', '')
                if state == 'up:standby-replay':
                    standby_replay_count += 1
                elif state.startswith('up:'):
                    active_count += 1

            summary['fsmap'] = {
                'num_active': active_count,
                'num_standbys': fsmap.get('up:standby', 0) + standby_replay_count,
            }

        if self._has_permissions(Permission.READ, Scope.RGW):
            daemons = (
                data.get('servicemap', {})
                .get('services', {})
                .get('rgw', {})
                .get('daemons', {})
                or {}
            )
            daemons.pop("summary", None)
            summary['num_rgw_gateways'] = len(daemons)

        if self._has_permissions(Permission.READ, Scope.ISCSI):
            summary['num_iscsi_gateways'] = self.health_minimal.iscsi_daemons()

        if self._has_permissions(Permission.READ, Scope.HOSTS):
            summary['num_hosts'] = len(get_hosts())

        return summary
