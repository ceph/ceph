# -*- coding: utf-8 -*-

import json
import logging
import time
from typing import Any, Dict, List, Optional, Union

from ceph.deployment.drive_group import DriveGroupSpec, DriveGroupValidationError  # type: ignore
from mgr_util import get_most_recent_rate

from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope
from ..services.ceph_service import CephService, SendCommandError
from ..services.exception import handle_orchestrator_error, handle_send_command_error
from ..services.orchestrator import OrchClient, OrchFeature
from ..services.osd import HostStorageSummary, OsdDeploymentOptions
from ..tools import str_to_bool
from . import APIDoc, APIRouter, CreatePermission, DeletePermission, Endpoint, \
    EndpointDoc, ReadPermission, RESTController, Task, UIRouter, \
    UpdatePermission, allow_empty_body
from ._version import APIVersion
from .orchestrator import raise_if_no_orchestrator

logger = logging.getLogger('controllers.osd')

SAFE_TO_DESTROY_SCHEMA = {
    "safe_to_destroy": ([str], "Is OSD safe to destroy?"),
    "active": ([int], ""),
    "missing_stats": ([str], ""),
    "stored_pgs": ([str], "Stored Pool groups in Osd"),
    "is_safe_to_destroy": (bool, "Is OSD safe to destroy?")
}

EXPORT_FLAGS_SCHEMA = {
    "list_of_flags": ([str], "")
}

EXPORT_INDIV_FLAGS_SCHEMA = {
    "added": ([str], "List of added flags"),
    "removed": ([str], "List of removed flags"),
    "ids": ([int], "List of updated OSDs")
}

EXPORT_INDIV_FLAGS_GET_SCHEMA = {
    "osd": (int, "OSD ID"),
    "flags": ([str], "List of active flags")
}


class DeploymentOptions:
    def __init__(self):
        self.options = {
            OsdDeploymentOptions.COST_CAPACITY:
                HostStorageSummary(OsdDeploymentOptions.COST_CAPACITY,
                                   title='Cost/Capacity-optimized',
                                   desc='All the available HDDs are selected'),
            OsdDeploymentOptions.THROUGHPUT:
                HostStorageSummary(OsdDeploymentOptions.THROUGHPUT,
                                   title='Throughput-optimized',
                                   desc="HDDs/SSDs are selected for data"
                                   "devices and SSDs/NVMes for DB/WAL devices"),
            OsdDeploymentOptions.IOPS:
                HostStorageSummary(OsdDeploymentOptions.IOPS,
                                   title='IOPS-optimized',
                                   desc='All the available NVMes are selected'),
        }
        self.recommended_option = None

    def as_dict(self):
        return {
            'options': {k: v.as_dict() for k, v in self.options.items()},
            'recommended_option': self.recommended_option
        }


predefined_drive_groups = {
    OsdDeploymentOptions.COST_CAPACITY: {
        'service_type': 'osd',
        'service_id': 'cost_capacity',
        'placement': {
            'host_pattern': '*'
        },
        'data_devices': {
            'rotational': 1
        },
        'encrypted': False
    },
    OsdDeploymentOptions.THROUGHPUT: {
        'service_type': 'osd',
        'service_id': 'throughput_optimized',
        'placement': {
            'host_pattern': '*'
        },
        'data_devices': {
            'rotational': 1
        },
        'db_devices': {
            'rotational': 0
        },
        'encrypted': False
    },
    OsdDeploymentOptions.IOPS: {
        'service_type': 'osd',
        'service_id': 'iops_optimized',
        'placement': {
            'host_pattern': '*'
        },
        'data_devices': {
            'rotational': 0
        },
        'encrypted': False
    },
}


def osd_task(name, metadata, wait_for=2.0):
    return Task("osd/{}".format(name), metadata, wait_for)


@APIRouter('/osd', Scope.OSD)
@APIDoc('OSD management API', 'OSD')
class Osd(RESTController):
    def list(self):
        osds = self.get_osd_map()

        # Extending by osd stats information
        for stat in mgr.get('osd_stats')['osd_stats']:
            if stat['osd'] in osds:
                osds[stat['osd']]['osd_stats'] = stat

        # Extending by osd node information
        nodes = mgr.get('osd_map_tree')['nodes']
        for node in nodes:
            if node['type'] == 'osd' and node['id'] in osds:
                osds[node['id']]['tree'] = node

        # Extending by osd parent node information
        for host in [n for n in nodes if n['type'] == 'host']:
            for osd_id in host['children']:
                if osd_id >= 0 and osd_id in osds:
                    osds[osd_id]['host'] = host

        removing_osd_ids = self.get_removing_osds()

        # Extending by osd histogram and orchestrator data
        for osd_id, osd in osds.items():
            osd['stats'] = {}
            osd['stats_history'] = {}
            osd_spec = str(osd_id)
            if 'osd' not in osd:
                continue  # pragma: no cover - simple early continue
            self.gauge_stats(osd, osd_spec)
            osd['operational_status'] = self._get_operational_status(osd_id, removing_osd_ids)
        return list(osds.values())

    @staticmethod
    def gauge_stats(osd, osd_spec):
        for stat in ['osd.op_w', 'osd.op_in_bytes', 'osd.op_r', 'osd.op_out_bytes']:
            prop = stat.split('.')[1]
            rates = CephService.get_rates('osd', osd_spec, stat)
            osd['stats'][prop] = get_most_recent_rate(rates)
            osd['stats_history'][prop] = rates
            # Gauge stats
        for stat in ['osd.numpg', 'osd.stat_bytes', 'osd.stat_bytes_used']:
            osd['stats'][stat.split('.')[1]] = mgr.get_latest('osd', osd_spec, stat)

    @RESTController.Collection('GET', version=APIVersion.EXPERIMENTAL)
    @ReadPermission
    def settings(self):
        data = {
            'nearfull_ratio': -1,
            'full_ratio': -1
        }
        try:
            result = CephService.send_command('mon', 'osd dump')
            data['nearfull_ratio'] = result['nearfull_ratio']
            data['full_ratio'] = result['full_ratio']
        except TypeError:
            logger.error(
                'Error setting nearfull_ratio and full_ratio:', exc_info=True)
        return data

    def _get_operational_status(self, osd_id: int, removing_osd_ids: Optional[List[int]]):
        if removing_osd_ids is None:
            return 'unmanaged'
        if osd_id in removing_osd_ids:
            return 'deleting'
        return 'working'

    @staticmethod
    def get_removing_osds() -> Optional[List[int]]:
        orch = OrchClient.instance()
        if orch.available(features=[OrchFeature.OSD_GET_REMOVE_STATUS]):
            return [osd.osd_id for osd in orch.osds.removing_status()]
        return None

    @staticmethod
    def get_osd_map(svc_id=None):
        # type: (Union[int, None]) -> Dict[int, Union[dict, Any]]
        def add_id(osd):
            osd['id'] = osd['osd']
            return osd

        resp = {
            osd['osd']: add_id(osd)
            for osd in mgr.get('osd_map')['osds'] if svc_id is None or osd['osd'] == int(svc_id)
        }
        return resp if svc_id is None else resp[int(svc_id)]

    @staticmethod
    def _get_smart_data(osd_id):
        # type: (str) -> dict
        """Returns S.M.A.R.T data for the given OSD ID."""
        logger.debug('[SMART] retrieving data from OSD with ID %s', osd_id)
        return CephService.get_smart_data_by_daemon('osd', osd_id)

    @RESTController.Resource('GET')
    def smart(self, svc_id):
        # type: (str) -> dict
        return self._get_smart_data(svc_id)

    @handle_send_command_error('osd')
    def get(self, svc_id):
        """
        Returns collected data about an OSD.

        :return: Returns the requested data.
        """
        return {
            'osd_map': self.get_osd_map(svc_id),
            'osd_metadata': mgr.get_metadata('osd', svc_id),
            'operational_status': self._get_operational_status(int(svc_id),
                                                               self.get_removing_osds())
        }

    @RESTController.Resource('GET')
    @handle_send_command_error('osd')
    def histogram(self, svc_id):
        # type: (int) -> Dict[str, Any]
        """
        :return: Returns the histogram data.
        """
        try:
            histogram = CephService.send_command(
                'osd', srv_spec=svc_id, prefix='perf histogram dump')
        except SendCommandError as e:  # pragma: no cover - the handling is too obvious
            raise DashboardException(
                component='osd', http_status_code=400, msg=str(e))

        return histogram

    def set(self, svc_id, device_class):  # pragma: no cover
        old_device_class = CephService.send_command('mon', 'osd crush get-device-class',
                                                    ids=[svc_id])
        old_device_class = old_device_class[0]['device_class']
        if old_device_class != device_class:
            CephService.send_command('mon', 'osd crush rm-device-class',
                                     ids=[svc_id])
            if device_class:
                CephService.send_command('mon', 'osd crush set-device-class', **{
                    'class': device_class,
                    'ids': [svc_id]
                })

    def _check_delete(self, osd_ids):
        # type: (List[str]) -> Dict[str, Any]
        """
        Check if it's safe to remove OSD(s).

        :param osd_ids: list of OSD IDs
        :return: a dictionary contains the following attributes:
            `safe`: bool, indicate if it's safe to remove OSDs.
            `message`: str, help message if it's not safe to remove OSDs.
        """
        _ = osd_ids
        health_data = mgr.get('health')  # type: ignore
        health = json.loads(health_data['json'])
        checks = health['checks'].keys()
        unsafe_checks = set(['OSD_FULL', 'OSD_BACKFILLFULL', 'OSD_NEARFULL'])
        failed_checks = checks & unsafe_checks
        msg = 'Removing OSD(s) is not recommended because of these failed health check(s): {}.'.\
            format(', '.join(failed_checks)) if failed_checks else ''
        return {
            'safe': not bool(failed_checks),
            'message': msg
        }

    @DeletePermission
    @raise_if_no_orchestrator([OrchFeature.OSD_DELETE, OrchFeature.OSD_GET_REMOVE_STATUS])
    @handle_orchestrator_error('osd')
    @osd_task('delete', {'svc_id': '{svc_id}'})
    def delete(self, svc_id, preserve_id=None, force=None):  # pragma: no cover
        replace = False
        check: Union[Dict[str, Any], bool] = False
        try:
            if preserve_id is not None:
                replace = str_to_bool(preserve_id)
            if force is not None:
                check = not str_to_bool(force)
        except ValueError:
            raise DashboardException(
                component='osd', http_status_code=400, msg='Invalid parameter(s)')
        orch = OrchClient.instance()
        if check:
            logger.info('Check for removing osd.%s...', svc_id)
            check = self._check_delete([svc_id])
            if not check['safe']:
                logger.error('Unable to remove osd.%s: %s', svc_id, check['message'])
                raise DashboardException(component='osd', msg=check['message'])

        logger.info('Start removing osd.%s (replace: %s)...', svc_id, replace)
        orch.osds.remove([svc_id], replace)
        while True:
            removal_osds = orch.osds.removing_status()
            logger.info('Current removing OSDs %s', removal_osds)
            pending = [osd for osd in removal_osds if osd.osd_id == int(svc_id)]
            if not pending:
                break
            logger.info('Wait until osd.%s is removed...', svc_id)
            time.sleep(60)

    @RESTController.Resource('POST', query_params=['deep'])
    @UpdatePermission
    @allow_empty_body
    def scrub(self, svc_id, deep=False):
        api_scrub = "osd deep-scrub" if str_to_bool(deep) else "osd scrub"
        CephService.send_command("mon", api_scrub, who=svc_id)

    @RESTController.Resource('PUT')
    @EndpointDoc("Mark OSD flags (out, in, down, lost, ...)",
                 parameters={'svc_id': (str, 'SVC ID')})
    def mark(self, svc_id, action):
        """
        Note: osd must be marked `down` before marking lost.
        """
        valid_actions = ['out', 'in', 'down', 'lost']
        args = {'srv_type': 'mon', 'prefix': 'osd ' + action}
        if action.lower() in valid_actions:
            if action == 'lost':
                args['id'] = int(svc_id)
                args['yes_i_really_mean_it'] = True
            else:
                args['ids'] = [svc_id]

            CephService.send_command(**args)
        else:
            logger.error("Invalid OSD mark action: %s attempted on SVC_ID: %s", action, svc_id)

    @RESTController.Resource('POST')
    @allow_empty_body
    def reweight(self, svc_id, weight):
        """
        Reweights the OSD temporarily.

        Note that ‘ceph osd reweight’ is not a persistent setting. When an OSD
        gets marked out, the osd weight will be set to 0. When it gets marked
        in again, the weight will be changed to 1.

        Because of this ‘ceph osd reweight’ is a temporary solution. You should
        only use it to keep your cluster running while you’re ordering more
        hardware.

        - Craig Lewis (http://lists.ceph.com/pipermail/ceph-users-ceph.com/2014-June/040967.html)
        """
        CephService.send_command(
            'mon',
            'osd reweight',
            id=int(svc_id),
            weight=float(weight))

    def _create_predefined_drive_group(self, data):
        orch = OrchClient.instance()
        option = OsdDeploymentOptions(data[0]['option'])
        if option in list(OsdDeploymentOptions):
            try:
                predefined_drive_groups[
                    option]['encrypted'] = data[0]['encrypted']
                orch.osds.create([DriveGroupSpec.from_json(
                    predefined_drive_groups[option])])
            except (ValueError, TypeError, KeyError, DriveGroupValidationError) as e:
                raise DashboardException(e, component='osd')

    def _create_bare(self, data):
        """Create a OSD container that has no associated device.

        :param data: contain attributes to create a bare OSD.
        :    `uuid`: will be set automatically if the OSD starts up
        :    `svc_id`: the ID is only used if a valid uuid is given.
        """
        try:
            uuid = data['uuid']
            svc_id = int(data['svc_id'])
        except (KeyError, ValueError) as e:
            raise DashboardException(e, component='osd', http_status_code=400)

        result = CephService.send_command(
            'mon', 'osd create', id=svc_id, uuid=uuid)
        return {
            'result': result,
            'svc_id': svc_id,
            'uuid': uuid,
        }

    @raise_if_no_orchestrator([OrchFeature.OSD_CREATE])
    @handle_orchestrator_error('osd')
    def _create_with_drive_groups(self, drive_groups):
        """Create OSDs with DriveGroups."""
        orch = OrchClient.instance()
        try:
            dg_specs = [DriveGroupSpec.from_json(dg) for dg in drive_groups]
            orch.osds.create(dg_specs)
        except (ValueError, TypeError, DriveGroupValidationError) as e:
            raise DashboardException(e, component='osd')

    @CreatePermission
    @osd_task('create', {'tracking_id': '{tracking_id}'})
    def create(self, method, data, tracking_id):  # pylint: disable=unused-argument
        if method == 'bare':
            return self._create_bare(data)
        if method == 'drive_groups':
            return self._create_with_drive_groups(data)
        if method == 'predefined':
            return self._create_predefined_drive_group(data)
        raise DashboardException(
            component='osd', http_status_code=400, msg='Unknown method: {}'.format(method))

    @RESTController.Resource('POST')
    @allow_empty_body
    def purge(self, svc_id):
        """
        Note: osd must be marked `down` before removal.
        """
        CephService.send_command('mon', 'osd purge-actual', id=int(svc_id),
                                 yes_i_really_mean_it=True)

    @RESTController.Resource('POST')
    @allow_empty_body
    def destroy(self, svc_id):
        """
        Mark osd as being destroyed. Keeps the ID intact (allowing reuse), but
        removes cephx keys, config-key data and lockbox keys, rendering data
        permanently unreadable.

        The osd must be marked down before being destroyed.
        """
        CephService.send_command(
            'mon', 'osd destroy-actual', id=int(svc_id), yes_i_really_mean_it=True)

    @Endpoint('GET', query_params=['ids'])
    @ReadPermission
    @EndpointDoc("Check If OSD is Safe to Destroy",
                 parameters={
                     'ids': (str, 'OSD Service Identifier'),
                 },
                 responses={200: SAFE_TO_DESTROY_SCHEMA})
    def safe_to_destroy(self, ids):
        """
        :type ids: int|[int]
        """

        ids = json.loads(ids)
        if isinstance(ids, list):
            ids = list(map(str, ids))
        else:
            ids = [str(ids)]

        try:
            result = CephService.send_command(
                'mon', 'osd safe-to-destroy', ids=ids, target=('mgr', ''))
            result['is_safe_to_destroy'] = set(result['safe_to_destroy']) == set(map(int, ids))
            return result

        except SendCommandError as e:
            return {
                'message': str(e),
                'is_safe_to_destroy': False,
            }

    @Endpoint('GET', query_params=['svc_ids'])
    @ReadPermission
    @raise_if_no_orchestrator()
    @handle_orchestrator_error('osd')
    def safe_to_delete(self, svc_ids):
        """
        :type ids: int|[int]
        """
        check = self._check_delete(svc_ids)
        return {
            'is_safe_to_delete': check.get('safe', False),
            'message': check.get('message', '')
        }

    @RESTController.Resource('GET')
    def devices(self, svc_id):
        # type: (str) -> Union[list, str]
        devices: Union[list, str] = CephService.send_command(
            'mon', 'device ls-by-daemon', who='osd.{}'.format(svc_id))
        mgr_map = mgr.get('mgr_map')
        available_modules = [m['name'] for m in mgr_map['available_modules']]

        life_expectancy_enabled = any(
            item.startswith('diskprediction_') for item in available_modules)
        for device in devices:
            device['life_expectancy_enabled'] = life_expectancy_enabled

        return devices


@UIRouter('/osd', Scope.OSD)
@APIDoc("Dashboard UI helper function; not part of the public API", "OsdUI")
class OsdUi(Osd):
    @Endpoint('GET')
    @ReadPermission
    @raise_if_no_orchestrator([OrchFeature.DAEMON_LIST])
    @handle_orchestrator_error('host')
    def deployment_options(self):
        orch = OrchClient.instance()
        hdds = 0
        ssds = 0
        nvmes = 0
        res = DeploymentOptions()

        for inventory_host in orch.inventory.list(hosts=None, refresh=True):
            for device in inventory_host.devices.devices:
                if device.available:
                    if device.human_readable_type == 'hdd':
                        hdds += 1
                    # SSDs and NVMe are both counted as 'ssd'
                    # so differentiating nvme using its path
                    elif '/dev/nvme' in device.path:
                        nvmes += 1
                    else:
                        ssds += 1

        if hdds:
            res.options[OsdDeploymentOptions.COST_CAPACITY].available = True
            res.recommended_option = OsdDeploymentOptions.COST_CAPACITY
        if hdds and ssds:
            res.options[OsdDeploymentOptions.THROUGHPUT].available = True
            res.recommended_option = OsdDeploymentOptions.THROUGHPUT
        if nvmes:
            res.options[OsdDeploymentOptions.IOPS].available = True

        return res.as_dict()


@APIRouter('/osd/flags', Scope.OSD)
@APIDoc(group='OSD')
class OsdFlagsController(RESTController):
    @staticmethod
    def _osd_flags():
        enabled_flags = mgr.get('osd_map')['flags_set']
        if 'pauserd' in enabled_flags and 'pausewr' in enabled_flags:
            # 'pause' is set by calling `ceph osd set pause` and unset by
            # calling `set osd unset pause`, but `ceph osd dump | jq '.flags'`
            # will contain 'pauserd,pausewr' if pause is set.
            # Let's pretend to the API that 'pause' is in fact a proper flag.
            enabled_flags = list(
                set(enabled_flags) - {'pauserd', 'pausewr'} | {'pause'})
        return sorted(enabled_flags)

    @staticmethod
    def _update_flags(action, flags, ids=None):
        if ids:
            if flags:
                ids = list(map(str, ids))
                CephService.send_command('mon', 'osd ' + action, who=ids,
                                         flags=','.join(flags))
        else:
            for flag in flags:
                CephService.send_command('mon', 'osd ' + action, '', key=flag)

    @EndpointDoc("Display OSD Flags",
                 responses={200: EXPORT_FLAGS_SCHEMA})
    def list(self):
        return self._osd_flags()

    @EndpointDoc('Sets OSD flags for the entire cluster.',
                 parameters={
                     'flags': ([str], 'List of flags to set. The flags `recovery_deletes`, '
                                      '`sortbitwise` and `pglog_hardlimit` cannot be unset. '
                                      'Additionally `purged_snapshots` cannot even be set.')
                 },
                 responses={200: EXPORT_FLAGS_SCHEMA})
    def bulk_set(self, flags):
        """
        The `recovery_deletes`, `sortbitwise` and `pglog_hardlimit` flags cannot be unset.
        `purged_snapshots` cannot even be set. It is therefore required to at
        least include those four flags for a successful operation.
        """
        assert isinstance(flags, list)

        enabled_flags = set(self._osd_flags())
        data = set(flags)
        added = data - enabled_flags
        removed = enabled_flags - data

        self._update_flags('set', added)
        self._update_flags('unset', removed)

        logger.info('Changed OSD flags: added=%s removed=%s', added, removed)

        return sorted(enabled_flags - removed | added)

    @Endpoint('PUT', 'individual')
    @UpdatePermission
    @EndpointDoc('Sets OSD flags for a subset of individual OSDs.',
                 parameters={
                     'flags': ({'noout': (bool, 'Sets/unsets `noout`', True, None),
                                'noin': (bool, 'Sets/unsets `noin`', True, None),
                                'noup': (bool, 'Sets/unsets `noup`', True, None),
                                'nodown': (bool, 'Sets/unsets `nodown`', True, None)},
                               'Directory of flags to set or unset. The flags '
                               '`noin`, `noout`, `noup` and `nodown` are going to '
                               'be considered only.'),
                     'ids': ([int], 'List of OSD ids the flags should be applied '
                                    'to.')
                 },
                 responses={200: EXPORT_INDIV_FLAGS_SCHEMA})
    def set_individual(self, flags, ids):
        """
        Updates flags (`noout`, `noin`, `nodown`, `noup`) for an individual
        subset of OSDs.
        """
        assert isinstance(flags, dict)
        assert isinstance(ids, list)
        assert all(isinstance(id, int) for id in ids)

        # These are to only flags that can be applied to an OSD individually.
        all_flags = {'noin', 'noout', 'nodown', 'noup'}
        added = set()
        removed = set()
        for flag, activated in flags.items():
            if flag in all_flags:
                if activated is not None:
                    if activated:
                        added.add(flag)
                    else:
                        removed.add(flag)

        self._update_flags('set-group', added, ids)
        self._update_flags('unset-group', removed, ids)

        logger.error('Changed individual OSD flags: added=%s removed=%s for ids=%s',
                     added, removed, ids)

        return {'added': sorted(added),
                'removed': sorted(removed),
                'ids': ids}

    @Endpoint('GET', 'individual')
    @ReadPermission
    @EndpointDoc('Displays individual OSD flags',
                 responses={200: EXPORT_INDIV_FLAGS_GET_SCHEMA})
    def get_individual(self):
        osd_map = mgr.get('osd_map')['osds']
        resp = []

        for osd in osd_map:
            resp.append({
                'osd': osd['osd'],
                'flags': osd['state']
            })
        return resp
