# -*- coding: utf-8 -*-
# pylint: disable=C0302
# pylint: disable=too-many-branches
# pylint: disable=too-many-lines

import json
import re
from copy import deepcopy
from typing import Any, Dict, List, no_type_check

import cherrypy
import rados
import rbd

from .. import mgr
from ..exceptions import DashboardException
from ..rest_client import RequestException
from ..security import Scope
from ..services.exception import handle_request_error
from ..services.iscsi_cli import IscsiGatewaysConfig
from ..services.iscsi_client import IscsiClient
from ..services.iscsi_config import IscsiGatewayDoesNotExist
from ..services.rbd import format_bitmask
from ..services.tcmu_service import TcmuService
from ..tools import TaskManager, str_to_bool
from . import ApiController, BaseController, ControllerDoc, Endpoint, \
    EndpointDoc, ReadPermission, RESTController, Task, UiApiController, \
    UpdatePermission

ISCSI_SCHEMA = {
    'user': (str, 'username'),
    'password': (str, 'password'),
    'mutual_user': (str, ''),
    'mutual_password': (str, '')
}


@UiApiController('/iscsi', Scope.ISCSI)
class IscsiUi(BaseController):

    REQUIRED_CEPH_ISCSI_CONFIG_MIN_VERSION = 10
    REQUIRED_CEPH_ISCSI_CONFIG_MAX_VERSION = 11

    @Endpoint()
    @ReadPermission
    @no_type_check
    def status(self):
        status = {'available': False}
        try:
            gateway = get_available_gateway()
        except DashboardException as e:
            status['message'] = str(e)
            return status
        try:
            config = IscsiClient.instance(gateway_name=gateway).get_config()
            if config['version'] < IscsiUi.REQUIRED_CEPH_ISCSI_CONFIG_MIN_VERSION or \
                    config['version'] > IscsiUi.REQUIRED_CEPH_ISCSI_CONFIG_MAX_VERSION:
                status['message'] = 'Unsupported `ceph-iscsi` config version. ' \
                                    'Expected >= {} and <= {} but found' \
                                    ' {}.'.format(IscsiUi.REQUIRED_CEPH_ISCSI_CONFIG_MIN_VERSION,
                                                  IscsiUi.REQUIRED_CEPH_ISCSI_CONFIG_MAX_VERSION,
                                                  config['version'])
                return status
            status['available'] = True
        except RequestException as e:
            if e.content:
                try:
                    content = json.loads(e.content)
                    content_message = content.get('message')
                except ValueError:
                    content_message = e.content
                if content_message:
                    status['message'] = content_message

        return status

    @Endpoint()
    @ReadPermission
    def version(self):
        gateway = get_available_gateway()
        config = IscsiClient.instance(gateway_name=gateway).get_config()
        return {
            'ceph_iscsi_config_version': config['version']
        }

    @Endpoint()
    @ReadPermission
    def settings(self):
        gateway = get_available_gateway()
        settings = IscsiClient.instance(gateway_name=gateway).get_settings()
        if 'target_controls_limits' in settings:
            target_default_controls = settings['target_default_controls']
            for ctrl_k, ctrl_v in target_default_controls.items():
                limits = settings['target_controls_limits'].get(ctrl_k, {})
                if 'type' not in limits:
                    # default
                    limits['type'] = 'int'
                    # backward compatibility
                    if target_default_controls[ctrl_k] in ['Yes', 'No']:
                        limits['type'] = 'bool'
                        target_default_controls[ctrl_k] = str_to_bool(ctrl_v)
                settings['target_controls_limits'][ctrl_k] = limits
        if 'disk_controls_limits' in settings:
            for backstore, disk_controls_limits in settings['disk_controls_limits'].items():
                disk_default_controls = settings['disk_default_controls'][backstore]
                for ctrl_k, ctrl_v in disk_default_controls.items():
                    limits = disk_controls_limits.get(ctrl_k, {})
                    if 'type' not in limits:
                        # default
                        limits['type'] = 'int'
                    settings['disk_controls_limits'][backstore][ctrl_k] = limits
        return settings

    @Endpoint()
    @ReadPermission
    def portals(self):
        portals = []
        gateways_config = IscsiGatewaysConfig.get_gateways_config()
        for name in gateways_config['gateways']:
            try:
                ip_addresses = IscsiClient.instance(gateway_name=name).get_ip_addresses()
                portals.append({'name': name, 'ip_addresses': ip_addresses['data']})
            except RequestException:
                pass
        return sorted(portals, key=lambda p: '{}.{}'.format(p['name'], p['ip_addresses']))

    @Endpoint()
    @ReadPermission
    def overview(self):
        gateways_names = IscsiGatewaysConfig.get_gateways_config()['gateways'].keys()
        config = None
        for gateway_name in gateways_names:
            try:
                config = IscsiClient.instance(gateway_name=gateway_name).get_config()
                break
            except RequestException:
                pass

        result_gateways = self._get_gateways_info(gateways_names, config)
        result_images = self._get_images_info(config)

        return {
            'gateways': sorted(result_gateways, key=lambda g: g['name']),
            'images': sorted(result_images, key=lambda i: '{}/{}'.format(i['pool'], i['image']))
        }

    def _get_images_info(self, config):
        # Images info
        result_images = []
        if config:
            tcmu_info = TcmuService.get_iscsi_info()
            for _, disk_config in config['disks'].items():
                image = {
                    'pool': disk_config['pool'],
                    'image': disk_config['image'],
                    'backstore': disk_config['backstore'],
                    'optimized_since': None,
                    'stats': None,
                    'stats_history': None
                }
                tcmu_image_info = TcmuService.get_image_info(image['pool'],
                                                             image['image'],
                                                             tcmu_info)
                if tcmu_image_info:
                    if 'optimized_since' in tcmu_image_info:
                        image['optimized_since'] = tcmu_image_info['optimized_since']
                    if 'stats' in tcmu_image_info:
                        image['stats'] = tcmu_image_info['stats']
                    if 'stats_history' in tcmu_image_info:
                        image['stats_history'] = tcmu_image_info['stats_history']
                result_images.append(image)
        return result_images

    def _get_gateways_info(self, gateways_names, config):
        result_gateways = []
        # Gateways info
        for gateway_name in gateways_names:
            gateway = {
                'name': gateway_name,
                'state': '',
                'num_targets': 'n/a',
                'num_sessions': 'n/a'
            }
            try:
                IscsiClient.instance(gateway_name=gateway_name).ping()
                gateway['state'] = 'up'
                if config:
                    gateway['num_sessions'] = 0
                    if gateway_name in config['gateways']:
                        gatewayinfo = IscsiClient.instance(
                            gateway_name=gateway_name).get_gatewayinfo()
                        gateway['num_sessions'] = gatewayinfo['num_sessions']
            except RequestException:
                gateway['state'] = 'down'
            if config:
                gateway['num_targets'] = len([target for _, target in config['targets'].items()
                                              if gateway_name in target['portals']])
            result_gateways.append(gateway)
        return result_gateways


@ApiController('/iscsi', Scope.ISCSI)
@ControllerDoc("Iscsi Management API", "Iscsi")
class Iscsi(BaseController):
    @Endpoint('GET', 'discoveryauth')
    @ReadPermission
    @EndpointDoc("Get Iscsi discoveryauth Details",
                 responses={'200': [ISCSI_SCHEMA]})
    def get_discoveryauth(self):
        gateway = get_available_gateway()
        return self._get_discoveryauth(gateway)

    @Endpoint('PUT', 'discoveryauth',
              query_params=['user', 'password', 'mutual_user', 'mutual_password'])
    @UpdatePermission
    @EndpointDoc("Set Iscsi discoveryauth",
                 parameters={
                     'user': (str, 'Username'),
                     'password': (str, 'Password'),
                     'mutual_user': (str, 'Mutual UserName'),
                     'mutual_password': (str, 'Mutual Password'),
                 })
    def set_discoveryauth(self, user, password, mutual_user, mutual_password):
        validate_auth({
            'user': user,
            'password': password,
            'mutual_user': mutual_user,
            'mutual_password': mutual_password
        })

        gateway = get_available_gateway()
        config = IscsiClient.instance(gateway_name=gateway).get_config()
        gateway_names = list(config['gateways'].keys())
        validate_rest_api(gateway_names)
        IscsiClient.instance(gateway_name=gateway).update_discoveryauth(user,
                                                                        password,
                                                                        mutual_user,
                                                                        mutual_password)
        return self._get_discoveryauth(gateway)

    def _get_discoveryauth(self, gateway):
        config = IscsiClient.instance(gateway_name=gateway).get_config()
        user = config['discovery_auth']['username']
        password = config['discovery_auth']['password']
        mutual_user = config['discovery_auth']['mutual_username']
        mutual_password = config['discovery_auth']['mutual_password']
        return {
            'user': user,
            'password': password,
            'mutual_user': mutual_user,
            'mutual_password': mutual_password
        }


def iscsi_target_task(name, metadata, wait_for=2.0):
    return Task("iscsi/target/{}".format(name), metadata, wait_for)


@ApiController('/iscsi/target', Scope.ISCSI)
@ControllerDoc("Get Iscsi Target Details", "IscsiTarget")
class IscsiTarget(RESTController):

    def list(self):
        gateway = get_available_gateway()
        config = IscsiClient.instance(gateway_name=gateway).get_config()
        targets = []
        for target_iqn in config['targets'].keys():
            target = IscsiTarget._config_to_target(target_iqn, config)
            IscsiTarget._set_info(target)
            targets.append(target)
        return targets

    def get(self, target_iqn):
        gateway = get_available_gateway()
        config = IscsiClient.instance(gateway_name=gateway).get_config()
        if target_iqn not in config['targets']:
            raise cherrypy.HTTPError(404)
        target = IscsiTarget._config_to_target(target_iqn, config)
        IscsiTarget._set_info(target)
        return target

    @iscsi_target_task('delete', {'target_iqn': '{target_iqn}'})
    def delete(self, target_iqn):
        gateway = get_available_gateway()
        config = IscsiClient.instance(gateway_name=gateway).get_config()
        if target_iqn not in config['targets']:
            raise DashboardException(msg='Target does not exist',
                                     code='target_does_not_exist',
                                     component='iscsi')
        portal_names = list(config['targets'][target_iqn]['portals'].keys())
        validate_rest_api(portal_names)
        if portal_names:
            portal_name = portal_names[0]
            target_info = IscsiClient.instance(gateway_name=portal_name).get_targetinfo(target_iqn)
            if target_info['num_sessions'] > 0:
                raise DashboardException(msg='Target has active sessions',
                                         code='target_has_active_sessions',
                                         component='iscsi')
        IscsiTarget._delete(target_iqn, config, 0, 100)

    @iscsi_target_task('create', {'target_iqn': '{target_iqn}'})
    def create(self, target_iqn=None, target_controls=None, acl_enabled=None,
               auth=None, portals=None, disks=None, clients=None, groups=None):
        target_controls = target_controls or {}
        portals = portals or []
        disks = disks or []
        clients = clients or []
        groups = groups or []

        validate_auth(auth)
        for client in clients:
            validate_auth(client['auth'])

        gateway = get_available_gateway()
        config = IscsiClient.instance(gateway_name=gateway).get_config()
        if target_iqn in config['targets']:
            raise DashboardException(msg='Target already exists',
                                     code='target_already_exists',
                                     component='iscsi')
        settings = IscsiClient.instance(gateway_name=gateway).get_settings()
        IscsiTarget._validate(target_iqn, target_controls, portals, disks, groups, settings)

        IscsiTarget._create(target_iqn, target_controls, acl_enabled, auth, portals, disks,
                            clients, groups, 0, 100, config, settings)

    @iscsi_target_task('edit', {'target_iqn': '{target_iqn}'})
    def set(self, target_iqn, new_target_iqn=None, target_controls=None, acl_enabled=None,
            auth=None, portals=None, disks=None, clients=None, groups=None):
        target_controls = target_controls or {}
        portals = IscsiTarget._sorted_portals(portals)
        disks = IscsiTarget._sorted_disks(disks)
        clients = IscsiTarget._sorted_clients(clients)
        groups = IscsiTarget._sorted_groups(groups)

        validate_auth(auth)
        for client in clients:
            validate_auth(client['auth'])

        gateway = get_available_gateway()
        config = IscsiClient.instance(gateway_name=gateway).get_config()
        if target_iqn not in config['targets']:
            raise DashboardException(msg='Target does not exist',
                                     code='target_does_not_exist',
                                     component='iscsi')
        if target_iqn != new_target_iqn and new_target_iqn in config['targets']:
            raise DashboardException(msg='Target IQN already in use',
                                     code='target_iqn_already_in_use',
                                     component='iscsi')

        settings = IscsiClient.instance(gateway_name=gateway).get_settings()
        new_portal_names = {p['host'] for p in portals}
        old_portal_names = set(config['targets'][target_iqn]['portals'].keys())
        deleted_portal_names = list(old_portal_names - new_portal_names)
        validate_rest_api(deleted_portal_names)
        IscsiTarget._validate(new_target_iqn, target_controls, portals, disks, groups, settings)
        IscsiTarget._validate_delete(gateway, target_iqn, config, new_target_iqn, target_controls,
                                     disks, clients, groups)
        config = IscsiTarget._delete(target_iqn, config, 0, 50, new_target_iqn, target_controls,
                                     portals, disks, clients, groups)
        IscsiTarget._create(new_target_iqn, target_controls, acl_enabled, auth, portals, disks,
                            clients, groups, 50, 100, config, settings)

    @staticmethod
    def _delete(target_iqn, config, task_progress_begin, task_progress_end, new_target_iqn=None,
                new_target_controls=None, new_portals=None, new_disks=None, new_clients=None,
                new_groups=None):
        new_target_controls = new_target_controls or {}
        new_portals = new_portals or []
        new_disks = new_disks or []
        new_clients = new_clients or []
        new_groups = new_groups or []

        TaskManager.current_task().set_progress(task_progress_begin)
        target_config = config['targets'][target_iqn]
        if not target_config['portals'].keys():
            raise DashboardException(msg="Cannot delete a target that doesn't contain any portal",
                                     code='cannot_delete_target_without_portals',
                                     component='iscsi')
        target = IscsiTarget._config_to_target(target_iqn, config)
        n_groups = len(target_config['groups'])
        n_clients = len(target_config['clients'])
        n_target_disks = len(target_config['disks'])
        task_progress_steps = n_groups + n_clients + n_target_disks
        task_progress_inc = 0
        if task_progress_steps != 0:
            task_progress_inc = int((task_progress_end - task_progress_begin) / task_progress_steps)

        gateway_name = list(target_config['portals'].keys())[0]
        IscsiTarget._delete_groups(target_config, target, new_target_iqn,
                                   new_target_controls, new_groups, gateway_name,
                                   target_iqn, task_progress_inc)
        deleted_clients, deleted_client_luns = IscsiTarget._delete_clients(
            target_config, target, new_target_iqn, new_target_controls, new_clients,
            gateway_name, target_iqn, new_groups, task_progress_inc)
        IscsiTarget._delete_disks(target_config, target, new_target_iqn, new_target_controls,
                                  new_disks, deleted_clients, new_groups, deleted_client_luns,
                                  gateway_name, target_iqn, task_progress_inc)
        IscsiTarget._delete_gateways(target, new_portals, gateway_name, target_iqn)
        if IscsiTarget._target_deletion_required(target, new_target_iqn, new_target_controls):
            IscsiClient.instance(gateway_name=gateway_name).delete_target(target_iqn)
        TaskManager.current_task().set_progress(task_progress_end)
        return IscsiClient.instance(gateway_name=gateway_name).get_config()

    @staticmethod
    def _delete_gateways(target, new_portals, gateway_name, target_iqn):
        old_portals_by_host = IscsiTarget._get_portals_by_host(target['portals'])
        new_portals_by_host = IscsiTarget._get_portals_by_host(new_portals)
        for old_portal_host, old_portal_ip_list in old_portals_by_host.items():
            if IscsiTarget._target_portal_deletion_required(old_portal_host,
                                                            old_portal_ip_list,
                                                            new_portals_by_host):
                IscsiClient.instance(gateway_name=gateway_name).delete_gateway(target_iqn,
                                                                               old_portal_host)

    @staticmethod
    def _delete_disks(target_config, target, new_target_iqn, new_target_controls,
                      new_disks, deleted_clients, new_groups, deleted_client_luns,
                      gateway_name, target_iqn, task_progress_inc):
        for image_id in target_config['disks']:
            if IscsiTarget._target_lun_deletion_required(target, new_target_iqn,
                                                         new_target_controls, new_disks, image_id):
                all_clients = target_config['clients'].keys()
                not_deleted_clients = [c for c in all_clients if c not in deleted_clients
                                       and not IscsiTarget._client_in_group(target['groups'], c)
                                       and not IscsiTarget._client_in_group(new_groups, c)]
                for client_iqn in not_deleted_clients:
                    client_image_ids = target_config['clients'][client_iqn]['luns'].keys()
                    for client_image_id in client_image_ids:
                        if image_id == client_image_id and \
                                (client_iqn, client_image_id) not in deleted_client_luns:
                            IscsiClient.instance(gateway_name=gateway_name).delete_client_lun(
                                target_iqn, client_iqn, client_image_id)
                IscsiClient.instance(gateway_name=gateway_name).delete_target_lun(target_iqn,
                                                                                  image_id)
                pool, image = image_id.split('/', 1)
                IscsiClient.instance(gateway_name=gateway_name).delete_disk(pool, image)
            TaskManager.current_task().inc_progress(task_progress_inc)

    @staticmethod
    def _delete_clients(target_config, target, new_target_iqn, new_target_controls,
                        new_clients, gateway_name, target_iqn, new_groups, task_progress_inc):
        deleted_clients = []
        deleted_client_luns = []
        for client_iqn, client_config in target_config['clients'].items():
            if IscsiTarget._client_deletion_required(target, new_target_iqn, new_target_controls,
                                                     new_clients, client_iqn):
                deleted_clients.append(client_iqn)
                IscsiClient.instance(gateway_name=gateway_name).delete_client(target_iqn,
                                                                              client_iqn)
            else:
                for image_id in list(client_config.get('luns', {}).keys()):
                    if IscsiTarget._client_lun_deletion_required(target, client_iqn, image_id,
                                                                 new_clients, new_groups):
                        deleted_client_luns.append((client_iqn, image_id))
                        IscsiClient.instance(gateway_name=gateway_name).delete_client_lun(
                            target_iqn, client_iqn, image_id)
            TaskManager.current_task().inc_progress(task_progress_inc)
        return deleted_clients, deleted_client_luns

    @staticmethod
    def _delete_groups(target_config, target, new_target_iqn, new_target_controls,
                       new_groups, gateway_name, target_iqn, task_progress_inc):
        for group_id in list(target_config['groups'].keys()):
            if IscsiTarget._group_deletion_required(target, new_target_iqn, new_target_controls,
                                                    new_groups, group_id):
                IscsiClient.instance(gateway_name=gateway_name).delete_group(target_iqn,
                                                                             group_id)
            else:
                group = IscsiTarget._get_group(new_groups, group_id)

                old_group_disks = set(target_config['groups'][group_id]['disks'].keys())
                new_group_disks = {'{}/{}'.format(x['pool'], x['image']) for x in group['disks']}
                local_deleted_disks = list(old_group_disks - new_group_disks)

                old_group_members = set(target_config['groups'][group_id]['members'])
                new_group_members = set(group['members'])
                local_deleted_members = list(old_group_members - new_group_members)

                if local_deleted_disks or local_deleted_members:
                    IscsiClient.instance(gateway_name=gateway_name).update_group(
                        target_iqn, group_id, local_deleted_members, local_deleted_disks)
            TaskManager.current_task().inc_progress(task_progress_inc)

    @staticmethod
    def _get_group(groups, group_id):
        for group in groups:
            if group['group_id'] == group_id:
                return group
        return None

    @staticmethod
    def _group_deletion_required(target, new_target_iqn, new_target_controls,
                                 new_groups, group_id):
        if IscsiTarget._target_deletion_required(target, new_target_iqn, new_target_controls):
            return True
        new_group = IscsiTarget._get_group(new_groups, group_id)
        if not new_group:
            return True
        return False

    @staticmethod
    def _get_client(clients, client_iqn):
        for client in clients:
            if client['client_iqn'] == client_iqn:
                return client
        return None

    @staticmethod
    def _client_deletion_required(target, new_target_iqn, new_target_controls,
                                  new_clients, client_iqn):
        if IscsiTarget._target_deletion_required(target, new_target_iqn, new_target_controls):
            return True
        new_client = IscsiTarget._get_client(new_clients, client_iqn)
        if not new_client:
            return True
        return False

    @staticmethod
    def _client_in_group(groups, client_iqn):
        for group in groups:
            if client_iqn in group['members']:
                return True
        return False

    @staticmethod
    def _client_lun_deletion_required(target, client_iqn, image_id, new_clients, new_groups):
        new_client = IscsiTarget._get_client(new_clients, client_iqn)
        if not new_client:
            return True

        # Disks inherited from groups must be considered
        was_in_group = IscsiTarget._client_in_group(target['groups'], client_iqn)
        is_in_group = IscsiTarget._client_in_group(new_groups, client_iqn)

        if not was_in_group and is_in_group:
            return True

        if is_in_group:
            return False

        new_lun = IscsiTarget._get_disk(new_client.get('luns', []), image_id)
        if not new_lun:
            return True

        old_client = IscsiTarget._get_client(target['clients'], client_iqn)
        if not old_client:
            return False

        old_lun = IscsiTarget._get_disk(old_client.get('luns', []), image_id)
        return new_lun != old_lun

    @staticmethod
    def _get_disk(disks, image_id):
        for disk in disks:
            if '{}/{}'.format(disk['pool'], disk['image']) == image_id:
                return disk
        return None

    @staticmethod
    def _target_lun_deletion_required(target, new_target_iqn, new_target_controls,
                                      new_disks, image_id):
        if IscsiTarget._target_deletion_required(target, new_target_iqn, new_target_controls):
            return True
        new_disk = IscsiTarget._get_disk(new_disks, image_id)
        if not new_disk:
            return True
        old_disk = IscsiTarget._get_disk(target['disks'], image_id)
        new_disk_without_controls = deepcopy(new_disk)
        new_disk_without_controls.pop('controls')
        old_disk_without_controls = deepcopy(old_disk)
        old_disk_without_controls.pop('controls')
        if new_disk_without_controls != old_disk_without_controls:
            return True
        return False

    @staticmethod
    def _target_portal_deletion_required(old_portal_host, old_portal_ip_list, new_portals_by_host):
        if old_portal_host not in new_portals_by_host:
            return True
        if sorted(old_portal_ip_list) != sorted(new_portals_by_host[old_portal_host]):
            return True
        return False

    @staticmethod
    def _target_deletion_required(target, new_target_iqn, new_target_controls):
        gateway = get_available_gateway()
        settings = IscsiClient.instance(gateway_name=gateway).get_settings()

        if target['target_iqn'] != new_target_iqn:
            return True
        if settings['api_version'] < 2 and target['target_controls'] != new_target_controls:
            return True
        return False

    @staticmethod
    def _validate(target_iqn, target_controls, portals, disks, groups, settings):
        if not target_iqn:
            raise DashboardException(msg='Target IQN is required',
                                     code='target_iqn_required',
                                     component='iscsi')

        minimum_gateways = max(1, settings['config']['minimum_gateways'])
        portals_by_host = IscsiTarget._get_portals_by_host(portals)
        if len(portals_by_host.keys()) < minimum_gateways:
            if minimum_gateways == 1:
                msg = 'At least one portal is required'
            else:
                msg = 'At least {} portals are required'.format(minimum_gateways)
            raise DashboardException(msg=msg,
                                     code='portals_required',
                                     component='iscsi')

        # 'target_controls_limits' was introduced in ceph-iscsi > 3.2
        # When using an older `ceph-iscsi` version these validations will
        # NOT be executed beforehand
        IscsiTarget._validate_target_controls_limits(settings, target_controls)
        portal_names = [p['host'] for p in portals]
        validate_rest_api(portal_names)
        IscsiTarget._validate_disks(disks, settings)
        IscsiTarget._validate_initiators(groups)

    @staticmethod
    def _validate_initiators(groups):
        initiators = []  # type: List[Any]
        for group in groups:
            initiators = initiators + group['members']
        if len(initiators) != len(set(initiators)):
            raise DashboardException(msg='Each initiator can only be part of 1 group at a time',
                                     code='initiator_in_multiple_groups',
                                     component='iscsi')

    @staticmethod
    def _validate_disks(disks, settings):
        for disk in disks:
            pool = disk['pool']
            image = disk['image']
            backstore = disk['backstore']
            required_rbd_features = settings['required_rbd_features'][backstore]
            unsupported_rbd_features = settings['unsupported_rbd_features'][backstore]
            IscsiTarget._validate_image(pool, image, backstore, required_rbd_features,
                                        unsupported_rbd_features)
            IscsiTarget._validate_disk_controls_limits(settings, disk, backstore)

    @staticmethod
    def _validate_disk_controls_limits(settings, disk, backstore):
        # 'disk_controls_limits' was introduced in ceph-iscsi > 3.2
        # When using an older `ceph-iscsi` version these validations will
        # NOT be executed beforehand
        if 'disk_controls_limits' in settings:
            for disk_control_name, disk_control_value in disk['controls'].items():
                limits = settings['disk_controls_limits'][backstore].get(disk_control_name)
                if limits is not None:
                    min_value = limits.get('min')
                    if min_value is not None and disk_control_value < min_value:
                        raise DashboardException(msg='Disk control {} must be >= '
                                                     '{}'.format(disk_control_name, min_value),
                                                 code='disk_control_invalid_min',
                                                 component='iscsi')
                    max_value = limits.get('max')
                    if max_value is not None and disk_control_value > max_value:
                        raise DashboardException(msg='Disk control {} must be <= '
                                                     '{}'.format(disk_control_name, max_value),
                                                 code='disk_control_invalid_max',
                                                 component='iscsi')

    @staticmethod
    def _validate_target_controls_limits(settings, target_controls):
        if 'target_controls_limits' in settings:
            for target_control_name, target_control_value in target_controls.items():
                limits = settings['target_controls_limits'].get(target_control_name)
                if limits is not None:
                    min_value = limits.get('min')
                    if min_value is not None and target_control_value < min_value:
                        raise DashboardException(msg='Target control {} must be >= '
                                                     '{}'.format(target_control_name, min_value),
                                                 code='target_control_invalid_min',
                                                 component='iscsi')
                    max_value = limits.get('max')
                    if max_value is not None and target_control_value > max_value:
                        raise DashboardException(msg='Target control {} must be <= '
                                                     '{}'.format(target_control_name, max_value),
                                                 code='target_control_invalid_max',
                                                 component='iscsi')

    @staticmethod
    def _validate_image(pool, image, backstore, required_rbd_features, unsupported_rbd_features):
        try:
            ioctx = mgr.rados.open_ioctx(pool)
            try:
                with rbd.Image(ioctx, image) as img:
                    if img.features() & required_rbd_features != required_rbd_features:
                        raise DashboardException(msg='Image {} cannot be exported using {} '
                                                     'backstore because required features are '
                                                     'missing (required features are '
                                                     '{})'.format(image,
                                                                  backstore,
                                                                  format_bitmask(
                                                                      required_rbd_features)),
                                                 code='image_missing_required_features',
                                                 component='iscsi')
                    if img.features() & unsupported_rbd_features != 0:
                        raise DashboardException(msg='Image {} cannot be exported using {} '
                                                     'backstore because it contains unsupported '
                                                     'features ('
                                                     '{})'.format(image,
                                                                  backstore,
                                                                  format_bitmask(
                                                                      unsupported_rbd_features)),
                                                 code='image_contains_unsupported_features',
                                                 component='iscsi')

            except rbd.ImageNotFound:
                raise DashboardException(msg='Image {} does not exist'.format(image),
                                         code='image_does_not_exist',
                                         component='iscsi')
        except rados.ObjectNotFound:
            raise DashboardException(msg='Pool {} does not exist'.format(pool),
                                     code='pool_does_not_exist',
                                     component='iscsi')

    @staticmethod
    def _validate_delete(gateway, target_iqn, config, new_target_iqn=None, new_target_controls=None,
                         new_disks=None, new_clients=None, new_groups=None):
        new_target_controls = new_target_controls or {}
        new_disks = new_disks or []
        new_clients = new_clients or []
        new_groups = new_groups or []

        target_config = config['targets'][target_iqn]
        target = IscsiTarget._config_to_target(target_iqn, config)
        for client_iqn in list(target_config['clients'].keys()):
            if IscsiTarget._client_deletion_required(target, new_target_iqn, new_target_controls,
                                                     new_clients, client_iqn):
                client_info = IscsiClient.instance(gateway_name=gateway).get_clientinfo(target_iqn,
                                                                                        client_iqn)
                if client_info.get('state', {}).get('LOGGED_IN', []):
                    raise DashboardException(msg="Client '{}' cannot be deleted until it's logged "
                                             "out".format(client_iqn),
                                             code='client_logged_in',
                                             component='iscsi')

    @staticmethod
    def _update_targetauth(config, target_iqn, auth, gateway_name):
        # Target level authentication was introduced in ceph-iscsi config v11
        if config['version'] > 10:
            user = auth['user']
            password = auth['password']
            mutual_user = auth['mutual_user']
            mutual_password = auth['mutual_password']
            IscsiClient.instance(gateway_name=gateway_name).update_targetauth(target_iqn,
                                                                              user,
                                                                              password,
                                                                              mutual_user,
                                                                              mutual_password)

    @staticmethod
    def _update_targetacl(target_config, target_iqn, acl_enabled, gateway_name):
        if not target_config or target_config['acl_enabled'] != acl_enabled:
            targetauth_action = ('enable_acl' if acl_enabled else 'disable_acl')
            IscsiClient.instance(gateway_name=gateway_name).update_targetacl(target_iqn,
                                                                             targetauth_action)

    @staticmethod
    def _is_auth_equal(auth_config, auth):
        return auth['user'] == auth_config['username'] and \
            auth['password'] == auth_config['password'] and \
            auth['mutual_user'] == auth_config['mutual_username'] and \
            auth['mutual_password'] == auth_config['mutual_password']

    @staticmethod
    @handle_request_error('iscsi')
    def _create(target_iqn, target_controls, acl_enabled,
                auth, portals, disks, clients, groups,
                task_progress_begin, task_progress_end, config, settings):
        target_config = config['targets'].get(target_iqn, None)
        TaskManager.current_task().set_progress(task_progress_begin)
        portals_by_host = IscsiTarget._get_portals_by_host(portals)
        n_hosts = len(portals_by_host)
        n_disks = len(disks)
        n_clients = len(clients)
        n_groups = len(groups)
        task_progress_steps = n_hosts + n_disks + n_clients + n_groups
        task_progress_inc = 0
        if task_progress_steps != 0:
            task_progress_inc = int((task_progress_end - task_progress_begin) / task_progress_steps)
        gateway_name = portals[0]['host']
        if not target_config:
            IscsiClient.instance(gateway_name=gateway_name).create_target(target_iqn,
                                                                          target_controls)
        IscsiTarget._create_gateways(portals_by_host, target_config,
                                     gateway_name, target_iqn, task_progress_inc)

        update_acl = not target_config or \
            acl_enabled != target_config['acl_enabled'] or \
            not IscsiTarget._is_auth_equal(target_config['auth'], auth)
        if update_acl:
            IscsiTarget._update_acl(acl_enabled, config, target_iqn,
                                    auth, gateway_name, target_config)

        IscsiTarget._create_disks(disks, config, gateway_name, target_config,
                                  target_iqn, settings, task_progress_inc)
        IscsiTarget._create_clients(clients, target_config, gateway_name,
                                    target_iqn, groups, task_progress_inc)
        IscsiTarget._create_groups(groups, target_config, gateway_name,
                                   target_iqn, task_progress_inc, target_controls,
                                   task_progress_end)

    @staticmethod
    def _update_acl(acl_enabled, config, target_iqn, auth, gateway_name, target_config):
        if acl_enabled:
            IscsiTarget._update_targetauth(config, target_iqn, auth, gateway_name)
            IscsiTarget._update_targetacl(target_config, target_iqn, acl_enabled,
                                          gateway_name)
        else:
            IscsiTarget._update_targetacl(target_config, target_iqn, acl_enabled,
                                          gateway_name)
            IscsiTarget._update_targetauth(config, target_iqn, auth, gateway_name)

    @staticmethod
    def _create_gateways(portals_by_host, target_config, gateway_name, target_iqn,
                         task_progress_inc):
        for host, ip_list in portals_by_host.items():
            if not target_config or host not in target_config['portals']:
                IscsiClient.instance(gateway_name=gateway_name).create_gateway(target_iqn,
                                                                               host,
                                                                               ip_list)
            TaskManager.current_task().inc_progress(task_progress_inc)

    @staticmethod
    def _create_groups(groups, target_config, gateway_name, target_iqn, task_progress_inc,
                       target_controls, task_progress_end):
        for group in groups:
            group_id = group['group_id']
            members = group['members']
            image_ids = []
            for disk in group['disks']:
                image_ids.append('{}/{}'.format(disk['pool'], disk['image']))

            if target_config and group_id in target_config['groups']:
                old_members = target_config['groups'][group_id]['members']
                old_disks = target_config['groups'][group_id]['disks'].keys()

            if not target_config or group_id not in target_config['groups'] or \
                    list(set(group['members']) - set(old_members)) or \
                    list(set(image_ids) - set(old_disks)):
                IscsiClient.instance(gateway_name=gateway_name).create_group(
                    target_iqn, group_id, members, image_ids)
            TaskManager.current_task().inc_progress(task_progress_inc)
        if target_controls:
            if not target_config or target_controls != target_config['controls']:
                IscsiClient.instance(gateway_name=gateway_name).reconfigure_target(
                    target_iqn, target_controls)
        TaskManager.current_task().set_progress(task_progress_end)

    @staticmethod
    def _create_clients(clients, target_config, gateway_name, target_iqn, groups,
                        task_progress_inc):
        for client in clients:
            client_iqn = client['client_iqn']
            if not target_config or client_iqn not in target_config['clients']:
                IscsiClient.instance(gateway_name=gateway_name).create_client(target_iqn,
                                                                              client_iqn)
            if not target_config or client_iqn not in target_config['clients'] or \
                    not IscsiTarget._is_auth_equal(target_config['clients'][client_iqn]['auth'],
                                                   client['auth']):
                user = client['auth']['user']
                password = client['auth']['password']
                m_user = client['auth']['mutual_user']
                m_password = client['auth']['mutual_password']
                IscsiClient.instance(gateway_name=gateway_name).create_client_auth(
                    target_iqn, client_iqn, user, password, m_user, m_password)
            for lun in client['luns']:
                pool = lun['pool']
                image = lun['image']
                image_id = '{}/{}'.format(pool, image)
                # Disks inherited from groups must be considered
                group_disks = []
                for group in groups:
                    if client_iqn in group['members']:
                        group_disks = ['{}/{}'.format(x['pool'], x['image'])
                                       for x in group['disks']]
                if not target_config or client_iqn not in target_config['clients'] or \
                        (image_id not in target_config['clients'][client_iqn]['luns']
                         and image_id not in group_disks):
                    IscsiClient.instance(gateway_name=gateway_name).create_client_lun(
                        target_iqn, client_iqn, image_id)
            TaskManager.current_task().inc_progress(task_progress_inc)

    @staticmethod
    def _create_disks(disks, config, gateway_name, target_config, target_iqn, settings,
                      task_progress_inc):
        for disk in disks:
            pool = disk['pool']
            image = disk['image']
            image_id = '{}/{}'.format(pool, image)
            backstore = disk['backstore']
            wwn = disk.get('wwn')
            lun = disk.get('lun')
            if image_id not in config['disks']:
                IscsiClient.instance(gateway_name=gateway_name).create_disk(pool,
                                                                            image,
                                                                            backstore,
                                                                            wwn)
            if not target_config or image_id not in target_config['disks']:
                IscsiClient.instance(gateway_name=gateway_name).create_target_lun(target_iqn,
                                                                                  image_id,
                                                                                  lun)

            controls = disk['controls']
            d_conf_controls = {}
            if image_id in config['disks']:
                d_conf_controls = config['disks'][image_id]['controls']
                disk_default_controls = settings['disk_default_controls'][backstore]
                for old_control in d_conf_controls.keys():
                    # If control was removed, restore the default value
                    if old_control not in controls:
                        controls[old_control] = disk_default_controls[old_control]

            if (image_id not in config['disks'] or d_conf_controls != controls) and controls:
                IscsiClient.instance(gateway_name=gateway_name).reconfigure_disk(pool,
                                                                                 image,
                                                                                 controls)
            TaskManager.current_task().inc_progress(task_progress_inc)

    @staticmethod
    def _config_to_target(target_iqn, config):
        target_config = config['targets'][target_iqn]
        portals = []
        for host, portal_config in target_config['portals'].items():
            for portal_ip in portal_config['portal_ip_addresses']:
                portal = {
                    'host': host,
                    'ip': portal_ip
                }
                portals.append(portal)
        portals = IscsiTarget._sorted_portals(portals)
        disks = []
        for target_disk in target_config['disks']:
            disk_config = config['disks'][target_disk]
            disk = {
                'pool': disk_config['pool'],
                'image': disk_config['image'],
                'controls': disk_config['controls'],
                'backstore': disk_config['backstore'],
                'wwn': disk_config['wwn']
            }
            # lun_id was introduced in ceph-iscsi config v11
            if config['version'] > 10:
                disk['lun'] = target_config['disks'][target_disk]['lun_id']
            disks.append(disk)
        disks = IscsiTarget._sorted_disks(disks)
        clients = []
        for client_iqn, client_config in target_config['clients'].items():
            luns = []
            for client_lun in client_config['luns'].keys():
                pool, image = client_lun.split('/', 1)
                lun = {
                    'pool': pool,
                    'image': image
                }
                luns.append(lun)
            user = client_config['auth']['username']
            password = client_config['auth']['password']
            mutual_user = client_config['auth']['mutual_username']
            mutual_password = client_config['auth']['mutual_password']
            client = {
                'client_iqn': client_iqn,
                'luns': luns,
                'auth': {
                    'user': user,
                    'password': password,
                    'mutual_user': mutual_user,
                    'mutual_password': mutual_password
                }
            }
            clients.append(client)
        clients = IscsiTarget._sorted_clients(clients)
        groups = []
        for group_id, group_config in target_config['groups'].items():
            group_disks = []
            for group_disk_key, _ in group_config['disks'].items():
                pool, image = group_disk_key.split('/', 1)
                group_disk = {
                    'pool': pool,
                    'image': image
                }
                group_disks.append(group_disk)
            group = {
                'group_id': group_id,
                'disks': group_disks,
                'members': group_config['members'],
            }
            groups.append(group)
        groups = IscsiTarget._sorted_groups(groups)
        target_controls = target_config['controls']
        acl_enabled = target_config['acl_enabled']
        target = {
            'target_iqn': target_iqn,
            'portals': portals,
            'disks': disks,
            'clients': clients,
            'groups': groups,
            'target_controls': target_controls,
            'acl_enabled': acl_enabled
        }
        # Target level authentication was introduced in ceph-iscsi config v11
        if config['version'] > 10:
            target_user = target_config['auth']['username']
            target_password = target_config['auth']['password']
            target_mutual_user = target_config['auth']['mutual_username']
            target_mutual_password = target_config['auth']['mutual_password']
            target['auth'] = {
                'user': target_user,
                'password': target_password,
                'mutual_user': target_mutual_user,
                'mutual_password': target_mutual_password
            }
        return target

    @staticmethod
    def _is_executing(target_iqn):
        executing_tasks, _ = TaskManager.list()
        for t in executing_tasks:
            if t.name.startswith('iscsi/target') and t.metadata.get('target_iqn') == target_iqn:
                return True
        return False

    @staticmethod
    def _set_info(target):
        if not target['portals']:
            return
        target_iqn = target['target_iqn']
        # During task execution, additional info is not available
        if IscsiTarget._is_executing(target_iqn):
            return
        # If any portal is down, additional info is not available
        for portal in target['portals']:
            try:
                IscsiClient.instance(gateway_name=portal['host']).ping()
            except (IscsiGatewayDoesNotExist, RequestException):
                return
        gateway_name = target['portals'][0]['host']
        try:
            target_info = IscsiClient.instance(gateway_name=gateway_name).get_targetinfo(
                target_iqn)
            target['info'] = target_info
            for client in target['clients']:
                client_iqn = client['client_iqn']
                client_info = IscsiClient.instance(gateway_name=gateway_name).get_clientinfo(
                    target_iqn, client_iqn)
                client['info'] = client_info
        except RequestException as e:
            # Target/Client has been removed in the meanwhile (e.g. using gwcli)
            if e.status_code != 404:
                raise e

    @staticmethod
    def _sorted_portals(portals):
        portals = portals or []
        return sorted(portals, key=lambda p: '{}.{}'.format(p['host'], p['ip']))

    @staticmethod
    def _sorted_disks(disks):
        disks = disks or []
        return sorted(disks, key=lambda d: '{}.{}'.format(d['pool'], d['image']))

    @staticmethod
    def _sorted_clients(clients):
        clients = clients or []
        for client in clients:
            client['luns'] = sorted(client['luns'],
                                    key=lambda d: '{}.{}'.format(d['pool'], d['image']))
        return sorted(clients, key=lambda c: c['client_iqn'])

    @staticmethod
    def _sorted_groups(groups):
        groups = groups or []
        for group in groups:
            group['disks'] = sorted(group['disks'],
                                    key=lambda d: '{}.{}'.format(d['pool'], d['image']))
            group['members'] = sorted(group['members'])
        return sorted(groups, key=lambda g: g['group_id'])

    @staticmethod
    def _get_portals_by_host(portals):
        # type: (List[dict]) -> Dict[str, List[str]]
        portals_by_host = {}  # type: Dict[str, List[str]]
        for portal in portals:
            host = portal['host']
            ip = portal['ip']
            if host not in portals_by_host:
                portals_by_host[host] = []
            portals_by_host[host].append(ip)
        return portals_by_host


def get_available_gateway():
    gateways = IscsiGatewaysConfig.get_gateways_config()['gateways']
    if not gateways:
        raise DashboardException(msg='There are no gateways defined',
                                 code='no_gateways_defined',
                                 component='iscsi')
    for gateway in gateways:
        try:
            IscsiClient.instance(gateway_name=gateway).ping()
            return gateway
        except RequestException:
            pass
    raise DashboardException(msg='There are no gateways available',
                             code='no_gateways_available',
                             component='iscsi')


def validate_rest_api(gateways):
    for gateway in gateways:
        try:
            IscsiClient.instance(gateway_name=gateway).ping()
        except RequestException:
            raise DashboardException(msg='iSCSI REST Api not available for gateway '
                                         '{}'.format(gateway),
                                     code='ceph_iscsi_rest_api_not_available_for_gateway',
                                     component='iscsi')


def validate_auth(auth):
    username_regex = re.compile(r'^[\w\.:@_-]{8,64}$')
    password_regex = re.compile(r'^[\w@\-_\/]{12,16}$')
    result = True

    if auth['user'] or auth['password']:
        result = bool(username_regex.match(auth['user'])) and \
            bool(password_regex.match(auth['password']))

    if auth['mutual_user'] or auth['mutual_password']:
        result = result and bool(username_regex.match(auth['mutual_user'])) and \
            bool(password_regex.match(auth['mutual_password'])) and auth['user']

    if not result:
        raise DashboardException(msg='Bad authentication',
                                 code='target_bad_auth',
                                 component='iscsi')
