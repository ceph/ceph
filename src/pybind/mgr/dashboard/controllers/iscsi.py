# -*- coding: utf-8 -*-
# pylint: disable=too-many-branches
from __future__ import absolute_import

from copy import deepcopy
import json
import cherrypy

import rados
import rbd

from . import ApiController, UiApiController, RESTController, BaseController, Endpoint,\
    ReadPermission, UpdatePermission, Task
from .. import mgr
from ..rest_client import RequestException
from ..security import Scope
from ..services.iscsi_client import IscsiClient
from ..services.iscsi_cli import IscsiGatewaysConfig
from ..services.rbd import format_bitmask
from ..services.tcmu_service import TcmuService
from ..exceptions import DashboardException
from ..tools import str_to_bool, TaskManager


@UiApiController('/iscsi', Scope.ISCSI)
class IscsiUi(BaseController):

    REQUIRED_CEPH_ISCSI_CONFIG_MIN_VERSION = 10
    REQUIRED_CEPH_ISCSI_CONFIG_MAX_VERSION = 11

    @Endpoint()
    @ReadPermission
    def status(self):
        status = {'available': False}
        gateways = IscsiGatewaysConfig.get_gateways_config()['gateways']
        if not gateways:
            status['message'] = 'There are no gateways defined'
            return status
        try:
            for gateway in gateways:
                try:
                    IscsiClient.instance(gateway_name=gateway).ping()
                except RequestException:
                    status['message'] = 'Gateway {} is inaccessible'.format(gateway)
                    return status
            config = IscsiClient.instance().get_config()
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
        return {
            'ceph_iscsi_config_version': IscsiClient.instance().get_config()['version']
        }

    @Endpoint()
    @ReadPermission
    def settings(self):
        settings = IscsiClient.instance().get_settings()
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
            ip_addresses = IscsiClient.instance(gateway_name=name).get_ip_addresses()
            portals.append({'name': name, 'ip_addresses': ip_addresses['data']})
        return sorted(portals, key=lambda p: '{}.{}'.format(p['name'], p['ip_addresses']))

    @Endpoint()
    @ReadPermission
    def overview(self):
        result_gateways = []
        result_images = []
        gateways_names = IscsiGatewaysConfig.get_gateways_config()['gateways'].keys()
        config = None
        for gateway_name in gateways_names:
            try:
                config = IscsiClient.instance(gateway_name=gateway_name).get_config()
                break
            except RequestException:
                pass

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

        # Images info
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

        return {
            'gateways': sorted(result_gateways, key=lambda g: g['name']),
            'images': sorted(result_images, key=lambda i: '{}/{}'.format(i['pool'], i['image']))
        }


@ApiController('/iscsi', Scope.ISCSI)
class Iscsi(BaseController):

    @Endpoint('GET', 'discoveryauth')
    @ReadPermission
    def get_discoveryauth(self):
        return self._get_discoveryauth()

    @Endpoint('PUT', 'discoveryauth')
    @UpdatePermission
    def set_discoveryauth(self, user, password, mutual_user, mutual_password):
        IscsiClient.instance().update_discoveryauth(user, password, mutual_user, mutual_password)
        return self._get_discoveryauth()

    def _get_discoveryauth(self):
        config = IscsiClient.instance().get_config()
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
class IscsiTarget(RESTController):

    def list(self):
        config = IscsiClient.instance().get_config()
        targets = []
        for target_iqn in config['targets'].keys():
            target = IscsiTarget._config_to_target(target_iqn, config)
            IscsiTarget._set_info(target)
            targets.append(target)
        return targets

    def get(self, target_iqn):
        config = IscsiClient.instance().get_config()
        if target_iqn not in config['targets']:
            raise cherrypy.HTTPError(404)
        target = IscsiTarget._config_to_target(target_iqn, config)
        IscsiTarget._set_info(target)
        return target

    @iscsi_target_task('delete', {'target_iqn': '{target_iqn}'})
    def delete(self, target_iqn):
        config = IscsiClient.instance().get_config()
        if target_iqn not in config['targets']:
            raise DashboardException(msg='Target does not exist',
                                     code='target_does_not_exist',
                                     component='iscsi')
        if target_iqn not in config['targets']:
            raise DashboardException(msg='Target does not exist',
                                     code='target_does_not_exist',
                                     component='iscsi')
        target_info = IscsiClient.instance().get_targetinfo(target_iqn)
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

        config = IscsiClient.instance().get_config()
        if target_iqn in config['targets']:
            raise DashboardException(msg='Target already exists',
                                     code='target_already_exists',
                                     component='iscsi')
        settings = IscsiClient.instance().get_settings()
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

        config = IscsiClient.instance().get_config()
        if target_iqn not in config['targets']:
            raise DashboardException(msg='Target does not exist',
                                     code='target_does_not_exist',
                                     component='iscsi')
        if target_iqn != new_target_iqn and new_target_iqn in config['targets']:
            raise DashboardException(msg='Target IQN already in use',
                                     code='target_iqn_already_in_use',
                                     component='iscsi')
        settings = IscsiClient.instance().get_settings()
        IscsiTarget._validate(new_target_iqn, target_controls, portals, disks, groups, settings)
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
        deleted_groups = []
        for group_id in list(target_config['groups'].keys()):
            if IscsiTarget._group_deletion_required(target, new_target_iqn, new_target_controls,
                                                    new_groups, group_id, new_clients,
                                                    new_disks):
                deleted_groups.append(group_id)
                IscsiClient.instance(gateway_name=gateway_name).delete_group(target_iqn,
                                                                             group_id)
            TaskManager.current_task().inc_progress(task_progress_inc)
        for client_iqn in list(target_config['clients'].keys()):
            if IscsiTarget._client_deletion_required(target, new_target_iqn, new_target_controls,
                                                     new_clients, client_iqn,
                                                     new_groups, deleted_groups):
                IscsiClient.instance(gateway_name=gateway_name).delete_client(target_iqn,
                                                                              client_iqn)
            TaskManager.current_task().inc_progress(task_progress_inc)
        for image_id in target_config['disks']:
            if IscsiTarget._target_lun_deletion_required(target, new_target_iqn,
                                                         new_target_controls,
                                                         new_disks, image_id):
                IscsiClient.instance(gateway_name=gateway_name).delete_target_lun(target_iqn,
                                                                                  image_id)
                pool, image = image_id.split('/', 1)
                IscsiClient.instance(gateway_name=gateway_name).delete_disk(pool, image)
            TaskManager.current_task().inc_progress(task_progress_inc)
        old_portals_by_host = IscsiTarget._get_portals_by_host(target['portals'])
        new_portals_by_host = IscsiTarget._get_portals_by_host(new_portals)
        for old_portal_host, old_portal_ip_list in old_portals_by_host.items():
            if IscsiTarget._target_portal_deletion_required(old_portal_host,
                                                            old_portal_ip_list,
                                                            new_portals_by_host):
                IscsiClient.instance(gateway_name=gateway_name).delete_gateway(target_iqn,
                                                                               old_portal_host)
        if IscsiTarget._target_deletion_required(target, new_target_iqn, new_target_controls):
            IscsiClient.instance(gateway_name=gateway_name).delete_target(target_iqn)
        TaskManager.current_task().set_progress(task_progress_end)
        return IscsiClient.instance(gateway_name=gateway_name).get_config()

    @staticmethod
    def _get_group(groups, group_id):
        for group in groups:
            if group['group_id'] == group_id:
                return group
        return None

    @staticmethod
    def _group_deletion_required(target, new_target_iqn, new_target_controls,
                                 new_groups, group_id, new_clients, new_disks):
        if IscsiTarget._target_deletion_required(target, new_target_iqn, new_target_controls):
            return True
        new_group = IscsiTarget._get_group(new_groups, group_id)
        if not new_group:
            return True
        old_group = IscsiTarget._get_group(target['groups'], group_id)
        if new_group != old_group:
            return True
        # Check if any client inside this group has changed
        for client_iqn in new_group['members']:
            if IscsiTarget._client_deletion_required(target, new_target_iqn, new_target_controls,
                                                     new_clients, client_iqn,
                                                     new_groups, []):
                return True
        # Check if any disk inside this group has changed
        for disk in new_group['disks']:
            image_id = '{}/{}'.format(disk['pool'], disk['image'])
            if IscsiTarget._target_lun_deletion_required(target, new_target_iqn,
                                                         new_target_controls,
                                                         new_disks, image_id):
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
                                  new_clients, client_iqn, new_groups, deleted_groups):
        if IscsiTarget._target_deletion_required(target, new_target_iqn, new_target_controls):
            return True
        new_client = deepcopy(IscsiTarget._get_client(new_clients, client_iqn))
        if not new_client:
            return True
        # Disks inherited from groups must be considered
        for group in new_groups:
            if client_iqn in group['members']:
                new_client['luns'] += group['disks']
        old_client = IscsiTarget._get_client(target['clients'], client_iqn)
        if new_client != old_client:
            return True
        # Check if client belongs to a groups that has been deleted
        for group in target['groups']:
            if group['group_id'] in deleted_groups and client_iqn in group['members']:
                return True
        return False

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
        if target['target_iqn'] != new_target_iqn:
            return True
        if target['target_controls'] != new_target_controls:
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

        for portal in portals:
            gateway_name = portal['host']
            try:
                IscsiClient.instance(gateway_name=gateway_name).ping()
            except RequestException:
                raise DashboardException(msg='iSCSI REST Api not available for gateway '
                                             '{}'.format(gateway_name),
                                         code='ceph_iscsi_rest_api_not_available_for_gateway',
                                         component='iscsi')

        for disk in disks:
            pool = disk['pool']
            image = disk['image']
            backstore = disk['backstore']
            required_rbd_features = settings['required_rbd_features'][backstore]
            unsupported_rbd_features = settings['unsupported_rbd_features'][backstore]
            IscsiTarget._validate_image(pool, image, backstore, required_rbd_features,
                                        unsupported_rbd_features)

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

        initiators = []
        for group in groups:
            initiators = initiators + group['members']
        if len(initiators) != len(set(initiators)):
            raise DashboardException(msg='Each initiator can only be part of 1 group at a time',
                                     code='initiator_in_multiple_groups',
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
        try:
            gateway_name = portals[0]['host']
            if not target_config:
                IscsiClient.instance(gateway_name=gateway_name).create_target(target_iqn,
                                                                              target_controls)
            for host, ip_list in portals_by_host.items():
                if not target_config or host not in target_config['portals']:
                    IscsiClient.instance(gateway_name=gateway_name).create_gateway(target_iqn,
                                                                                   host,
                                                                                   ip_list)
                TaskManager.current_task().inc_progress(task_progress_inc)

            if acl_enabled:
                IscsiTarget._update_targetauth(config, target_iqn, auth, gateway_name)
                IscsiTarget._update_targetacl(target_config, target_iqn, acl_enabled, gateway_name)

            else:
                IscsiTarget._update_targetacl(target_config, target_iqn, acl_enabled, gateway_name)
                IscsiTarget._update_targetauth(config, target_iqn, auth, gateway_name)

            for disk in disks:
                pool = disk['pool']
                image = disk['image']
                image_id = '{}/{}'.format(pool, image)
                backstore = disk['backstore']
                if image_id not in config['disks']:
                    IscsiClient.instance(gateway_name=gateway_name).create_disk(pool,
                                                                                image,
                                                                                backstore)
                if not target_config or image_id not in target_config['disks']:
                    IscsiClient.instance(gateway_name=gateway_name).create_target_lun(target_iqn,
                                                                                      image_id)

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
            for client in clients:
                client_iqn = client['client_iqn']
                if not target_config or client_iqn not in target_config['clients']:
                    IscsiClient.instance(gateway_name=gateway_name).create_client(target_iqn,
                                                                                  client_iqn)
                    for lun in client['luns']:
                        pool = lun['pool']
                        image = lun['image']
                        image_id = '{}/{}'.format(pool, image)
                        IscsiClient.instance(gateway_name=gateway_name).create_client_lun(
                            target_iqn, client_iqn, image_id)
                    user = client['auth']['user']
                    password = client['auth']['password']
                    m_user = client['auth']['mutual_user']
                    m_password = client['auth']['mutual_password']
                    IscsiClient.instance(gateway_name=gateway_name).create_client_auth(
                        target_iqn, client_iqn, user, password, m_user, m_password)
                TaskManager.current_task().inc_progress(task_progress_inc)
            for group in groups:
                group_id = group['group_id']
                members = group['members']
                image_ids = []
                for disk in group['disks']:
                    image_ids.append('{}/{}'.format(disk['pool'], disk['image']))
                if not target_config or group_id not in target_config['groups']:
                    IscsiClient.instance(gateway_name=gateway_name).create_group(
                        target_iqn, group_id, members, image_ids)
                TaskManager.current_task().inc_progress(task_progress_inc)
            if target_controls:
                if not target_config or target_controls != target_config['controls']:
                    IscsiClient.instance(gateway_name=gateway_name).reconfigure_target(
                        target_iqn, target_controls)
            TaskManager.current_task().set_progress(task_progress_end)
        except RequestException as e:
            if e.content:
                content = json.loads(e.content)
                content_message = content.get('message')
                if content_message:
                    raise DashboardException(msg=content_message, component='iscsi')
            raise DashboardException(e=e, component='iscsi')

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
                'backstore': disk_config['backstore']
            }
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
        portals_by_host = {}
        for portal in portals:
            host = portal['host']
            ip = portal['ip']
            if host not in portals_by_host:
                portals_by_host[host] = []
            portals_by_host[host].append(ip)
        return portals_by_host
