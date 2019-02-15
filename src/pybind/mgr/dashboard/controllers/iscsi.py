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
from ..exceptions import DashboardException
from ..tools import TaskManager


@UiApiController('/iscsi', Scope.ISCSI)
class IscsiUi(BaseController):

    @Endpoint()
    @ReadPermission
    def status(self):
        status = {'available': False}
        if not IscsiGatewaysConfig.get_gateways_config()['gateways']:
            status['message'] = 'There are no gateways defined'
            return status
        try:
            IscsiClient.instance().get_config()
            status['available'] = True
        except RequestException as e:
            if e.content:
                content = json.loads(e.content)
                content_message = content.get('message')
                if content_message:
                    status['message'] = content_message
        return status

    @Endpoint()
    @ReadPermission
    def settings(self):
        return IscsiClient.instance().get_settings()

    @Endpoint()
    @ReadPermission
    def portals(self):
        portals = []
        gateways_config = IscsiGatewaysConfig.get_gateways_config()
        for name in gateways_config['gateways'].keys():
            ip_addresses = IscsiClient.instance(gateway_name=name).get_ip_addresses()
            portals.append({'name': name, 'ip_addresses': ip_addresses['data']})
        return sorted(portals, key=lambda p: '{}.{}'.format(p['name'], p['ip_addresses']))


@ApiController('/iscsi', Scope.ISCSI)
class Iscsi(BaseController):

    @Endpoint('GET', 'discoveryauth')
    @UpdatePermission
    def get_discoveryauth(self):
        return self._get_discoveryauth()

    @Endpoint('PUT', 'discoveryauth')
    @UpdatePermission
    def set_discoveryauth(self, user, password, mutual_user, mutual_password):
        IscsiClient.instance().update_discoveryauth(user, password, mutual_user, mutual_password)
        return self._get_discoveryauth()

    def _get_discoveryauth(self):
        config = IscsiClient.instance().get_config()
        user = ''
        password = ''
        chap = config['discovery_auth']['chap']
        if chap:
            user, password = chap.split('/')
        mutual_user = ''
        mutual_password = ''
        chap_mutual = config['discovery_auth']['chap_mutual']
        if chap_mutual:
            mutual_user, mutual_password = chap_mutual.split('/')
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
            targets.append(target)
        return targets

    def get(self, target_iqn):
        config = IscsiClient.instance().get_config()
        if target_iqn not in config['targets']:
            raise cherrypy.HTTPError(404)
        return IscsiTarget._config_to_target(target_iqn, config)

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
        IscsiTarget._delete(target_iqn, config, 0, 100)

    @iscsi_target_task('create', {'target_iqn': '{target_iqn}'})
    def create(self, target_iqn=None, target_controls=None,
               portals=None, disks=None, clients=None, groups=None):
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
        IscsiTarget._validate(target_iqn, portals, disks)
        IscsiTarget._create(target_iqn, target_controls, portals, disks, clients, groups, 0, 100,
                            config)

    @iscsi_target_task('edit', {'target_iqn': '{target_iqn}'})
    def set(self, target_iqn, new_target_iqn=None, target_controls=None,
            portals=None, disks=None, clients=None, groups=None):
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
        IscsiTarget._validate(new_target_iqn, portals, disks)
        config = IscsiTarget._delete(target_iqn, config, 0, 50, new_target_iqn, target_controls,
                                     portals, disks, clients, groups)
        IscsiTarget._create(new_target_iqn, target_controls, portals, disks, clients, groups,
                            50, 100, config)

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
                                                    new_portals, new_groups, group_id, new_clients,
                                                    new_disks):
                deleted_groups.append(group_id)
                IscsiClient.instance(gateway_name=gateway_name).delete_group(target_iqn,
                                                                             group_id)
            TaskManager.current_task().inc_progress(task_progress_inc)
        for client_iqn in list(target_config['clients'].keys()):
            if IscsiTarget._client_deletion_required(target, new_target_iqn, new_target_controls,
                                                     new_portals, new_clients, client_iqn,
                                                     new_groups, deleted_groups):
                IscsiClient.instance(gateway_name=gateway_name).delete_client(target_iqn,
                                                                              client_iqn)
            TaskManager.current_task().inc_progress(task_progress_inc)
        for image_id in target_config['disks']:
            if IscsiTarget._target_lun_deletion_required(target, new_target_iqn,
                                                         new_target_controls, new_portals,
                                                         new_disks, image_id):
                IscsiClient.instance(gateway_name=gateway_name).delete_target_lun(target_iqn,
                                                                                  image_id)
                IscsiClient.instance(gateway_name=gateway_name).delete_disk(image_id)
            TaskManager.current_task().inc_progress(task_progress_inc)
        if IscsiTarget._target_deletion_required(target, new_target_iqn, new_target_controls,
                                                 new_portals):
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
    def _group_deletion_required(target, new_target_iqn, new_target_controls, new_portals,
                                 new_groups, group_id, new_clients, new_disks):
        if IscsiTarget._target_deletion_required(target, new_target_iqn, new_target_controls,
                                                 new_portals):
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
                                                     new_portals, new_clients, client_iqn,
                                                     new_groups, []):
                return True
        # Check if any disk inside this group has changed
        for disk in new_group['disks']:
            image_id = '{}.{}'.format(disk['pool'], disk['image'])
            if IscsiTarget._target_lun_deletion_required(target, new_target_iqn,
                                                         new_target_controls, new_portals,
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
    def _client_deletion_required(target, new_target_iqn, new_target_controls, new_portals,
                                  new_clients, client_iqn, new_groups, deleted_groups):
        if IscsiTarget._target_deletion_required(target, new_target_iqn, new_target_controls,
                                                 new_portals):
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
            if '{}.{}'.format(disk['pool'], disk['image']) == image_id:
                return disk
        return None

    @staticmethod
    def _target_lun_deletion_required(target, new_target_iqn, new_target_controls, new_portals,
                                      new_disks, image_id):
        if IscsiTarget._target_deletion_required(target, new_target_iqn, new_target_controls,
                                                 new_portals):
            return True
        new_disk = IscsiTarget._get_disk(new_disks, image_id)
        if not new_disk:
            return True
        old_disk = IscsiTarget._get_disk(target['disks'], image_id)
        if new_disk != old_disk:
            return True
        return False

    @staticmethod
    def _target_deletion_required(target, new_target_iqn, new_target_controls, new_portals):
        if target['target_iqn'] != new_target_iqn:
            return True
        if target['target_controls'] != new_target_controls:
            return True
        if target['portals'] != new_portals:
            return True
        return False

    @staticmethod
    def _validate(target_iqn, portals, disks):
        if not target_iqn:
            raise DashboardException(msg='Target IQN is required',
                                     code='target_iqn_required',
                                     component='iscsi')

        settings = IscsiClient.instance().get_settings()
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
            IscsiTarget._validate_image_exists(pool, image)

    @staticmethod
    def _validate_image_exists(pool, image):
        try:
            ioctx = mgr.rados.open_ioctx(pool)
            try:
                rbd.Image(ioctx, image)
            except rbd.ImageNotFound:
                raise DashboardException(msg='Image {} does not exist'.format(image),
                                         code='image_does_not_exist',
                                         component='iscsi')
        except rados.ObjectNotFound:
            raise DashboardException(msg='Pool {} does not exist'.format(pool),
                                     code='pool_does_not_exist',
                                     component='iscsi')

    @staticmethod
    def _create(target_iqn, target_controls,
                portals, disks, clients, groups,
                task_progress_begin, task_progress_end, config):
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
                    IscsiClient.instance(gateway_name=gateway_name).create_gateway(target_iqn,
                                                                                   host,
                                                                                   ip_list)
                    TaskManager.current_task().inc_progress(task_progress_inc)
            for disk in disks:
                pool = disk['pool']
                image = disk['image']
                image_id = '{}.{}'.format(pool, image)
                if image_id not in config['disks']:
                    IscsiClient.instance(gateway_name=gateway_name).create_disk(image_id)
                if not target_config or image_id not in target_config['disks']:
                    IscsiClient.instance(gateway_name=gateway_name).create_target_lun(target_iqn,
                                                                                      image_id)
                    controls = disk['controls']
                    if controls:
                        IscsiClient.instance(gateway_name=gateway_name).reconfigure_disk(image_id,
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
                        image_id = '{}.{}'.format(pool, image)
                        IscsiClient.instance(gateway_name=gateway_name).create_client_lun(
                            target_iqn, client_iqn, image_id)
                    user = client['auth']['user']
                    password = client['auth']['password']
                    chap = '{}/{}'.format(user, password) if user and password else ''
                    m_user = client['auth']['mutual_user']
                    m_password = client['auth']['mutual_password']
                    m_chap = '{}/{}'.format(m_user, m_password) if m_user and m_password else ''
                    IscsiClient.instance(gateway_name=gateway_name).create_client_auth(
                        target_iqn, client_iqn, chap, m_chap)
                TaskManager.current_task().inc_progress(task_progress_inc)
            for group in groups:
                group_id = group['group_id']
                members = group['members']
                image_ids = []
                for disk in group['disks']:
                    image_ids.append('{}.{}'.format(disk['pool'], disk['image']))
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
        for host in target_config['portals'].keys():
            ips = IscsiClient.instance(gateway_name=host).get_ip_addresses()['data']
            portal_ips = [ip for ip in ips if ip in target_config['ip_list']]
            for portal_ip in portal_ips:
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
            }
            disks.append(disk)
        disks = IscsiTarget._sorted_disks(disks)
        clients = []
        for client_iqn, client_config in target_config['clients'].items():
            luns = []
            for client_lun in client_config['luns'].keys():
                pool, image = client_lun.split('.', 1)
                lun = {
                    'pool': pool,
                    'image': image
                }
                luns.append(lun)
            user = None
            password = None
            if '/' in client_config['auth']['chap']:
                user, password = client_config['auth']['chap'].split('/', 1)
            mutual_user = None
            mutual_password = None
            if '/' in client_config['auth']['chap_mutual']:
                mutual_user, mutual_password = client_config['auth']['chap_mutual'].split('/', 1)
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
                pool, image = group_disk_key.split('.', 1)
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
        for key, value in target_controls.items():
            if isinstance(value, bool):
                target_controls[key] = 'Yes' if value else 'No'
        target = {
            'target_iqn': target_iqn,
            'portals': portals,
            'disks': disks,
            'clients': clients,
            'groups': groups,
            'target_controls': target_controls,
        }
        return target

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
