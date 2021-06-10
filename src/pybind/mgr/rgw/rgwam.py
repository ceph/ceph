#!@Python3_EXECUTABLE@
# -*- mode:python -*-
# vim: ts=4 sw=4 smarttab expandtab
#
# Processed in Makefile to add python #! line and version variable
#
#

import subprocess
import random
import string
import json
import argparse
import sys
import socket
import base64
import logging
import errno

from urllib.parse import urlparse

from .types import RGWAMException, RGWAMCmdRunException, RGWPeriod, RGWUser, RealmToken

from ceph.deployment.service_spec import RGWSpec


DEFAULT_PORT = 8000

log = logging.getLogger(__name__)


def bool_str(x):
    return 'true' if x else 'false'

def rand_alphanum_lower(l):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=l))

def get_endpoints(endpoints, period = None):
    if endpoints:
        return endpoints

    hostname = socket.getfqdn()

    port = DEFAULT_PORT

    while True:
        ep = 'http://%s:%d' % (hostname, port)
        if not period or not period.endpoint_exists(ep):
            return ep
        port += 1


class EnvArgs:
    def __init__(self, mgr, ceph_conf, ceph_name, ceph_keyring):
        self.mgr = mgr
        self.ceph_conf = ceph_conf
        self.ceph_name = ceph_name
        self.ceph_keyring = ceph_keyring

class RGWCmdBase:
    def __init__(self, prog, env):
        self.cmd_prefix = [ prog ]
        if env.ceph_conf:
            self.cmd_prefix += [ '-c', env.ceph_conf ]
        if env.ceph_name:
            self.cmd_prefix += [ '-n', env.ceph_name ]
        if env.ceph_keyring:
            self.cmd_prefix += [ '-k', env.ceph_keyring ]

    def run(self, cmd):
        run_cmd = self.cmd_prefix + cmd
        result = subprocess.run(run_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stdout = result.stdout.decode('utf-8')
        stderr = result.stderr.decode('utf-8')

        log.debug('cmd=%s' % str(cmd))

        log.debug('stdout=%s' % stdout)

        if result.returncode != 0:
            cmd_str = ' '.join(run_cmd)
            log.error('ERROR: command exited with error status (%d): %s\nstdout=%s\nstderr=%s' % (result.returncode, cmd_str, stdout, stderr))
            raise RGWAMCmdRunException(cmd_str, -result.returncode, stdout, stderr)

        return (stdout, stderr)

class RGWAdminCmd(RGWCmdBase):
    def __init__(self, env):
        super().__init__('radosgw-admin', env)

class RGWAdminJSONCmd(RGWAdminCmd):
    def __init__(self, env):
        super().__init__(env)

    def run(self, cmd):
        stdout, _ = RGWAdminCmd.run(self, cmd)

        return json.loads(stdout)


class RGWCmd(RGWCmdBase):
    def __init__(self, env):
        super().__init__('radosgw', env)

class RealmOp(RGWAdminCmd):
    def __init__(self, env):
        super().__init__(env)
        
    def get(self):
        params = [ 'realm',
                   'get' ]

        return RGWAdminJSONCmd.run(self, params)

    def create(self, name = None, is_default = True):
        self.name = name
        if not self.name:
            self.name = 'realm-' + rand_alphanum_lower(8)

        params = [ 'realm',
                   'create',
                   '--rgw-realm', self.name ]

        if is_default:
            params += [ '--default' ]

        return RGWAdminJSONCmd.run(self, params)

    def pull(self, url, access_key, secret, set_default = False):
        params = [ 'realm',
                   'pull',
                   '--url', url,
                   '--access-key', access_key,
                   '--secret', secret ]

        if set_default:
            params += [ '--default' ]

        return RGWAdminJSONCmd.run(self, params)

class ZonegroupOp(RGWAdminCmd):
    def __init__(self, env):
        super().__init__(env)
        
    def create(self, realm, name = None, endpoints = None, is_master = True, is_default = True):
        self.name = name
        if not self.name:
            self.name = 'zg-' + rand_alphanum_lower(8)

        params = [ 'zonegroup',
                   'create',
                   '--rgw-realm', realm,
                   '--rgw-zonegroup', self.name,
                   '--endpoints', endpoints ]

        if is_master:
            params += [ '--master' ]

        if is_default:
            params += [ '--default' ]

        stdout, _ = RGWAdminCmd.run(self, params)

        self.info = json.loads(stdout)

        return self.info

class ZoneOp(RGWAdminCmd):
    def __init__(self, env):
        super().__init__(env)
        
    def get(self):
        params = [ 'zone',
                   'get' ]

        return RGWAdminJSONCmd.run(self, params)

    def create(self, realm, zonegroup, name = None, endpoints = None, is_master = True, is_default = True,
               access_key = None, secret = None):
        self.name = name
        if not self.name:
            self.name = 'z-' + rand_alphanum_lower(8)

        params = [ 'zone',
                   'create',
                   '--rgw-realm', realm,
                   '--rgw-zonegroup', zonegroup,
                   '--rgw-zone', self.name,
                   '--endpoints', endpoints ]

        if is_master:
            params += [ '--master' ]

        if is_default:
            params += [ '--default' ]

        if access_key:
            params += [ '--access-key', access_key ]

        if secret:
            params += [ '--secret', secret ]

        return RGWAdminJSONCmd.run(self, params)

    def modify(self, endpoints = None, is_master = None, is_default = None, access_key = None, secret = None):
        params = [ 'zone',
                   'modify' ]

        if endpoints:
            params += [ '--endpoints', endpoints ]

        if is_master is not None:
            params += [ '--master', bool_str(is_master) ]

        if is_default is not None:
            params += [ '--default', bool_str(is_default) ]

        if access_key:
            params += [ '--access-key', access_key ]

        if secret:
            params += [ '--secret', secret ]

        return RGWAdminJSONCmd.run(self, params)

class PeriodOp(RGWAdminCmd):
    def __init__(self, env):
        super().__init__(env)
        
    def update(self, realm, commit = True):

        params = [ 'period',
                   'update',
                   '--rgw-realm', realm ]

        if commit:
            params += [ '--commit' ]

        return RGWAdminJSONCmd.run(self, params)

    def get(self, realm = None):
        params = [ 'period',
                   'get' ]

        if realm:
            params += [ '--rgw-realm', realm ]

        return RGWAdminJSONCmd.run(self, params)

class UserOp(RGWAdminCmd):
    def __init__(self, env):
        super().__init__(env)
        
    def create(self, uid = None, uid_prefix = None, display_name = None, email = None, is_system = False):
        self.uid = uid
        if not self.uid:
            prefix = uid_prefix or 'user'
            self.uid = prefix + '-' + rand_alphanum_lower(6)

        self.display_name = display_name
        if not self.display_name:
            self.display_name = self.uid

        params = [ 'user',
                   'create',
                   '--uid', self.uid,
                   '--display-name', self.display_name ]

        if email:
            params += [ '--email', email ]

        if is_system:
            params += [ '--system' ]

        return RGWAdminJSONCmd.run(self, params)

class RGWAM:
    def __init__(self, env):
        self.env = env

    def realm_op(self):
        return RealmOp(self.env)

    def period_op(self):
        return PeriodOp(self.env)

    def zonegroup_op(self):
        return ZonegroupOp(self.env)

    def zone_op(self):
        return ZoneOp(self.env)

    def user_op(self):
        return UserOp(self.env)

    def realm_bootstrap(self, realm, zonegroup, zone, endpoints, sys_uid, uid, start_radosgw):
        endpoints = get_endpoints(endpoints)

        try:
            realm_info = self.realm_op().create(realm)
        except RGWAMException as e:
            raise RGWAMException('failed to create realm', e)

        realm_name = realm_info['name']
        realm_id = realm_info['id']
        logging.info('Created realm %s (%s)' % (realm_name, realm_id))

        try:
            zg_info = self.zonegroup_op().create(realm_name, zonegroup, endpoints, True, True)
        except RGWAMException as e:
            raise RGWAMException('failed to create zonegroup', e)

        zg_name = zg_info['name']
        zg_id = zg_info['id']
        logging.info('Created zonegroup %s (%s)' % (zg_name, zg_id))

        try:
            zone_info = self.zone_op().create(realm_name, zg_name, zone, endpoints, True, True)
        except RGWAMException as e:
            raise RGWAMException('failed to create zone', e)

        zone_name = zone_info['name']
        zone_id = zone_info['id']
        logging.info('Created zone %s (%s)' % (zone_name, zone_id))

        try:
            period_info = self.period_op().update(realm_name, True)
        except RGWAMCmdRunException as e:
            raise RGWAMException('failed to update period', e)

        period = RGWPeriod(period_info)

        logging.info('Period: ' + period.id)

        try:
            sys_user_info = self.user_op().create(uid = sys_uid, uid_prefix = 'user-sys', is_system = True)
        except RGWAMException as e:
            raise RGWAMException('failed to create system user', e)

        sys_user = RGWUser(sys_user_info)

        logging.info('Created system user: %s' % sys_user.uid)

        sys_access_key = ''
        sys_secret = ''

        if len(sys_user.keys) > 0:
            sys_access_key = sys_user.keys[0].access_key
            sys_secret = sys_user.keys[0].secret_key

        try:
            zone_info = self.zone_op().modify(endpoints, None, None, sys_access_key, sys_secret)
        except RGWAMException as e:
            raise RGWAMException('failed to modify zone info', e)

        try:
            user_info = self.user_op().create(uid = uid, is_system = False)
        except RGWAMException as e:
            raise RGWAMException('failed to create user', e)

        user = RGWUser(user_info)

        logging.info('Created regular user: %s' % user.uid)

        eps = endpoints.split(',')
        ep = ''
        if len(eps) > 0:
            ep = eps[0]
            if start_radosgw:
                o = urlparse(ep)
                svc_id = realm_name  + '.' + zone_name
                spec = RGWSpec(service_id = svc_id,
                               rgw_realm = realm_name,
                               rgw_zone = zone_name,
                               rgw_frontend_port = o.port)
                self.env.mgr.apply_rgw(spec)

        realm_token = RealmToken(ep, sys_user.uid, sys_access_key, sys_secret)

        logging.info(realm_token.to_json())

        realm_token_b = realm_token.to_json().encode('utf-8')
        return (0, 'Realm Token: %s' % base64.b64encode(realm_token_b).decode('utf-8'), '')

    def realm_new_zone_creds(self, endpoints, sys_uid):
        try:
            period_info = self.period_op().get()
        except RGWAMException as e:
            raise RGWAMException('failed to fetch period info', e)

        period = RGWPeriod(period_info)

        try:
            zone_info = self.zone_op().get()
        except RGWAMException as e:
            raise RGWAMException('failed to create zone', e)

        zone_name = zone_info['name']
        zone_id = zone_info['id']

        logging.info('Period: ' + period.id)
        logging.info('Master zone: ' + period.master_zone)
        logging.info('Current zone: ' + zone_id)

        if period.master_zone != zone_id:
            return (-errno.EINVAL, '', 'Command needs to run on master zone')

        ep = ''
        if not endpoints:
            eps = period.get_zone_endpoints(period.master_zonegroup, period.master_zone)
        else:
            eps = endpoints.split(',')

        if len(eps) > 0:
            ep = eps[0]

        try:
            sys_user_info = self.user_op().create(uid = sys_uid, uid_prefix = 'user-sys', is_system = True)
        except RGWAMException as e:
            raise RGWAMException('failed to create system user', e)

        sys_user = RGWUser(sys_user_info)

        logging.info('Created system user: %s' % sys_user.uid)

        sys_access_key = ''
        sys_secret = ''

        if len(sys_user.keys) > 0:
            sys_access_key = sys_user.keys[0].access_key
            sys_secret = sys_user.keys[0].secret_key

        realm_token = RealmToken(ep, sys_user.uid, sys_access_key, sys_secret)

        logging.info(realm_token.to_json())

        realm_token_b = realm_token.to_json().encode('utf-8')
        return (0, 'Realm Token: %s' % base64.b64encode(realm_token_b).decode('utf-8'), '')

    def zone_create(self, realm_token_b64, zonegroup = None, zone = None, endpoints = None, start_radosgw = True):
        if not realm_token_b64:
            print('missing realm access config')
            return False

        realm_token_b = base64.b64decode(realm_token_b64)
        realm_token_s = realm_token_b.decode('utf-8')

        realm_token = json.loads(realm_token_s)

        access_key = realm_token['access_key']
        secret = realm_token['secret']

        try:
            realm_info = self.realm_op().pull(realm_token['endpoint'], access_key, secret, set_default = True)
        except RGWAMException as e:
            raise RGWAMException('failed to pull realm', e)

        realm_name = realm_info['name']
        realm_id = realm_info['id']
        logging.info('Pulled realm %s (%s)' % (realm_name, realm_id))

        period_info = self.period_op().get()

        period = RGWPeriod(period_info)

        logging.info('Period: ' + period.id)
        endpoints = get_endpoints(endpoints, period)

        zg = period.find_zonegroup_by_name(zonegroup)
        if not zg:
            raise RGWAMException('zonegroup %s not found' % (zonegroup or '<none>'))

        try:
            zone_info = self.zone_op().create(realm_name, zg.name, zone, endpoints, False, True,
                access_key, secret)
        except RGWAMException as e:
            raise RGWAMException('failed to create zone', e)

        zone_name = zone_info['name']
        zone_id = zone_info['id']

        success_message = 'Created zone %s (%s)' % (zone_name, zone_id)
        logging.info(success_message)

        try:
            period_info = self.period_op().update(realm_name, True)
        except RGWAMException as e:
            raise RGWAMException('failed to update period', e)

        period = RGWPeriod(period_info)

        logging.debug(period.to_json())

        svc_id = realm_name  + '.' + zone_name

        #if endpoints:
        #    eps = endpoints.split(',')
        #    ep = ''
        #    if len(eps) > 0:
        #        ep = eps[0]
        #        o = urlparse(ep)
        #        port = o.port
        #        spec = RGWSpec(service_id = svc_id,
        #                       rgw_realm = realm_name,
        #                       rgw_zone = zone_name,
        #                       rgw_frontend_port = o.port)
        #        self.env.mgr.apply_rgw(spec)

        spec = RGWSpec(service_id = svc_id,
                       rgw_realm = realm_name,
                       rgw_zone = zone_name)

        completion = self.env.mgr.apply_rgw(spec)
        orchestrator.raise_if_exception(completion)

        completion = self.env.mgr.list_daemons(svc_id, 'rgw', refresh=True)

        daemons = orchestrator.raise_if_exception(completion)

        ep = []
        for s in daemons:
            for p in s.ports:
                ep.append('http://%s:%d' % (s.hostname, p))

        log.error('ERROR: ep=%s' % ','.join(ep))

        try:
            zone_info = self.zone_op().modify(zone_name, endpoints = ','.join(ep))
        except RGWAMException as e:
            raise RGWAMException('failed to modify zone', e)

        return (0, success_message, '')

    def run_radosgw(self, port = None, log_file = None, debug_ms = None, debug_rgw = None):

        fe_cfg = 'beast'
        if port:
            fe_cfg += ' port=%s' % port


        params = [ '--rgw-frontends', fe_cfg ]

        if log_file:
            params += [ '--log-file', log_file ]

        if debug_ms:
            params += [ '--debug-ms', debug_ms ]

        if debug_rgw:
            params += [ '--debug-rgw', debug_rgw ]

        (retcode, stdout, stderr) = RGWCmd(self.env).run(params)

        return (retcode, stdout, stderr)

