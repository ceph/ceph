# -*- mode:python -*-
# vim: ts=4 sw=4 smarttab expandtab
#
# Processed in Makefile to add python #! line and version variable
#
#

import random
import string
import json
import socket
import base64
import logging
import errno

from .types import RGWAMException, RGWAMCmdRunException, RGWPeriod, RGWUser, RealmToken
from .diff import RealmsEPs

DEFAULT_PORT = 8000

log = logging.getLogger(__name__)


def bool_str(x):
    return 'true' if x else 'false'


def rand_alphanum_lower(k):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=k))


def gen_name(prefix, suffix_len):
    return prefix + rand_alphanum_lower(suffix_len)


def set_or_gen(val, gen, prefix):
    if val:
        return val
    if gen:
        return gen_name(prefix, 8)

    return None


def get_endpoints(endpoints, period=None):
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
    def __init__(self, mgr):
        self.mgr = mgr


class EntityKey:
    def __init__(self, name=None, id=None):
        self.name = name
        self.id = id

    def safe_vals(ek):
        if not ek:
            return None, None
        return ek.name, ek.id


class EntityName(EntityKey):
    def __init__(self, name=None):
        super().__init__(name=name)


class EntityID(EntityKey):
    def __init__(self, id=None):
        super().__init__(id=id)


class ZoneEnv:
    def __init__(self, env: EnvArgs, realm: EntityKey = None, zg: EntityKey = None,
                 zone: EntityKey = None):
        self.env = env
        self.realm = realm
        self.zg = zg
        self.zone = zone

    def set(self, env: EnvArgs = None, realm: EntityKey = None, zg: EntityKey = None,
            zone: EntityKey = None):
        if env:
            self.env = env
        if realm:
            self.realm = realm
        if zg:
            self.zg = zg
        if zone:
            self.zone = zone

        return self

    def _init_entity(self, ek: EntityKey, gen, prefix):
        name, id = EntityKey.safe_vals(ek)
        name = set_or_gen(name, gen, prefix)

        return EntityKey(name, id)

    def init_realm(self, realm: EntityKey = None, gen=False):
        self.realm = self._init_entity(realm, gen, 'realm-')
        return self

    def init_zg(self, zg: EntityKey = None, gen=False):
        self.zg = self._init_entity(zg, gen, 'zg-')
        return self

    def init_zone(self, zone: EntityKey = None, gen=False):
        self.zone = self._init_entity(zone, gen, 'zone-')
        return self


def opt_arg(params, cmd, arg):
    if arg:
        params += [cmd, arg]


def opt_arg_bool(params, flag, arg):
    if arg:
        params += [flag]


class RGWCmdBase:
    def __init__(self, prog, zone_env: ZoneEnv):
        self.env = zone_env.env
        self.mgr = self.env.mgr
        self.prog = prog
        self.cmd_suffix = []
        if zone_env.realm:
            opt_arg(self.cmd_suffix, '--rgw-realm', zone_env.realm.name)
            opt_arg(self.cmd_suffix, '--realm-id', zone_env.realm.id)
        if zone_env.zg:
            opt_arg(self.cmd_suffix, '--rgw-zonegroup', zone_env.zg.name)
            opt_arg(self.cmd_suffix, '--zonegroup-id', zone_env.zg.id)
        if zone_env.zone:
            opt_arg(self.cmd_suffix, '--rgw-zone', zone_env.zone.name)
            opt_arg(self.cmd_suffix, '--zone-id', zone_env.zone.id)

    def run(self, cmd, stdin=None):
        args = cmd + self.cmd_suffix
        cmd, returncode, stdout, stderr = self.mgr.tool_exec(self.prog, args, stdin)

        log.debug('cmd=%s' % str(cmd))
        log.debug(f'stdin={stdin}')
        log.debug('stdout=%s' % stdout)

        if returncode != 0:
            cmd_str = ' '.join(cmd)
            log.error('ERROR: command exited with error status (%d): %s\nstdout=%s\nstderr=%s' %
                      (returncode, cmd_str, stdout, stderr))
            raise RGWAMCmdRunException(cmd_str, -returncode, stdout, stderr)

        return (stdout, stderr)


class RGWAdminCmd(RGWCmdBase):
    def __init__(self, zone_env: ZoneEnv):
        super().__init__('radosgw-admin', zone_env)


class RGWAdminJSONCmd(RGWAdminCmd):
    def __init__(self, zone_env: ZoneEnv):
        super().__init__(zone_env)

    def run(self, cmd, stdin=None):
        stdout, _ = RGWAdminCmd.run(self, cmd, stdin)

        return json.loads(stdout)


class RGWCmd(RGWCmdBase):
    def __init__(self, zone_env: ZoneEnv):
        super().__init__('radosgw', zone_env)


class RealmOp:
    def __init__(self, env: EnvArgs):
        self.env = env

    def list(self):
        try:
            ze = ZoneEnv(self.env)
            params = ['realm', 'list']
            output = RGWAdminJSONCmd(ze).run(params)
            return output.get('realms') or []
        except RGWAMException as e:
            logging.info(f'Exception while listing realms {e.message}')
            # in case the realm list is empty an exception is raised
            return []

    def get(self, realm: EntityKey = None):
        ze = ZoneEnv(self.env, realm=realm)
        params = ['realm', 'get']
        return RGWAdminJSONCmd(ze).run(params)

    def create(self, realm: EntityKey = None):
        ze = ZoneEnv(self.env).init_realm(realm=realm, gen=True)
        params = ['realm', 'create']
        return RGWAdminJSONCmd(ze).run(params)

    def pull(self, realm, url, access_key, secret):
        params = ['realm',
                  'pull',
                  '--url', url,
                  '--access-key', access_key,
                  '--secret', secret]
        ze = ZoneEnv(self.env, realm=realm)
        return RGWAdminJSONCmd(ze).run(params)


class ZonegroupOp:
    def __init__(self, env: EnvArgs):
        self.env = env

    def list(self):
        try:
            ze = ZoneEnv(self.env)
            params = ['zonegroup', 'list']
            output = RGWAdminJSONCmd(ze).run(params)
            return output.get('zonegroups') or []
        except RGWAMException as e:
            logging.info(f'Exception while listing zonegroups {e.message}')
            return []

    def get(self, zonegroup: EntityKey = None):
        ze = ZoneEnv(self.env)
        params = ['zonegroup', 'get']
        return RGWAdminJSONCmd(ze).run(params)

    def set(self, zonegroup: EntityKey, zg_json: str):
        ze = ZoneEnv(self.env)
        params = ['zonegroup', 'set']
        return RGWAdminJSONCmd(ze).run(params, stdin=zg_json.encode('utf-8'))

    def create(self, realm: EntityKey, zg: EntityKey = None, endpoints=None, is_master=True):
        ze = ZoneEnv(self.env, realm=realm).init_zg(zg, gen=True)

        params = ['zonegroup',
                  'create']

        opt_arg_bool(params, '--master', is_master)
        opt_arg(params, '--endpoints', endpoints)

        stdout, _ = RGWAdminCmd(ze).run(params)

        return json.loads(stdout)

    def modify(self, realm: EntityKey, zg: EntityKey, endpoints=None):
        ze = ZoneEnv(self.env, realm=realm, zg=zg)
        params = ['zonegroup', 'modify']
        opt_arg(params, '--endpoints', endpoints)
        return RGWAdminJSONCmd(ze).run(params)


class ZoneOp:
    def __init__(self, env: EnvArgs):
        self.env = env

    def list(self):
        try:
            ze = ZoneEnv(self.env)
            params = ['zone', 'list']
            output = RGWAdminJSONCmd(ze).run(params)
            return output.get('zones') or []
        except RGWAMException as e:
            logging.info(f'Exception while listing zones {e.message}')
            return []

    def get(self, zone: EntityKey):
        ze = ZoneEnv(self.env, zone=zone)

        params = ['zone',
                  'get']

        return RGWAdminJSONCmd(ze).run(params)

    def create(self, realm: EntityKey, zonegroup: EntityKey, zone: EntityKey = None,
               endpoints=None, is_master=True,
               access_key=None, secret=None):

        ze = ZoneEnv(self.env, realm=realm, zg=zonegroup).init_zone(zone, gen=True)

        params = ['zone',
                  'create']

        opt_arg_bool(params, '--master', is_master)
        opt_arg(params, '--access-key', access_key)
        opt_arg(params, '--secret', secret)
        opt_arg(params, '--endpoints', endpoints)

        return RGWAdminJSONCmd(ze).run(params)

    def modify(self, zone: EntityKey, zg: EntityKey, is_master=None,
               access_key=None, secret=None, endpoints=None):
        ze = ZoneEnv(self.env, zone=zone, zg=zg)

        params = ['zone',
                  'modify']

        opt_arg_bool(params, '--master', is_master)
        opt_arg(params, '--access-key', access_key)
        opt_arg(params, '--secret', secret)
        opt_arg(params, '--endpoints', endpoints)

        return RGWAdminJSONCmd(ze).run(params)


class PeriodOp:
    def __init__(self, env):
        self.env = env

    def update(self, realm: EntityKey, zonegroup: EntityKey, zone: EntityKey, commit=True):
        master_zone_info = self.get_master_zone(realm, zonegroup)
        master_zone = EntityName(master_zone_info['name']) if master_zone_info else zone
        master_zonegroup_info = self.get_master_zonegroup(realm)
        master_zonegroup = EntityName(master_zonegroup_info['name']) \
            if master_zonegroup_info else zonegroup
        ze = ZoneEnv(self.env, realm=realm,  zg=master_zonegroup, zone=master_zone)
        params = ['period', 'update']
        opt_arg_bool(params, '--commit', commit)
        return RGWAdminJSONCmd(ze).run(params)

    def get_master_zone(self, realm, zonegroup=None):
        try:
            ze = ZoneEnv(self.env, realm=realm, zg=zonegroup)
            params = ['zone', 'get']
            return RGWAdminJSONCmd(ze).run(params)
        except RGWAMCmdRunException:
            return None

    def get_master_zone_ep(self, realm, zonegroup=None):
        try:
            ze = ZoneEnv(self.env, realm=realm, zg=zonegroup)
            params = ['period', 'get']
            output = RGWAdminJSONCmd(ze).run(params)
            for zg in output['period_map']['zonegroups']:
                if not bool(zg['is_master']):
                    continue
                for zone in zg['zones']:
                    if zone['id'] == zg['master_zone']:
                        return zone['endpoints']
            return None
        except RGWAMCmdRunException:
            return None

    def get_master_zonegroup(self, realm):
        try:
            ze = ZoneEnv(self.env, realm=realm)
            params = ['zonegroup', 'get']
            return RGWAdminJSONCmd(ze).run(params)
        except RGWAMCmdRunException:
            return None

    def get(self, realm=None):
        ze = ZoneEnv(self.env, realm=realm)
        params = ['period', 'get']
        return RGWAdminJSONCmd(ze).run(params)


class UserOp:
    def __init__(self, env):
        self.env = env

    def create(self, zone: EntityKey, zg: EntityKey, uid=None, uid_prefix=None, display_name=None,
               email=None, is_system=False):
        ze = ZoneEnv(self.env, zone=zone, zg=zg)

        u = uid or gen_name(uid_prefix or 'user-', 6)

        dn = display_name or u

        params = ['user',
                  'create',
                  '--uid', u,
                  '--display-name', dn]

        opt_arg(params, '--email', email)
        opt_arg_bool(params, '--system', is_system)

        return RGWAdminJSONCmd(ze).run(params)

    def info(self, zone: EntityKey, zg: EntityKey, uid=None, access_key=None):
        ze = ZoneEnv(self.env, zone=zone, zg=zg)

        params = ['user',
                  'info']

        opt_arg(params, '--uid', uid)
        opt_arg(params, '--access-key', access_key)

        return RGWAdminJSONCmd(ze).run(params)

    def rm(self, zone: EntityKey, zg: EntityKey, uid=None, access_key=None):
        ze = ZoneEnv(self.env, zone=zone, zg=zg)

        params = ['user',
                  'rm']

        opt_arg(params, '--uid', uid)
        opt_arg(params, '--access-key', access_key)

        return RGWAdminCmd(ze).run(params)

    def rm_key(self, zone: EntityKey, zg: EntityKey, access_key=None):
        ze = ZoneEnv(self.env, zone=zone, zg=zg)

        params = ['key',
                  'remove']

        opt_arg(params, '--access-key', access_key)

        return RGWAdminCmd(ze).run(params)


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

    def get_realm(self, realm_name):
        try:
            realm_info = self.realm_op().get(EntityName(realm_name))
            realm = EntityKey(realm_info['name'], realm_info['id'])
            return realm
        except RGWAMException:
            raise None

    def create_realm(self, realm_name):
        try:
            realm_info = self.realm_op().create(EntityName(realm_name))
            realm = EntityKey(realm_info['name'], realm_info['id'])
            logging.info(f'Created realm name={realm.name} id={realm.id}')
            return realm
        except RGWAMException as e:
            raise RGWAMException('failed to create realm', e)

    def create_zonegroup(self, realm, zonegroup_name, zonegroup_is_master, endpoints=None):
        try:
            zg_info = self.zonegroup_op().create(realm,
                                                 EntityName(zonegroup_name),
                                                 endpoints,
                                                 is_master=zonegroup_is_master)
            zonegroup = EntityKey(zg_info['name'], zg_info['id'])
            logging.info(f'Created zonegroup name={zonegroup.name} id={zonegroup.id}')
            return zonegroup
        except RGWAMException as e:
            raise RGWAMException('failed to create zonegroup', e)

    def create_zone(self, realm, zg, zone_name, zone_is_master, access_key=None,
                    secret=None, endpoints=None):
        try:
            zone_info = self.zone_op().create(realm, zg,
                                              EntityName(zone_name),
                                              endpoints,
                                              is_master=zone_is_master,
                                              access_key=access_key,
                                              secret=secret)

            zone = EntityKey(zone_info['name'], zone_info['id'])
            logging.info(f'Created zone name={zone.name} id={zone.id}')
            return zone
        except RGWAMException as e:
            raise RGWAMException('failed to create zone', e)

    def create_system_user(self, realm, zonegroup, zone):
        try:
            sys_user_info = self.user_op().create(zone,
                                                  zonegroup,
                                                  uid=f'sysuser-{realm.name}',
                                                  uid_prefix='user-sys',
                                                  is_system=True)
            sys_user = RGWUser(sys_user_info)
            logging.info(f'Created system user: {sys_user.uid} on'
                         '{realm.name}/{zonegroup.name}/{zone.name}')
            return sys_user
        except RGWAMException as e:
            raise RGWAMException('failed to create system user', e)

    def create_normal_user(self, zg, zone, uid=None):
        try:
            user_info = self.user_op().create(zone, zg, uid=uid, is_system=False)
            user = RGWUser(user_info)
            logging.info('Created regular user {user.uid} on'
                         '{realm.name}/{zonegroup.name}/{zone.name}')
            return user
        except RGWAMException as e:
            raise RGWAMException('failed to create user', e)

    def update_period(self, realm, zg, zone=None):
        try:
            period_info = self.period_op().update(realm, zg, zone, commit=True)
            period = RGWPeriod(period_info)
            logging.info('Period: ' + period.id)
        except RGWAMCmdRunException as e:
            raise RGWAMException('failed to update period', e)

    def realm_bootstrap(self, rgw_spec, start_radosgw=True):

        realm_name = rgw_spec.rgw_realm
        zonegroup_name = rgw_spec.rgw_zonegroup
        zone_name = rgw_spec.rgw_zone

        # Some sanity checks
        if realm_name in self.realm_op().list():
            raise RGWAMException(f'Realm {realm_name} already exists')
        if zonegroup_name in self.zonegroup_op().list():
            raise RGWAMException(f'Zonegroup {zonegroup_name} already exists')
        if zone_name in self.zone_op().list():
            raise RGWAMException(f'Zone {zone_name} already exists')

        # Create RGW entities and update the period
        realm = self.create_realm(realm_name)
        zonegroup = self.create_zonegroup(realm, zonegroup_name, zonegroup_is_master=True)
        zone = self.create_zone(realm, zonegroup, zone_name, zone_is_master=True)
        self.update_period(realm, zonegroup)

        # Create system user, normal user and update the master zone
        sys_user = self.create_system_user(realm, zonegroup, zone)
        rgw_acces_key = sys_user.get_key(0)
        access_key = rgw_acces_key.access_key if rgw_acces_key else ''
        secret = rgw_acces_key.secret_key if rgw_acces_key else ''
        self.zone_op().modify(zone, zonegroup, None,
                              access_key, secret, endpoints=rgw_spec.zone_endpoints)
        self.update_period(realm, zonegroup)

        if start_radosgw and rgw_spec.zone_endpoints is None:
            # Instruct the orchestrator to start RGW daemons, asynchronically, this will
            # call back the rgw module to update the master zone with the corresponding endpoints
            realm_token = RealmToken(realm_name,
                                     realm.id,
                                     None,   # no endpoint
                                     access_key, secret)
            realm_token_b = realm_token.to_json().encode('utf-8')
            realm_token_s = base64.b64encode(realm_token_b).decode('utf-8')
            rgw_spec.rgw_realm_token = realm_token_s
            rgw_spec.update_endpoints = True
            self.env.mgr.apply_rgw(rgw_spec)

    def realm_new_zone_creds(self, realm_name, endpoints, sys_uid):
        try:
            period_info = self.period_op().get(EntityName(realm_name))
        except RGWAMException as e:
            raise RGWAMException('failed to fetch period info', e)

        period = RGWPeriod(period_info)

        master_zg = EntityID(period.master_zonegroup)
        master_zone = EntityID(period.master_zone)

        try:
            zone_info = self.zone_op().get(zone=master_zone)
        except RGWAMException as e:
            raise RGWAMException('failed to access master zone', e)

        zone_id = zone_info['id']

        logging.info('Period: ' + period.id)
        logging.info('Master zone: ' + period.master_zone)

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
            sys_user_info = self.user_op().create(master_zone, master_zg, uid=sys_uid,
                                                  uid_prefix='user-sys', is_system=True)
        except RGWAMException as e:
            raise RGWAMException('failed to create system user', e)

        sys_user = RGWUser(sys_user_info)

        logging.info('Created system user: %s' % sys_user.uid)

        sys_access_key = ''
        sys_secret = ''

        if len(sys_user.keys) > 0:
            sys_access_key = sys_user.keys[0].access_key
            sys_secret = sys_user.keys[0].secret_key

        realm_token = RealmToken(realm_name, period.realm_id, ep, sys_access_key, sys_secret)

        logging.info(realm_token.to_json())

        realm_token_b = realm_token.to_json().encode('utf-8')
        return (0, 'Realm Token: %s' % base64.b64encode(realm_token_b).decode('utf-8'), '')

    def realm_rm_zone_creds(self, realm_token_b64):
        if not realm_token_b64:
            raise RGWAMException('missing realm token')

        realm_token = RealmToken.from_base64_str(realm_token_b64)
        try:
            period_info = self.period_op().get(EntityID(realm_token.realm_id))
        except RGWAMException as e:
            raise RGWAMException('failed to fetch period info', e)

        period = RGWPeriod(period_info)
        master_zg = EntityID(period.master_zonegroup)
        master_zone = EntityID(period.master_zone)
        logging.info('Period: ' + period.id)
        logging.info('Master zone: ' + period.master_zone)
        try:
            zone_info = self.zone_op().get(zone=master_zone)
        except RGWAMException as e:
            raise RGWAMException('failed to access master zone', e)

        if period.master_zone != zone_info['id']:
            return (-errno.EINVAL, '', 'Command needs to run on master zone')

        access_key = realm_token.access_key
        try:
            user_info = self.user_op().info(master_zone, master_zg, access_key=access_key)
        except RGWAMException as e:
            raise RGWAMException('failed to get the system user information', e)

        user = RGWUser(user_info)

        only_key = True

        for k in user.keys:
            if k.access_key != access_key:
                only_key = False
                break

        success_message = ''

        if only_key:
            # the only key this user has is the one defined in the token
            # can remove the user completely

            try:
                self.user_op().rm(master_zone, master_zg, uid=user.uid)
            except RGWAMException as e:
                raise RGWAMException('failed removing user ' + user, user.uid, e)

            success_message = 'Removed uid ' + user.uid
        else:
            try:
                self.user_op().rm_key(master_zone, master_zg, access_key=access_key)
            except RGWAMException as e:
                raise RGWAMException('failed removing access key ' +
                                     access_key + '(uid = ' + user.uid + ')', e)

            success_message = 'Removed access key ' + access_key + '(uid = ' + user.uid + ')'

        return (0, success_message, '')

    def zone_modify(self, realm_name, zonegroup_name, zone_name, endpoints, realm_token_b64):

        if not realm_token_b64:
            raise RGWAMException('missing realm access config')
        if zone_name is None:
            raise RGWAMException('Zone name is a mandatory parameter')

        realm_token = RealmToken.from_base64_str(realm_token_b64)
        access_key = realm_token.access_key
        secret = realm_token.secret
        realm_name = realm_token.realm_name
        realm_id = realm_token.realm_id
        logging.info(f'Using realm {realm_name} {realm_id}')

        realm = EntityID(realm_id)
        period_info = self.period_op().get(realm)
        period = RGWPeriod(period_info)
        logging.info('Period: ' + period.id)
        zonegroup = period.find_zonegroup_by_name(zonegroup_name)
        if not zonegroup:
            raise RGWAMException(f'zonegroup {zonegroup_name} not found')

        zg = EntityName(zonegroup.name)
        zone = EntityName(zone_name)
        master_zone_info = self.period_op().get_master_zone(realm, zg)
        success_message = f'Modified zone {realm_name} {zonegroup_name} {zone_name}'
        logging.info(success_message)
        try:
            self.zone_op().modify(zone, zg, access_key=access_key,
                                  secret=secret, endpoints=','.join(endpoints))
            # we only update the zonegroup endpoints if the zone being
            # modified is a master zone
            if zone_name == master_zone_info['name']:
                self.zonegroup_op().modify(realm, zg, endpoints=','.join(endpoints))
        except RGWAMException as e:
            raise RGWAMException('failed to modify zone', e)

        # done, let's update the period
        try:
            period_info = self.period_op().update(realm, zg, zone, True)
        except RGWAMException as e:
            raise RGWAMException('failed to update period', e)

        period = RGWPeriod(period_info)
        logging.debug(period.to_json())

        return (0, success_message, '')

    def zonegroup_modify(self, realm_name, zonegroup_name, zone_name, hostnames):
        if realm_name is None:
            raise RGWAMException('Realm name is a mandatory parameter')
        if zone_name is None:
            raise RGWAMException('Zone name is a mandatory parameter')
        if zonegroup_name is None:
            raise RGWAMException('Zonegroup name is a mandatory parameter')

        realm = EntityName(realm_name)
        zone = EntityName(zone_name)
        period_info = self.period_op().get(realm)
        period = RGWPeriod(period_info)
        logging.info('Period: ' + period.id)
        zonegroup = period.find_zonegroup_by_name(zonegroup_name)
        if not zonegroup:
            raise RGWAMException(f'zonegroup {zonegroup_name} not found')
        zg = EntityName(zonegroup.name)
        zg_json = self.zonegroup_op().get(zg)

        if hostnames:
            zg_json['hostnames'] = hostnames

        try:
            self.zonegroup_op().set(zg, json.dumps(zg_json))
        except RGWAMException as e:
            raise RGWAMException('failed to set zonegroup', e)

        try:
            period_info = self.period_op().update(realm, zg, zone, True)
        except RGWAMException as e:
            raise RGWAMException('failed to update period', e)

        period = RGWPeriod(period_info)
        logging.debug(period.to_json())

        return (0, f'Modified zonegroup {zonegroup_name} of realm {realm_name}', '')

    def get_realms_info(self):
        realms_info = []
        for realm_name in self.realm_op().list():
            realm = self.get_realm(realm_name)
            master_zone_inf = self.period_op().get_master_zone(realm)
            zone_ep = self.period_op().get_master_zone_ep(realm)
            if master_zone_inf and 'system_key' in master_zone_inf:
                access_key = master_zone_inf['system_key']['access_key']
                secret = master_zone_inf['system_key']['secret_key']
            else:
                access_key = ''
                secret = ''
            realms_info.append({"realm_name": realm_name,
                                "realm_id": realm.id,
                                "master_zone_id": master_zone_inf['id'] if master_zone_inf else '',
                                "endpoint": zone_ep[0] if zone_ep else None,
                                "access_key": access_key,
                                "secret": secret})
        return realms_info

    def zone_create(self, rgw_spec, start_radosgw):

        if not rgw_spec.rgw_realm_token:
            raise RGWAMException('missing realm token')
        if rgw_spec.rgw_zone is None:
            raise RGWAMException('Zone name is a mandatory parameter')
        if rgw_spec.rgw_zone in self.zone_op().list():
            raise RGWAMException(f'Zone {rgw_spec.rgw_zone} already exists')

        realm_token = RealmToken.from_base64_str(rgw_spec.rgw_realm_token)
        if realm_token.endpoint is None:
            raise RGWAMException('Provided realm token has no endpoint')

        access_key = realm_token.access_key
        secret = realm_token.secret
        try:
            realm_info = self.realm_op().pull(EntityName(realm_token.realm_name),
                                              realm_token.endpoint, access_key, secret)
        except RGWAMException as e:
            raise RGWAMException('failed to pull realm', e)

        logging.info(f"Pulled realm {realm_info['name']} ({realm_info['id']})")
        realm_name = realm_info['name']
        realm_id = realm_info['id']

        realm = EntityID(realm_id)
        period_info = self.period_op().get(realm)
        period = RGWPeriod(period_info)
        logging.info('Period: ' + period.id)

        zonegroup = period.get_master_zonegroup()
        if not zonegroup:
            raise RGWAMException(f'Cannot find master zonegroup of realm {realm_name}')

        zone = self.create_zone(realm, zonegroup, rgw_spec.rgw_zone,
                                False,  # secondary zone
                                access_key, secret, endpoints=rgw_spec.zone_endpoints)
        self.update_period(realm, zonegroup, zone)

        period = RGWPeriod(period_info)
        logging.debug(period.to_json())

        if start_radosgw and rgw_spec.zone_endpoints is None:
            secondary_realm_token = RealmToken(realm_name,
                                               realm_id,
                                               None,   # no endpoint
                                               realm_token.access_key,
                                               realm_token.secret)
            realm_token_b = secondary_realm_token.to_json().encode('utf-8')
            realm_token_s = base64.b64encode(realm_token_b).decode('utf-8')
            rgw_spec.update_endpoints = True
            rgw_spec.rgw_token = realm_token_s
            rgw_spec.rgw_zonegroup = zonegroup.name  # master zonegroup is used
            self.env.mgr.apply_rgw(rgw_spec)

    def _get_daemon_eps(self, realm_name=None, zonegroup_name=None, zone_name=None):
        # get running daemons info
        service_name = None
        if realm_name and zone_name:
            service_name = 'rgw.%s.%s' % (realm_name, zone_name)

        daemon_type = 'rgw'
        daemon_id = None
        hostname = None
        refresh = True

        daemons = self.env.mgr.list_daemons(service_name,
                                            daemon_type,
                                            daemon_id=daemon_id,
                                            host=hostname,
                                            refresh=refresh)

        rep = RealmsEPs()

        for s in daemons:
            for p in s.ports:
                svc_id = s.service_id()
                sp = svc_id.split('.')
                if len(sp) < 2:
                    log.error('ERROR: service id cannot be parsed: (svc_id=%s)' % svc_id)
                    continue

                svc_realm = sp[0]
                svc_zone = sp[1]

                if realm_name and svc_realm != realm_name:
                    log.debug('skipping realm %s' % svc_realm)
                    continue

                if zone_name and svc_zone != zone_name:
                    log.debug('skipping zone %s' % svc_zone)
                    continue

                ep = 'http://%s:%d' % (s.hostname, p)  # ssl?

                rep.add(svc_realm, svc_zone, ep)

        return rep

    def _get_rgw_eps(self, realm_name=None, zonegroup_name=None, zone_name=None):
        rep = RealmsEPs()

        try:
            realms = self.realm_op().list()
        except RGWAMException as e:
            raise RGWAMException('failed to list realms', e)

        zones_map = {}
        for realm in realms:
            if realm_name and realm != realm_name:
                log.debug('skipping realm %s' % realm)
                continue

            period_info = self.period_op().get(EntityName(realm))

            period = RGWPeriod(period_info)

            zones_map[realm] = {}

            for zg in period.iter_zonegroups():
                if zonegroup_name and zg.name != zonegroup_name:
                    log.debug('skipping zonegroup %s' % zg.name)
                    continue

                for zone in zg.iter_zones():
                    if zone_name and zone.name != zone_name:
                        log.debug('skipping zone %s' % zone.name)
                        continue

                    zones_map[realm][zone.name] = zg.name

                    if len(zone.endpoints) == 0:
                        rep.add(realm, zone.name, None)
                        continue

                    for ep in zone.endpoints:
                        rep.add(realm, zone.name, ep)

        return (rep, zones_map)

    def realm_reconcile(self, realm_name=None, zonegroup_name=None, zone_name=None, update=False):

        daemon_rep = self._get_daemon_eps(realm_name, zonegroup_name, zone_name)

        rgw_rep, zones_map = self._get_rgw_eps(realm_name, zonegroup_name, zone_name)

        diff = daemon_rep.diff(rgw_rep)

        diffj = json.dumps(diff)

        if not update:
            return (0, diffj, '')

        for realm, realm_diff in diff.items():
            for zone, endpoints in realm_diff.items():

                zg = zones_map[realm][zone]

                try:
                    self.zone_op().modify(EntityName(zone), EntityName(zg),
                                          endpoints=','.join(diff[realm][zone]))
                except RGWAMException as e:
                    raise RGWAMException('failed to modify zone', e)

            try:
                self.period_op().update(EntityName(realm), EntityName(zg), EntityName(zone), True)
            except RGWAMException as e:
                raise RGWAMException('failed to update period', e)

        return (0, 'Updated: ' + diffj, '')

    def run_radosgw(self, port=None, log_file=None, debug_ms=None, debug_rgw=None):

        fe_cfg = 'beast'
        if port:
            fe_cfg += ' port=%s' % port

        params = ['--rgw-frontends', fe_cfg]

        if log_file:
            params += ['--log-file', log_file]

        if debug_ms:
            params += ['--debug-ms', debug_ms]

        if debug_rgw:
            params += ['--debug-rgw', debug_rgw]

        (retcode, stdout, stderr) = RGWCmd(self.env).run(params)

        return (retcode, stdout, stderr)
