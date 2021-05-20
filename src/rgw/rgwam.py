import subprocess
import random
import string
import json
import argparse
import sys
import socket
import base64
import logging

from urllib.parse import urlparse

DEFAULT_PORT = 8000

class RGWAMException:
    def __init__(self, message):
        self.message = message


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

class JSONObj:
    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)

class RealmToken(JSONObj):
    def __init__(self, endpoint, uid, access_key, secret):
        self.endpoint = endpoint
        self.uid = uid
        self.access_key = access_key
        self.secret = secret

class RGWZone(JSONObj):
    def __init__(self, zone_dict):
        self.id = zone_dict['id']
        self.name = zone_dict['name']
        self.endpoints = zone_dict['endpoints']

class RGWZoneGroup(JSONObj):
    def __init__(self, zg_dict):
        self.id = zg_dict['id']
        self.name = zg_dict['name']
        self.api_name = zg_dict['api_name']
        self.is_master = zg_dict['is_master']
        self.endpoints = zg_dict['endpoints']

        self.zones_by_id = {}
        self.zones_by_name = {}
        self.all_endpoints = []

        for zone in zg_dict['zones']:
            z = RGWZone(zone)
            self.zones_by_id[zone['id']] = z
            self.zones_by_name[zone['name']] = z
            self.all_endpoints += z.endpoints

    def endpoint_exists(self, endpoint):
        for ep in self.all_endpoints:
            if ep == endpoint:
                return True
        return False

    def get_zone_endpoints(self, zone_id):
        z = self.zones_by_id.get(zone_id)
        if not z:
            return None

        return z.endpoints

class RGWPeriod(JSONObj):
    def __init__(self, period_dict):
        self.id = period_dict['id']
        self.epoch = period_dict['epoch']
        self.master_zone = period_dict['master_zone']
        self.master_zonegroup = period_dict['master_zonegroup']
        pm = period_dict['period_map']
        self.zonegroups_by_id = {}
        self.zonegroups_by_name = {}

        for zg in pm['zonegroups']:
            self.zonegroups_by_id[zg['id']] = RGWZoneGroup(zg)
            self.zonegroups_by_name[zg['name']] = RGWZoneGroup(zg)

    def endpoint_exists(self, endpoint):
        for _, zg in self.zonegroups_by_id.items():
            if zg.endpoint_exists(endpoint):
                return True
        return False
    
    def find_zonegroup_by_name(self, zonegroup):
        if not zonegroup:
            return self.find_zonegroup_by_id(self.master_zonegroup)
        return self.zonegroups_by_name.get(zonegroup)

    def find_zonegroup_by_id(self, zonegroup):
        return self.zonegroups_by_id.get(zonegroup)

    def get_zone_endpoints(self, zonegroup_id, zone_id):
        zg = self.zonegroups_by_id.get(zonegroup_id)
        if not zg:
            return None

        return zg.get_zone_endpoints(zone_id)

        

class RGWAccessKey(JSONObj):
    def __init__(self, d):
        self.uid = d['user']
        self.access_key = d['access_key']
        self.secret_key = d['secret_key']

class RGWUser(JSONObj):
    def __init__(self, d):
        self.uid = d['user_id']
        self.display_name = d['display_name']
        self.email = d['email']

        self.keys = []

        for k in d['keys']:
            self.keys.append(RGWAccessKey(k))

        is_system = d.get('system') or 'false'
        self.system = (is_system == 'true')



class RGWAMException(BaseException):
    def __init__(self, message):
        self.message = message


class RGWCmdBase:
    def __init__(self, prog, common_args):
        self.cmd_prefix = [ prog ]
        if common_args.ceph_name:
            self.cmd_prefix += [ '-n', common_args.ceph_name ]
        if common_args.ceph_keyring:
            self.cmd_prefix += [ '-k', common_args.ceph_keyring ]

    def run(self, cmd):
        run_cmd = self.cmd_prefix + cmd
        result = subprocess.run(run_cmd, stdout=subprocess.PIPE)
        return (result.returncode, result.stdout)

class RGWAdminCmd(RGWCmdBase):
    def __init__(self, common_args):
        super().__init__('radosgw-admin', common_args)

class RGWCmd(RGWCmdBase):
    def __init__(self, common_args):
        super().__init__('radosgw', common_args)

class RealmOp(RGWAdminCmd):
    def __init__(self, common_args):
        super().__init__(common_args)
        
    def get(self):
        params = [ 'realm',
                   'get' ]

        retcode, stdout = RGWAdminCmd.run(self, params)
        if retcode != 0:
            return None

        self.info = json.loads(stdout)

        return self.info

    def create(self, name = None, is_default = True):
        self.name = name
        if not self.name:
            self.name = 'realm-' + rand_alphanum_lower(8)

        params = [ 'realm',
                   'create',
                   '--rgw-realm', self.name ]

        if is_default:
            params += [ '--default' ]

        retcode, stdout = RGWAdminCmd.run(self, params)
        if retcode != 0:
            return None

        self.info = json.loads(stdout)

        return self.info

    def pull(self, url, access_key, secret, set_default = False):
        params = [ 'realm',
                   'pull',
                   '--url', url,
                   '--access-key', access_key,
                   '--secret', secret ]

        if set_default:
            params += [ '--default' ]

        retcode, stdout = RGWAdminCmd.run(self, params)
        if retcode != 0:
            return None

        self.info = json.loads(stdout)

        self.name = self.info['name']

        return self.info

class ZonegroupOp(RGWAdminCmd):
    def __init__(self, common_args):
        super().__init__(common_args)
        
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

        retcode, stdout = RGWAdminCmd.run(self, params)
        if retcode != 0:
            return None

        self.info = json.loads(stdout)

        return self.info

class ZoneOp(RGWAdminCmd):
    def __init__(self, common_args):
        super().__init__(common_args)
        
    def get(self):
        params = [ 'zone',
                   'get' ]

        retcode, stdout = RGWAdminCmd.run(self, params)
        if retcode != 0:
            return None

        self.info = json.loads(stdout)

        return self.info

    def create(self, realm, zonegroup, name = None, endpoints = None, is_master = True, is_default = True,
               access_key = None, secret = None):
        self.name = name
        if not self.name:
            self.name = 'zg-' + rand_alphanum_lower(8)

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

        retcode, stdout = RGWAdminCmd.run(self, params)
        if retcode != 0:
            return None

        self.info = json.loads(stdout)

        return self.info

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

        retcode, stdout = RGWAdminCmd.run(self, params)
        if retcode != 0:
            return None

        self.info = json.loads(stdout)

        return self.info

class PeriodOp(RGWAdminCmd):
    def __init__(self, common_args):
        super().__init__(common_args)
        
    def update(self, realm, commit = True):

        params = [ 'period',
                   'update',
                   '--rgw-realm', realm ]

        if commit:
            params += [ '--commit' ]

        retcode, stdout = RGWAdminCmd.run(self, params)
        if retcode != 0:
            return None

        self.info = json.loads(stdout)

        return self.info

    def get(self, realm = None):
        params = [ 'period',
                   'get' ]

        if realm:
            params += [ '--rgw-realm', realm ]

        retcode, stdout = RGWAdminCmd.run(self, params)
        if retcode != 0:
            return None

        self.info = json.loads(stdout)

        return self.info

class UserOp(RGWAdminCmd):
    def __init__(self, common_args):
        super().__init__(common_args)
        
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

        retcode, stdout = RGWAdminCmd.run(self, params)
        if retcode != 0:
            return None

        self.info = json.loads(stdout)

        return self.info

class RGWAM:
    def __init__(self, common_args):
        self.common_args = common_args

    def realm_op(self):
        return RealmOp(self.common_args)

    def period_op(self):
        return PeriodOp(self.common_args)

    def zonegroup_op(self):
        return ZonegroupOp(self.common_args)

    def zone_op(self):
        return ZoneOp(self.common_args)

    def user_op(self):
        return UserOp(self.common_args)

    def realm_bootstrap(self, realm, zonegroup, zone, endpoints, sys_uid, uid, start_radosgw):
        endpoints = get_endpoints(endpoints)

        realm_info = self.realm_op().create(realm)
        if not realm_info:
            raise RGWAMException('failed to create realm')

        realm_name = realm_info['name']
        realm_id = realm_info['id']
        logging.info('Created realm %s (%s)' % (realm_name, realm_id))

        zg_info = self.zonegroup_op().create(realm_name, zonegroup, endpoints, True, True)
        if not zg_info:
            raise RGWAMException('failed to create zonegroup')

        zg_name = zg_info['name']
        zg_id = zg_info['id']
        logging.info('Created zonegroup %s (%s)' % (zg_name, zg_id))

        zone_info = self.zone_op().create(realm_name, zg_name, zone, endpoints, True, True)
        if not zone_info:
            raise RGWAMException('failed to create zone')

        zone_name = zone_info['name']
        zone_id = zone_info['id']
        logging.info('Created zone %s (%s)' % (zone_name, zone_id))

        period_info = self.period_op().update(realm_name, True)
        if not period_info:
            raise RGWAMException('failed to update period')

        period = RGWPeriod(period_info)

        logging.info('Period: ' + period.id)

        sys_user_info = self.user_op().create(uid = sys_uid, uid_prefix = 'user-sys', is_system = True)
        if not sys_user_info:
            raise RGWAMException('failed to create system user')

        sys_user = RGWUser(sys_user_info)

        logging.info('Created system user: %s' % sys_user.uid)

        sys_access_key = ''
        sys_secret = ''

        if len(sys_user.keys) > 0:
            sys_access_key = sys_user.keys[0].access_key
            sys_secret = sys_user.keys[0].secret_key

        zone_info = self.zone_op().modify(endpoints, None, None, sys_access_key, sys_secret)
        if not zone_info:
            raise RGWAMException('failed to modify zone info')

        user_info = self.user_op().create(uid = uid, is_system = False)
        if not user_info:
            raise RGWAMException('failed to create user')

        user = RGWUser(user_info)

        logging.info('Created regular user: %s' % user.uid)

        eps = endpoints.split(',')
        ep = ''
        if len(eps) > 0:
            ep = eps[0]
            if start_radosgw:
                o = urlparse(ep)
                self.run_radosgw(port = o.port)

        realm_token = RealmToken(ep, sys_user.uid, sys_access_key, sys_secret)

        logging.info(realm_token.to_json())

        realm_token_b = realm_token.to_json().encode('utf-8')
        print('Realm Token: %s' % base64.b64encode(realm_token_b).decode('utf-8'))

        return True

    def realm_new_zone_creds(self, endpoints, sys_uid):
        period_info = self.period_op().get()
        if not period_info:
            return

        period = RGWPeriod(period_info)

        zone_info = self.zone_op().get()
        if not zone_info:
            raise RGWAMException('failed to create zone')

        zone_name = zone_info['name']
        zone_id = zone_info['id']

        logging.info('Period: ' + period.id)
        logging.info('Master zone: ' + period.master_zone)
        logging.info('Current zone: ' + zone_id)

        if period.master_zone != zone_id:
            print('command needs to run on master zone')
            return False

        ep = ''
        if not endpoints:
            eps = period.get_zone_endpoints(period.master_zonegroup, period.master_zone)
        else:
            eps = endpoints.split(',')

        if len(eps) > 0:
            ep = eps[0]

        sys_user_info = self.user_op().create(uid = sys_uid, uid_prefix = 'user-sys', is_system = True)
        if not sys_user_info:
            raise RGWAMException('failed to create system user')

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
        print('Realm Token: %s' % base64.b64encode(realm_token_b).decode('utf-8'))

        return True

    def zone_create(self, realm_token_b64, zonegroup = None, zone = None, endpoints = None, start_radosgw = True):
        if not realm_token_b64:
            print('missing realm access config')
            return False

        realm_token_b = base64.b64decode(realm_token_b64)
        realm_token_s = realm_token_b.decode('utf-8')

        realm_token = json.loads(realm_token_s)

        access_key = realm_token['access_key']
        secret = realm_token['secret']

        realm_info = self.realm_op().pull(realm_token['endpoint'], access_key, secret, set_default = True)
        if not realm_info:
            raise RGWAMException('failed to pull realm')

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

        zone_info = self.zone_op().create(realm_name, zg.name, zone, endpoints, False, True,
                access_key, secret)
        if not zone_info:
            raise RGWAMException('failed to create zone')

        zone_name = zone_info['name']
        zone_id = zone_info['id']
        logging.info('Created zone %s (%s)' % (zone_name, zone_id))

        period_info = self.period_op().update(realm_name, True)
        if not period_info:
            raise RGWAMException('failed to update period')

        period = RGWPeriod(period_info)

        logging.debug(period.to_json())

        if start_radosgw:
            eps = endpoints.split(',')
            ep = ''
            if len(eps) > 0:
                ep = eps[0]
                o = urlparse(ep)
                ret = self.run_radosgw(port = o.port)
                if not ret:
                    logging.warning('failed to start radosgw')

        return True

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

        (retcode, _) = RGWCmd(self.common_args).run(params)

        return (retcode == 0)


class RealmCommand:
    def __init__(self, common_args, args):
        self.common_args = common_args
        self.args = args

    def parse(self):
        parser = argparse.ArgumentParser(
            usage='''rgwam realm <subcommand>

The subcommands are:
   bootstrap                     Bootstrap new realm
   new-zone-creds                Create credentials for connecting new zone
''')
        parser.add_argument('subcommand', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(self.args[0:1])

        sub = args.subcommand.replace('-', '_')

        if not hasattr(self, sub):
            print('Unrecognized subcommand:', args.subcommand)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name

        return getattr(self, sub)

    def bootstrap(self):
        parser = argparse.ArgumentParser(
            description='Bootstrap new realm',
            usage='rgwam realm bootstrap [<args>]')
        parser.add_argument('--realm')
        parser.add_argument('--zonegroup')
        parser.add_argument('--zone')
        parser.add_argument('--endpoints')
        parser.add_argument('--sys-uid')
        parser.add_argument('--uid')
        parser.add_argument('--start-radosgw', action='store_true', dest='start_radosgw', default=True)
        parser.add_argument('--no-start-radosgw', action='store_false', dest='start_radosgw')

        args = parser.parse_args(self.args[1:])

        return RGWAM(self.common_args).realm_bootstrap(args.realm, args.zonegroup, args.zone, args.endpoints,
                args.sys_uid, args.uid, args.start_radosgw)

    def new_zone_creds(self):
        parser = argparse.ArgumentParser(
            description='Bootstrap new realm',
            usage='rgwam realm new-zone-creds [<args>]')
        parser.add_argument('--endpoints')
        parser.add_argument('--sys-uid')

        args = parser.parse_args(self.args[1:])

        return RGWAM(self.common_args).realm_new_zone_creds(args.endpoints, args.sys_uid)


class ZoneCommand:
    def __init__(self, common_args, args):
        self.common_args = common_args
        self.args = args

    def parse(self):
        parser = argparse.ArgumentParser(
            usage='''rgwam zone <subcommand>

The subcommands are:
   run                     run radosgw daemon in current zone
''')
        parser.add_argument('subcommand', help='Subcommand to run')
        # parse_args defaults to [1:] for args, but you need to
        # exclude the rest of the args too, or validation will fail
        args = parser.parse_args(self.args[0:1])
        if not hasattr(self, args.subcommand):
            print('Unrecognized subcommand:', args.subcommand)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        return getattr(self, args.subcommand)

    def run(self):
        parser = argparse.ArgumentParser(
            description='Run radosgw daemon',
            usage='rgwam zone run [<args>]')
        parser.add_argument('--port')
        parser.add_argument('--log-file')
        parser.add_argument('--debug-ms')
        parser.add_argument('--debug-rgw')

        args = parser.parse_args(self.args[1:])

        return RGWAM(self.common_args).run_radosgw(port = args.port)

    def create(self):
        parser = argparse.ArgumentParser(
            description='Create new zone to join existing realm',
            usage='rgwam zone create [<args>]')
        parser.add_argument('--realm-token')
        parser.add_argument('--zone')
        parser.add_argument('--zonegroup')
        parser.add_argument('--endpoints')
        parser.add_argument('--start-radosgw', action='store_true', dest='start_radosgw', default=True)
        parser.add_argument('--no-start-radosgw', action='store_false', dest='start_radosgw')

        args = parser.parse_args(self.args[1:])

        return RGWAM(self.common_args).zone_create(args.realm_token, args.zonegroup, args.zone, args.endpoints, args.start_radosgw)

class CommonArgs:
    def __init__(self, ns):
        self.ceph_name = ns.ceph_name
        self.ceph_keyring = ns.ceph_keyring

class TopLevelCommand:

    def _parse(self):
        parser = argparse.ArgumentParser(
            description='RGW assist for multisite tool',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog='''
The commands are:
   realm bootstrap               Bootstrap new realm
   realm new-zone-creds          Create credentials to connect new zone to realm
   zone create                   Create new zone and connect it to existing realm
   zone run                      Run radosgw in current zone
''')

        parser.add_argument('command', help='command to run', default=None)
        parser.add_argument('-n', help='ceph user name', dest='ceph_name')
        parser.add_argument('-k', help='ceph keyring', dest='ceph_keyring')

        removed_args = []

        args = sys.argv[1:]
        if len(args) > 0:
            if hasattr(self, args[0]):
                # remove -h/--help if top command is not empty so that top level help
                # doesn't override subcommand, we'll add it later
                help_args = [ '-h', '--help' ]
                removed_args = [arg for arg in args if arg in help_args]
                args = [arg for arg in args if arg not in help_args]

        (ns, args) = parser.parse_known_args(args)
        if not hasattr(self, ns.command) or ns.command[0] == '_':
            print('Unrecognized command:', ns.command)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        args += removed_args
        return (getattr(self, ns.command), CommonArgs(ns), args)

    def realm(self, common_args, args):
        cmd = RealmCommand(common_args, args).parse()
        return cmd()

    def zone(self, common_args, args):
        cmd = ZoneCommand(common_args, args).parse()
        return cmd()


def main():
    logging.basicConfig(level=logging.INFO)

    (cmd, common_args, args)= TopLevelCommand()._parse()
    try:
        ret = cmd(common_args, args)
        if not ret:
            sys.exit(1)
    except RGWAMException as e:
        print('ERROR: ' + e.message)

    sys.exit(0)


if __name__ == '__main__':
    main()

