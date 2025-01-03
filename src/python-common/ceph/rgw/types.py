import json
import base64
import binascii
import errno

from abc import abstractmethod


class RGWAMException(Exception):
    def __init__(self, message, orig=None):
        if orig:
            self.message = message + ': ' + orig.message
            self.retcode = orig.retcode
            self.stdout = orig.stdout
            self.stderr = orig.stdout
        else:
            self.message = message
            self.retcode = -errno.EINVAL
            self.stdout = ''
            self.stderr = message


class RGWAMCmdRunException(RGWAMException):
    def __init__(self, cmd, retcode, stdout, stderr):
        super().__init__('Command error (%d): %s\nstdout:%s\nstderr:%s' %
                         (retcode, cmd, stdout, stderr))
        self.retcode = retcode
        self.stdout = stdout
        self.stderr = stderr


class RGWAMEnvMgr:
    @abstractmethod
    def tool_exec(self, prog, args):
        pass

    @abstractmethod
    def apply_rgw(self, spec):
        pass

    @abstractmethod
    def list_daemons(self, service_name, daemon_type=None, daemon_id=None, hostname=None,
                     refresh=True):
        pass


class JSONObj:
    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, indent=4)


class RealmToken(JSONObj):
    def __init__(self, realm_name, realm_id, endpoint, access_key, secret):
        self.realm_name = realm_name
        self.realm_id = realm_id
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret = secret

    @classmethod
    def from_base64_str(cls, realm_token_b64):
        try:
            realm_token_b = base64.b64decode(realm_token_b64)
            realm_token_s = realm_token_b.decode('utf-8')
            realm_token = json.loads(realm_token_s)
            return cls(**realm_token)
        except binascii.Error:
            return None


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

    def iter_zones(self):
        for zone in self.zones_by_id.values():
            yield zone


class RGWPeriod(JSONObj):
    def __init__(self, period_dict):
        self.id = period_dict['id']
        self.epoch = period_dict['epoch']
        self.master_zone = period_dict['master_zone']
        self.master_zonegroup = period_dict['master_zonegroup']
        self.realm_id = period_dict['realm_id']
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

    def get_master_zonegroup(self):
        return self.find_zonegroup_by_id(self.master_zonegroup)

    def find_zonegroup_by_id(self, zonegroup):
        return self.zonegroups_by_id.get(zonegroup)

    def get_zone_endpoints(self, zonegroup_id, zone_id):
        zg = self.zonegroups_by_id.get(zonegroup_id)
        if not zg:
            return None

        return zg.get_zone_endpoints(zone_id)

    def iter_zonegroups(self):
        for zg in self.zonegroups_by_id.values():
            yield zg


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

    def add_key(self, access_key, secret):
        self.keys.append(RGWAccessKey({'user': self.uid,
                                       'access_key': access_key,
                                       'secret_key': secret}))

    def get_key(self, index):
        return self.keys[index] if index < len(self.keys) else None
