from abc import ABCMeta, abstractmethod
from six import StringIO

import json

from .conn import get_gateway_connection, get_gateway_secure_connection

class Cluster:
    """ interface to run commands against a distinct ceph cluster """
    __metaclass__ = ABCMeta

    @abstractmethod
    def admin(self, args = None, **kwargs):
        """ execute a radosgw-admin command """
        pass

class Gateway:
    """ interface to control a single radosgw instance """
    __metaclass__ = ABCMeta

    def __init__(self, host = None, port = None, cluster = None, zone = None, ssl_port = 0):
        self.host = host
        self.port = port
        self.cluster = cluster
        self.zone = zone
        self.connection = None
        self.secure_connection = None
        self.ssl_port = ssl_port

    @abstractmethod
    def start(self, args = []):
        """ start the gateway with the given args """
        pass

    @abstractmethod
    def stop(self):
        """ stop the gateway """
        pass

    def endpoint(self):
        return 'http://%s:%d' % (self.host, self.port)

class SystemObject:
    """ interface for system objects, represented in json format and
    manipulated with radosgw-admin commands """
    __metaclass__ = ABCMeta

    def __init__(self, data = None, uuid = None):
        self.data = data
        self.id = uuid
        if data:
            self.load_from_json(data)

    @abstractmethod
    def build_command(self, command):
        """ return the command line for the given command, including arguments
        to specify this object """
        pass

    @abstractmethod
    def load_from_json(self, data):
        """ update internal state based on json data """
        pass

    def command(self, cluster, cmd, args = None, **kwargs):
        """ run the given command and return the output and retcode """
        args = self.build_command(cmd) + (args or [])
        return cluster.admin(args, **kwargs)

    def json_command(self, cluster, cmd, args = None, **kwargs):
        """ run the given command, parse the output and return the resulting
        data and retcode """
        s, r = self.command(cluster, cmd, args or [], **kwargs)
        if r == 0:
            data = json.loads(s)
            self.load_from_json(data)
            self.data = data
        return self.data, r

    # mixins for supported commands
    class Create(object):
        def create(self, cluster, args = None, **kwargs):
            """ create the object with the given arguments """
            return self.json_command(cluster, 'create', args, **kwargs)

    class Delete(object):
        def delete(self, cluster, args = None, **kwargs):
            """ delete the object """
            # not json_command() because delete has no output
            _, r = self.command(cluster, 'delete', args, **kwargs)
            if r == 0:
                self.data = None
            return r

    class Get(object):
        def get(self, cluster, args = None, **kwargs):
            """ read the object from storage """
            kwargs['read_only'] = True
            return self.json_command(cluster, 'get', args, **kwargs)

    class Set(object):
        def set(self, cluster, data, args = None, **kwargs):
            """ set the object by json """
            kwargs['stdin'] = StringIO(json.dumps(data))
            return self.json_command(cluster, 'set', args, **kwargs)

    class Modify(object):
        def modify(self, cluster, args = None, **kwargs):
            """ modify the object with the given arguments """
            return self.json_command(cluster, 'modify', args, **kwargs)

    class CreateDelete(Create, Delete): pass
    class GetSet(Get, Set): pass

class Zone(SystemObject, SystemObject.CreateDelete, SystemObject.GetSet, SystemObject.Modify):
    def __init__(self, name, zonegroup = None, cluster = None, data = None, zone_id = None, gateways = None):
        self.name = name
        self.zonegroup = zonegroup
        self.cluster = cluster
        self.gateways = gateways or []
        super(Zone, self).__init__(data, zone_id)

    def zone_arg(self):
        """ command-line argument to specify this zone """
        return ['--rgw-zone', self.name]

    def zone_args(self):
        """ command-line arguments to specify this zone/zonegroup/realm """
        args = self.zone_arg()
        if self.zonegroup:
            args += self.zonegroup.zonegroup_args()
        return args

    def build_command(self, command):
        """ build a command line for the given command and args """
        return ['zone', command] + self.zone_args()

    def load_from_json(self, data):
        """ load the zone from json """
        self.id = data['id']
        self.name = data['name']

    def start(self, args = None):
        """ start all gateways """
        for g in self.gateways:
            g.start(args)

    def stop(self):
        """ stop all gateways """
        for g in self.gateways:
            g.stop()

    def period(self):
        return self.zonegroup.period if self.zonegroup else None

    def realm(self):
        return self.zonegroup.realm() if self.zonegroup else None

    def is_read_only(self):
        return False

    def tier_type(self):
        raise NotImplementedError

    def syncs_from(self, zone_name):
        return zone_name != self.name

    def has_buckets(self):
        return True

    def get_conn(self, credentials):
        return ZoneConn(self, credentials) # not implemented, but can be used

class ZoneConn(object):
    def __init__(self, zone, credentials):
        self.zone = zone
        self.name = zone.name
        """ connect to the zone's first gateway """
        if isinstance(credentials, list):
            self.credentials = credentials[0]
        else:
            self.credentials = credentials

        if self.zone.gateways is not None:
            self.conn = get_gateway_connection(self.zone.gateways[0], self.credentials)
            self.secure_conn = get_gateway_secure_connection(self.zone.gateways[0], self.credentials)

    def get_connection(self):
        return self.conn

    def get_bucket(self, bucket_name, credentials):
        raise NotImplementedError

    def check_bucket_eq(self, zone, bucket_name):
        raise NotImplementedError

class ZoneGroup(SystemObject, SystemObject.CreateDelete, SystemObject.GetSet, SystemObject.Modify):
    def __init__(self, name, period = None, data = None, zonegroup_id = None, zones = None, master_zone  = None):
        self.name = name
        self.period = period
        self.zones = zones or []
        self.master_zone = master_zone
        super(ZoneGroup, self).__init__(data, zonegroup_id)
        self.rw_zones = []
        self.ro_zones = []
        self.zones_by_type = {}
        for z in self.zones:
            if z.is_read_only():
                self.ro_zones.append(z)
            else:
                self.rw_zones.append(z)

    def zonegroup_arg(self):
        """ command-line argument to specify this zonegroup """
        return ['--rgw-zonegroup', self.name]

    def zonegroup_args(self):
        """ command-line arguments to specify this zonegroup/realm """
        args = self.zonegroup_arg()
        realm = self.realm()
        if realm:
            args += realm.realm_arg()
        return args

    def build_command(self, command):
        """ build a command line for the given command and args """
        return ['zonegroup', command] + self.zonegroup_args()

    def zone_by_id(self, zone_id):
        """ return the matching zone by id """
        for zone in self.zones:
            if zone.id == zone_id:
                return zone
        return None

    def load_from_json(self, data):
        """ load the zonegroup from json """
        self.id = data['id']
        self.name = data['name']
        master_id = data['master_zone']
        if not self.master_zone or master_id != self.master_zone.id:
            self.master_zone = self.zone_by_id(master_id)

    def add(self, cluster, zone, args = None, **kwargs):
        """ add an existing zone to the zonegroup """
        args = zone.zone_arg() + (args or [])
        data, r = self.json_command(cluster, 'add', args, **kwargs)
        if r == 0:
            zone.zonegroup = self
            self.zones.append(zone)
        return data, r

    def remove(self, cluster, zone, args = None, **kwargs):
        """ remove an existing zone from the zonegroup """
        args = zone.zone_arg() + (args or [])
        data, r = self.json_command(cluster, 'remove', args, **kwargs)
        if r == 0:
            zone.zonegroup = None
            self.zones.remove(zone)
        return data, r

    def realm(self):
        return self.period.realm if self.period else None

class Period(SystemObject, SystemObject.Get):
    def __init__(self, realm = None, data = None, period_id = None, zonegroups = None, master_zonegroup = None):
        self.realm = realm
        self.zonegroups = zonegroups or []
        self.master_zonegroup = master_zonegroup
        super(Period, self).__init__(data, period_id)

    def zonegroup_by_id(self, zonegroup_id):
        """ return the matching zonegroup by id """
        for zonegroup in self.zonegroups:
            if zonegroup.id == zonegroup_id:
                return zonegroup
        return None

    def build_command(self, command):
        """ build a command line for the given command and args """
        return ['period', command]

    def load_from_json(self, data):
        """ load the period from json """
        self.id = data['id']
        master_id = data['master_zonegroup']
        if not self.master_zonegroup or master_id != self.master_zonegroup.id:
            self.master_zonegroup = self.zonegroup_by_id(master_id)

    def update(self, zone, args = None, **kwargs):
        """ run 'radosgw-admin period update' on the given zone """
        assert(zone.cluster)
        args = zone.zone_args() + (args or [])
        if kwargs.pop('commit', False):
            args.append('--commit')
        return self.json_command(zone.cluster, 'update', args, **kwargs)

    def commit(self, zone, args = None, **kwargs):
        """ run 'radosgw-admin period commit' on the given zone """
        assert(zone.cluster)
        args = zone.zone_args() + (args or [])
        return self.json_command(zone.cluster, 'commit', args, **kwargs)

class Realm(SystemObject, SystemObject.CreateDelete, SystemObject.GetSet):
    def __init__(self, name, period = None, data = None, realm_id = None):
        self.name = name
        self.current_period = period
        super(Realm, self).__init__(data, realm_id)

    def realm_arg(self):
        """ return the command-line arguments that specify this realm """
        return ['--rgw-realm', self.name]

    def build_command(self, command):
        """ build a command line for the given command and args """
        return ['realm', command] + self.realm_arg()

    def load_from_json(self, data):
        """ load the realm from json """
        self.id = data['id']

    def pull(self, cluster, gateway, credentials, args = [], **kwargs):
        """ pull an existing realm from the given gateway """
        args += ['--url', gateway.endpoint()]
        args += credentials.credential_args()
        return self.json_command(cluster, 'pull', args, **kwargs)

    def master_zonegroup(self):
        """ return the current period's master zonegroup """
        if self.current_period is None:
            return None
        return self.current_period.master_zonegroup

    def meta_master_zone(self):
        """ return the current period's metadata master zone """
        zonegroup = self.master_zonegroup()
        if zonegroup is None:
            return None
        return zonegroup.master_zone

class Credentials:
    def __init__(self, access_key, secret):
        self.access_key = access_key
        self.secret = secret

    def credential_args(self):
        return ['--access-key', self.access_key, '--secret', self.secret]

class User(SystemObject):
    def __init__(self, uid, data = None, name = None, credentials = None, tenant = None):
        self.name = name
        self.credentials = credentials or []
        self.tenant = tenant
        super(User, self).__init__(data, uid)

    def user_arg(self):
        """ command-line argument to specify this user """
        args = ['--uid', self.id]
        if self.tenant:
            args += ['--tenant', self.tenant]
        return args

    def build_command(self, command):
        """ build a command line for the given command and args """
        return ['user', command] + self.user_arg()

    def load_from_json(self, data):
        """ load the user from json """
        self.id = data['user_id']
        self.name = data['display_name']
        self.credentials = [Credentials(k['access_key'], k['secret_key']) for k in data['keys']]

    def create(self, zone, args = None, **kwargs):
        """ create the user with the given arguments """
        assert(zone.cluster)
        args = zone.zone_args() + (args or [])
        return self.json_command(zone.cluster, 'create', args, **kwargs)

    def info(self, zone, args = None, **kwargs):
        """ read the user from storage """
        assert(zone.cluster)
        args = zone.zone_args() + (args or [])
        kwargs['read_only'] = True
        return self.json_command(zone.cluster, 'info', args, **kwargs)

    def delete(self, zone, args = None, **kwargs):
        """ delete the user """
        assert(zone.cluster)
        args = zone.zone_args() + (args or [])
        return self.command(zone.cluster, 'delete', args, **kwargs)
