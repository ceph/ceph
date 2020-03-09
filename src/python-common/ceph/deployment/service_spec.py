import fnmatch
import re
from collections import namedtuple
from typing import Optional, Dict, Any, List

import six


class ServiceSpecValidationError(Exception):
    """
    Defining an exception here is a bit problematic, cause you cannot properly catch it,
    if it was raised in a different mgr module.
    """

    def __init__(self, msg):
        super(ServiceSpecValidationError, self).__init__(msg)


class HostPlacementSpec(namedtuple('HostPlacementSpec', ['hostname', 'network', 'name'])):
    def __str__(self):
        res = ''
        res += self.hostname
        if self.network:
            res += ':' + self.network
        if self.name:
            res += '=' + self.name
        return res

    @classmethod
    def from_json(cls, data):
        return cls(**data)

    def to_json(self):
        return {
            'hostname': self.hostname,
            'network': self.network,
            'name': self.name
        }

    @classmethod
    def parse(cls, host, require_network=True):
        # type: (str, bool) -> HostPlacementSpec
        """
        Split host into host, network, and (optional) daemon name parts.  The network
        part can be an IP, CIDR, or ceph addrvec like '[v2:1.2.3.4:3300,v1:1.2.3.4:6789]'.
        e.g.,
          "myhost"
          "myhost=name"
          "myhost:1.2.3.4"
          "myhost:1.2.3.4=name"
          "myhost:1.2.3.0/24"
          "myhost:1.2.3.0/24=name"
          "myhost:[v2:1.2.3.4:3000]=name"
          "myhost:[v2:1.2.3.4:3000,v1:1.2.3.4:6789]=name"
        """
        # Matches from start to : or = or until end of string
        host_re = r'^(.*?)(:|=|$)'
        # Matches from : to = or until end of string
        ip_re = r':(.*?)(=|$)'
        # Matches from = to end of string
        name_re = r'=(.*?)$'

        # assign defaults
        host_spec = cls('', '', '')

        match_host = re.search(host_re, host)
        if match_host:
            host_spec = host_spec._replace(hostname=match_host.group(1))

        name_match = re.search(name_re, host)
        if name_match:
            host_spec = host_spec._replace(name=name_match.group(1))

        ip_match = re.search(ip_re, host)
        if ip_match:
            host_spec = host_spec._replace(network=ip_match.group(1))

        if not require_network:
            return host_spec

        from ipaddress import ip_network, ip_address
        networks = list()  # type: List[str]
        network = host_spec.network
        # in case we have [v2:1.2.3.4:3000,v1:1.2.3.4:6478]
        if ',' in network:
            networks = [x for x in network.split(',')]
        else:
            networks.append(network)
        for network in networks:
            # only if we have versioned network configs
            if network.startswith('v') or network.startswith('[v'):
                network = network.split(':')[1]
            try:
                # if subnets are defined, also verify the validity
                if '/' in network:
                    ip_network(six.text_type(network))
                else:
                    ip_address(six.text_type(network))
            except ValueError as e:
                # logging?
                raise e

        return host_spec


class PlacementSpec(object):
    """
    For APIs that need to specify a host subset
    """

    def __init__(self, label=None, hosts=None, count=None, all_hosts=False, host_pattern=None):
        # type: (Optional[str], Optional[List], Optional[int], bool, Optional[str]) -> None
        if all_hosts and (count or hosts or label):
            raise ServiceSpecValidationError('cannot combine all:true and count|hosts|label')
        self.label = label
        self.hosts = []  # type: List[HostPlacementSpec]
        if hosts:
            if all([isinstance(host, HostPlacementSpec) for host in hosts]):
                self.hosts = hosts
            else:
                self.hosts = [HostPlacementSpec.parse(x, require_network=False) for x in hosts if x]

        self.count = count  # type: Optional[int]
        self.all_hosts = all_hosts  # type: bool

        #: An fnmatch pattern to select hosts. Can also be a single host.
        self.host_pattern = host_pattern

    def is_empty(self):
        return not self.all_hosts and \
            self.label is None and \
            not self.hosts and \
            not self.host_pattern and \
            self.count is None

    def set_hosts(self, hosts):
        # To backpopulate the .hosts attribute when using labels or count
        # in the orchestrator backend.
        self.hosts = hosts

    def pattern_matches_hosts(self, all_hosts):
        # type: (List[str]) -> List[str]
        if not self.host_pattern:
            return []
        return fnmatch.filter(all_hosts, self.host_pattern)

    def pretty_str(self):
        kv = []
        if self.count:
            kv.append('count:%d' % self.count)
        if self.label:
            kv.append('label:%s' % self.label)
        if self.hosts:
            kv.append('%s' % ','.join([str(h) for h in self.hosts]))
        if self.all_hosts:
            kv.append('all:true')
        if self.host_pattern:
            kv.append('host_pattern:{}'.format(self.host_pattern))
        return ' '.join(kv)

    def __repr__(self):
        kv = []
        if self.count:
            kv.append('count=%d' % self.count)
        if self.label:
            kv.append('label=%s' % repr(self.label))
        if self.hosts:
            kv.append('hosts={!r}'.format(self.hosts))
        if self.all_hosts:
            kv.append('all_hosts=True')
        if self.host_pattern:
            kv.append('host_pattern={!r}'.format(self.host_pattern))
        return "PlacementSpec(%s)" % ', '.join(kv)

    @classmethod
    def from_json(cls, data):
        hosts = data.get('hosts', [])
        if hosts:
            data['hosts'] = [HostPlacementSpec.from_json(host) for host in hosts]
        _cls = cls(**data)
        _cls.validate()
        return _cls

    def to_json(self):
        return {
            'label': self.label,
            'hosts': [host.to_json() for host in self.hosts] if self.hosts else [],
            'count': self.count,
            'all_hosts': self.all_hosts,
            'host_pattern': self.host_pattern,
        }

    def validate(self):
        if self.hosts and self.label:
            # TODO: a less generic Exception
            raise ServiceSpecValidationError('Host and label are mutually exclusive')
        if self.count is not None and self.count <= 0:
            raise ServiceSpecValidationError("num/count must be > 1")
        if self.host_pattern and (self.hosts or self.label or self.all_hosts):
            raise ServiceSpecValidationError('Host pattern is mutually exclusive to everything else'
                                             '.')

    @classmethod
    def from_string(cls, arg):
        # type: (Optional[str]) -> PlacementSpec
        """
        A single integer is parsed as a count:
        >>> PlacementSpec.from_string('3')
        PlacementSpec(count=3)

        A list of names is parsed as host specifications:
        >>> PlacementSpec.from_string('host1 host2')
        PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacemen\
tSpec(hostname='host2', network='', name='')])

        You can also prefix the hosts with a count as follows:
        >>> PlacementSpec.from_string('2 host1 host2')
        PlacementSpec(count=2, hosts=[HostPlacementSpec(hostname='host1', network='', name=''), Hos\
tPlacementSpec(hostname='host2', network='', name='')])

        You can spefify labels using `label:<label>`
        >>> PlacementSpec.from_string('label:mon')
        PlacementSpec(label='mon')

        Labels als support a count:
        >>> PlacementSpec.from_string('3 label:mon')
        PlacementSpec(count=3, label='mon')

        fnmatch is also supported:
        >>> PlacementSpec.from_string('host_pattern:data[1-3]')
        PlacementSpec(host_pattern='data[1-3]')

        >>> PlacementSpec.from_string(None)
        PlacementSpec()
        """
        if arg is None:
            strings = []
        elif isinstance(arg, str):
            if ' ' in arg:
                strings = arg.split(' ')
            elif ';' in arg:
                strings = arg.split(';')
            elif ',' in arg and '[' not in arg:
                # FIXME: this isn't quite right.  we want to avoid breaking
                # a list of mons with addrvecs... so we're basically allowing
                # , most of the time, except when addrvecs are used.  maybe
                # ok?
                strings = arg.split(',')
            else:
                strings = [arg]
        else:
            raise ServiceSpecValidationError('invalid placement %s' % arg)

        count = None
        if strings:
            try:
                count = int(strings[0])
                strings = strings[1:]
            except ValueError:
                pass
        for s in strings:
            if s.startswith('count:'):
                try:
                    count = int(s[6:])
                    strings.remove(s)
                    break
                except ValueError:
                    pass

        all_hosts = False
        if '*' in strings:
            all_hosts = True
            strings.remove('*')
        if 'all:true' in strings:
            all_hosts = True
            strings.remove('all:true')

        hosts = [x for x in strings
                 if x != '*' and 'label:' not in x and not x.startswith('host_pattern:')]
        labels = [x[6:] for x in strings if 'label:' in x]
        if len(labels) > 1:
            raise ServiceSpecValidationError('more than one label provided: {}'.format(labels))
        host_patterns = [x[13:] for x in strings if x.startswith('host_pattern:')]
        if len(host_patterns) > 1:
            raise ServiceSpecValidationError('more than one host_patterns provided: {}'.format(
                host_patterns))

        ps = PlacementSpec(count=count,
                           hosts=hosts,
                           label=labels[0] if labels else None,
                           all_hosts=all_hosts,
                           host_pattern=host_patterns[0] if host_patterns else None)
        ps.validate()
        return ps


class ServiceSpec(object):
    """
    Details of service creation.

    Request to the orchestrator for a cluster of daemons
    such as MDS, RGW, iscsi gateway, MONs, MGRs, Prometheus

    This structure is supposed to be enough information to
    start the services.

    """

    def __init__(self,
                 service_type,  # type: str
                 service_id=None,  # type: Optional[str]
                 placement: Optional[PlacementSpec] = None,
                 count: Optional[int] = None):
        self.placement = PlacementSpec() if placement is None else placement  # type: PlacementSpec

        assert service_type
        self.service_type = service_type
        self.service_id = service_id

    @classmethod
    def from_json(cls, json_spec):
        # type: (dict) -> Any
        # Python 3:
        # >>> ServiceSpecs = TypeVar('Base', bound=ServiceSpec)
        # then, the real type is: (dict) -> ServiceSpecs
        """
        Initialize 'ServiceSpec' object data from a json structure
        :param json_spec: A valid dict with ServiceSpec
        """
        from ceph.deployment.drive_group import DriveGroupSpec

        service_type = json_spec.get('service_type', '')
        _cls = {
            'rgw': RGWSpec,
            'nfs': NFSServiceSpec,
            'osd': DriveGroupSpec
        }.get(service_type, cls)

        if _cls == ServiceSpec and not service_type:
            raise ServiceSpecValidationError('Spec needs a "service_type" key.')

        return _cls._from_json_impl(json_spec)  # type: ignore

    @classmethod
    def _from_json_impl(cls, json_spec):
        args = {}  # type: Dict[str, Dict[Any, Any]]
        for k, v in json_spec.items():
            if k == 'placement':
                v = PlacementSpec.from_json(v)
            if k == 'spec':
                args.update(v)
                continue
            args.update({k: v})
        return cls(**args)

    def service_name(self):
        n = self.service_type
        if self.service_id:
            n += '.' + self.service_id
        return n

    def to_json(self):
        # type: () -> Dict[str, Any]
        c = self.__dict__.copy()
        if self.placement:
            c['placement'] = self.placement.to_json()
        return c

    def validate(self):
        if not self.service_type:
            raise ServiceSpecValidationError('Cannot add Service: type required')

        if self.placement is not None:
            self.placement.validate()

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.__dict__)


def servicespec_validate_add(self: ServiceSpec):
    # This must not be a method of ServiceSpec, otherwise you'll hunt
    # sub-interpreter affinity bugs.
    ServiceSpec.validate(self)
    if self.service_type in ['mds', 'rgw', 'nfs'] and not self.service_id:
        raise ServiceSpecValidationError('Cannot add Service: id required')


class NFSServiceSpec(ServiceSpec):
    def __init__(self, service_id, pool=None, namespace=None, placement=None,
                 service_type='nfs'):
        assert service_type == 'nfs'
        super(NFSServiceSpec, self).__init__('nfs', service_id=service_id, placement=placement)

        #: RADOS pool where NFS client recovery data is stored.
        self.pool = pool

        #: RADOS namespace where NFS client recovery data is stored in the pool.
        self.namespace = namespace

    def validate_add(self):
        servicespec_validate_add(self)

        if not self.pool:
            raise ServiceSpecValidationError('Cannot add NFS: No Pool specified')


class RGWSpec(ServiceSpec):
    """
    Settings to configure a (multisite) Ceph RGW

    """
    def __init__(self,
                 rgw_realm=None,  # type: Optional[str]
                 rgw_zone=None,  # type: Optional[str]
                 service_id=None,  # type: Optional[str]
                 placement=None,
                 service_type='rgw',
                 rgw_frontend_port=None,  # type: Optional[int]
                 ):
        assert service_type == 'rgw'
        if service_id:
            (rgw_realm, rgw_zone) = service_id.split('.', 1)
        else:
            service_id = '%s.%s' % (rgw_realm, rgw_zone)
        super(RGWSpec, self).__init__('rgw', service_id=service_id, placement=placement)

        self.rgw_realm = rgw_realm
        self.rgw_zone = rgw_zone
        self.rgw_frontend_port = rgw_frontend_port
