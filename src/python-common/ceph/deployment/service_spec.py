import fnmatch
import os
import re
import enum
from collections import OrderedDict
from contextlib import contextmanager
from functools import wraps
from ipaddress import ip_network, ip_address, ip_interface
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)

import yaml

from ceph.deployment.hostspec import HostSpec, SpecValidationError, assert_valid_host
from ceph.deployment.utils import unwrap_ipv6, valid_addr
from ceph.utils import is_hex

ServiceSpecT = TypeVar('ServiceSpecT', bound='ServiceSpec')
FuncT = TypeVar('FuncT', bound=Callable)


def handle_type_error(method: FuncT) -> FuncT:
    @wraps(method)
    def inner(cls: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return method(cls, *args, **kwargs)
        except (TypeError, AttributeError) as e:
            error_msg = '{}: {}'.format(cls.__name__, e)
            raise SpecValidationError(error_msg)
    return cast(FuncT, inner)


class HostPlacementSpec(NamedTuple):
    hostname: str
    network: str
    name: str

    def __str__(self) -> str:
        res = ''
        res += self.hostname
        if self.network:
            res += ':' + self.network
        if self.name:
            res += '=' + self.name
        return res

    @classmethod
    @handle_type_error
    def from_json(cls, data: Union[dict, str]) -> 'HostPlacementSpec':
        if isinstance(data, str):
            return cls.parse(data)
        return cls(**data)

    def to_json(self) -> str:
        return str(self)

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

        networks = list()  # type: List[str]
        network = host_spec.network
        # in case we have [v2:1.2.3.4:3000,v1:1.2.3.4:6478]
        if ',' in network:
            networks = [x for x in network.split(',')]
        else:
            if network != '':
                networks.append(network)

        for network in networks:
            # only if we have versioned network configs
            if network.startswith('v') or network.startswith('[v'):
                # if this is ipv6 we can't just simply split on ':' so do
                # a split once and rsplit once to leave us with just ipv6 addr
                network = network.split(':', 1)[1]
                network = network.rsplit(':', 1)[0]
            try:
                # if subnets are defined, also verify the validity
                if '/' in network:
                    ip_network(network)
                else:
                    ip_address(unwrap_ipv6(network))
            except ValueError as e:
                # logging?
                raise e
        host_spec.validate()
        return host_spec

    def validate(self) -> None:
        assert_valid_host(self.hostname)


HostPatternType = Union[str, None, Dict[str, Union[str, bool, None]], "HostPattern"]


class PatternType(enum.Enum):
    fnmatch = 'fnmatch'
    regex = 'regex'


class HostPattern():
    def __init__(self,
                 pattern: Optional[str] = None,
                 pattern_type: PatternType = PatternType.fnmatch) -> None:
        self.pattern: Optional[str] = pattern
        self.pattern_type: PatternType = pattern_type
        self.compiled_regex = None
        if self.pattern_type == PatternType.regex and self.pattern:
            self.compiled_regex = re.compile(self.pattern)

    def filter_hosts(self, hosts: List[str]) -> List[str]:
        if not self.pattern:
            return []
        if not self.pattern_type or self.pattern_type == PatternType.fnmatch:
            return fnmatch.filter(hosts, self.pattern)
        elif self.pattern_type == PatternType.regex:
            if not self.compiled_regex:
                self.compiled_regex = re.compile(self.pattern)
            return [h for h in hosts if re.match(self.compiled_regex, h)]
        raise SpecValidationError(f'Got unexpected pattern_type: {self.pattern_type}')

    @classmethod
    def to_host_pattern(cls, arg: HostPatternType) -> "HostPattern":
        if arg is None:
            return cls()
        elif isinstance(arg, str):
            return cls(arg)
        elif isinstance(arg, cls):
            return arg
        elif isinstance(arg, dict):
            if 'pattern' not in arg:
                raise SpecValidationError("Got dict for host pattern "
                                          f"with no pattern field: {arg}")
            pattern = arg['pattern']
            if not pattern:
                raise SpecValidationError("Got dict for host pattern"
                                          f"with empty pattern: {arg}")
            assert isinstance(pattern, str)
            if 'pattern_type' in arg:
                pattern_type = arg['pattern_type']
                if not pattern_type or pattern_type == 'fnmatch':
                    return cls(pattern, pattern_type=PatternType.fnmatch)
                elif pattern_type == 'regex':
                    return cls(pattern, pattern_type=PatternType.regex)
                else:
                    raise SpecValidationError("Got dict for host pattern "
                                              f"with unknown pattern type: {arg}")
            return cls(pattern)
        raise SpecValidationError(f"Cannot convert {type(arg)} object to HostPattern")

    def __eq__(self, other: Any) -> bool:
        try:
            other_hp = self.to_host_pattern(other)
        except SpecValidationError:
            return False
        return self.pattern == other_hp.pattern and self.pattern_type == other_hp.pattern_type

    def pretty_str(self) -> str:
        # Placement specs must be able to be converted between the Python object
        # representation and a pretty str both ways. So we need a corresponding
        # function for HostPattern to convert it to a pretty str that we can
        # convert back later.
        res = self.pattern if self.pattern else ''
        if self.pattern_type == PatternType.regex:
            res = 'regex:' + res
        return res

    @classmethod
    def from_pretty_str(cls, val: str) -> "HostPattern":
        if 'regex:' in val:
            return cls(val[6:], pattern_type=PatternType.regex)
        else:
            return cls(val)

    def __repr__(self) -> str:
        return f'HostPattern(pattern=\'{self.pattern}\', pattern_type={str(self.pattern_type)})'

    def to_json(self) -> Union[str, Dict[str, Any], None]:
        if self.pattern_type and self.pattern_type != PatternType.fnmatch:
            return {
                'pattern': self.pattern,
                'pattern_type': self.pattern_type.name
            }
        return self.pattern

    @classmethod
    def from_json(self, val: Dict[str, Any]) -> "HostPattern":
        return self.to_host_pattern(val)

    def __bool__(self) -> bool:
        if self.pattern:
            return True
        return False


class PlacementSpec(object):
    """
    For APIs that need to specify a host subset
    """

    def __init__(self,
                 label: Optional[str] = None,
                 hosts: Union[List[str], List[HostPlacementSpec], None] = None,
                 count: Optional[int] = None,
                 count_per_host: Optional[int] = None,
                 host_pattern: HostPatternType = None,
                 ):
        # type: (...) -> None
        self.label = label
        self.hosts = []  # type: List[HostPlacementSpec]

        if hosts:
            self.set_hosts(hosts)

        self.count = count  # type: Optional[int]
        self.count_per_host = count_per_host   # type: Optional[int]

        #: fnmatch patterns to select hosts. Can also be a single host.
        self.host_pattern: HostPattern = HostPattern.to_host_pattern(host_pattern)

        self.validate()

    def is_empty(self) -> bool:
        return (
            self.label is None
            and not self.hosts
            and not self.host_pattern
            and self.count is None
            and self.count_per_host is None
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, PlacementSpec):
            return self.label == other.label \
                   and self.hosts == other.hosts \
                   and self.count == other.count \
                   and self.host_pattern == other.host_pattern \
                   and self.count_per_host == other.count_per_host
        return NotImplemented

    def set_hosts(self, hosts: Union[List[str], List[HostPlacementSpec]]) -> None:
        # To backpopulate the .hosts attribute when using labels or count
        # in the orchestrator backend.
        if all([isinstance(host, HostPlacementSpec) for host in hosts]):
            self.hosts = hosts  # type: ignore
        else:
            self.hosts = [HostPlacementSpec.parse(x, require_network=False)  # type: ignore
                          for x in hosts if x]

    # deprecated
    def filter_matching_hosts(self, _get_hosts_func: Callable) -> List[str]:
        return self.filter_matching_hostspecs(_get_hosts_func(as_hostspec=True))

    def filter_matching_hostspecs(self, hostspecs: Iterable[HostSpec]) -> List[str]:
        if self.hosts:
            all_hosts = [hs.hostname for hs in hostspecs]
            return [h.hostname for h in self.hosts if h.hostname in all_hosts]
        if self.label:
            all_hosts = [hs.hostname for hs in hostspecs if self.label in hs.labels]
        else:
            all_hosts = [hs.hostname for hs in hostspecs]
        if self.host_pattern:
            return self.host_pattern.filter_hosts(all_hosts)
        return all_hosts

    def get_target_count(self, hostspecs: Iterable[HostSpec]) -> int:
        if self.count:
            return self.count
        return len(self.filter_matching_hostspecs(hostspecs)) * (self.count_per_host or 1)

    def pretty_str(self) -> str:
        """
        >>> #doctest: +SKIP
        ... ps = PlacementSpec(...)  # For all placement specs:
        ... PlacementSpec.from_string(ps.pretty_str()) == ps
        """
        kv = []
        if self.hosts:
            kv.append(';'.join([str(h) for h in self.hosts]))
        if self.count:
            kv.append('count:%d' % self.count)
        if self.count_per_host:
            kv.append('count-per-host:%d' % self.count_per_host)
        if self.label:
            kv.append('label:%s' % self.label)
        if self.host_pattern:
            kv.append(self.host_pattern.pretty_str())
        return ';'.join(kv)

    def __repr__(self) -> str:
        kv = []
        if self.count:
            kv.append('count=%d' % self.count)
        if self.count_per_host:
            kv.append('count_per_host=%d' % self.count_per_host)
        if self.label:
            kv.append('label=%s' % repr(self.label))
        if self.hosts:
            kv.append('hosts={!r}'.format(self.hosts))
        if self.host_pattern:
            kv.append('host_pattern={!r}'.format(self.host_pattern))
        return "PlacementSpec(%s)" % ', '.join(kv)

    @classmethod
    @handle_type_error
    def from_json(cls, data: dict) -> 'PlacementSpec':
        c = data.copy()
        hosts = c.get('hosts', [])
        if hosts:
            c['hosts'] = []
            for host in hosts:
                c['hosts'].append(HostPlacementSpec.from_json(host))
        _cls = cls(**c)
        _cls.validate()
        return _cls

    def to_json(self) -> dict:
        r: Dict[str, Any] = {}
        if self.label:
            r['label'] = self.label
        if self.hosts:
            r['hosts'] = [host.to_json() for host in self.hosts]
        if self.count:
            r['count'] = self.count
        if self.count_per_host:
            r['count_per_host'] = self.count_per_host
        if self.host_pattern:
            r['host_pattern'] = self.host_pattern.to_json()
        return r

    def validate(self) -> None:
        if self.hosts and self.label:
            # TODO: a less generic Exception
            raise SpecValidationError('Host and label are mutually exclusive')
        if self.count is not None:
            try:
                intval = int(self.count)
            except (ValueError, TypeError):
                raise SpecValidationError("num/count must be a numeric value")
            if self.count != intval:
                raise SpecValidationError("num/count must be an integer value")
            if self.count < 1:
                raise SpecValidationError("num/count must be >= 1")
        if self.count_per_host is not None:
            try:
                intval = int(self.count_per_host)
            except (ValueError, TypeError):
                raise SpecValidationError("count-per-host must be a numeric value")
            if self.count_per_host != intval:
                raise SpecValidationError("count-per-host must be an integer value")
            if self.count_per_host < 1:
                raise SpecValidationError("count-per-host must be >= 1")
        if self.count_per_host is not None and not (
                self.label
                or self.hosts
                or self.host_pattern
        ):
            raise SpecValidationError(
                "count-per-host must be combined with label or hosts or host_pattern"
            )
        if self.count is not None and self.count_per_host is not None:
            raise SpecValidationError("cannot combine count and count-per-host")
        if (
                self.count_per_host is not None
                and self.hosts
                and any([hs.network or hs.name for hs in self.hosts])
        ):
            raise SpecValidationError(
                "count-per-host cannot be combined explicit placement with names or networks"
            )
        if self.host_pattern:
            # if we got an invalid type for the host_pattern, it would have
            # triggered a SpecValidationError when attemptying to convert it
            # to a HostPattern type, so no type checking is needed here.
            if self.hosts:
                raise SpecValidationError('cannot combine host patterns and hosts')

        for h in self.hosts:
            h.validate()

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

        You can specify labels using `label:<label>`

        >>> PlacementSpec.from_string('label:mon')
        PlacementSpec(label='mon')

        Labels also support a count:

        >>> PlacementSpec.from_string('3 label:mon')
        PlacementSpec(count=3, label='mon')

        You can specify a regex to match with `regex:<regex>`

        >>> PlacementSpec.from_string('regex:Foo[0-9]|Bar[0-9]')
        PlacementSpec(host_pattern=HostPattern(pattern='Foo[0-9]|Bar[0-9]', \
pattern_type=PatternType.regex))

        fnmatch is the default for a single string if "regex:" is not provided:

        >>> PlacementSpec.from_string('data[1-3]')
        PlacementSpec(host_pattern=HostPattern(pattern='data[1-3]', \
pattern_type=PatternType.fnmatch))

        >>> PlacementSpec.from_string(None)
        PlacementSpec()
        """
        if arg is None or not arg:
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
            raise SpecValidationError('invalid placement %s' % arg)

        count = None
        count_per_host = None
        if strings:
            try:
                count = int(strings[0])
                strings = strings[1:]
            except ValueError:
                pass
        for s in strings:
            if s.startswith('count:'):
                try:
                    count = int(s[len('count:'):])
                    strings.remove(s)
                    break
                except ValueError:
                    pass
        for s in strings:
            if s.startswith('count-per-host:'):
                try:
                    count_per_host = int(s[len('count-per-host:'):])
                    strings.remove(s)
                    break
                except ValueError:
                    pass

        advanced_hostspecs = [h for h in strings if
                              (':' in h or '=' in h or not any(c in '[]?*:=' for c in h)) and
                              'label:' not in h and
                              'regex:' not in h]
        for a_h in advanced_hostspecs:
            strings.remove(a_h)

        labels = [x for x in strings if 'label:' in x]
        if len(labels) > 1:
            raise SpecValidationError('more than one label provided: {}'.format(labels))
        for l in labels:
            strings.remove(l)
        label = labels[0][6:] if labels else None

        host_patterns = strings
        host_pattern: Optional[HostPattern] = None
        if len(host_patterns) > 1:
            raise SpecValidationError(
                'more than one host pattern provided: {}'.format(host_patterns))
        if host_patterns:
            # host_patterns is a list not > 1, and not empty, so we should
            # be guaranteed just a single string here
            host_pattern = HostPattern.from_pretty_str(host_patterns[0])

        ps = PlacementSpec(count=count,
                           count_per_host=count_per_host,
                           hosts=advanced_hostspecs,
                           label=label,
                           host_pattern=host_pattern)
        return ps


_service_spec_from_json_validate = True


class CustomConfig:
    """
    Class to specify custom config files to be mounted in daemon's container
    """

    _fields = ['content', 'mount_path']

    def __init__(self, content: str, mount_path: str) -> None:
        self.content: str = content
        self.mount_path: str = mount_path
        self.validate()

    def to_json(self) -> Dict[str, Any]:
        return {
            'content': self.content,
            'mount_path': self.mount_path,
        }

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "CustomConfig":
        for k in cls._fields:
            if k not in data:
                raise SpecValidationError(f'CustomConfig must have "{k}" field')
        for k in data.keys():
            if k not in cls._fields:
                raise SpecValidationError(f'CustomConfig got unknown field "{k}"')
        return cls(**data)

    @property
    def filename(self) -> str:
        return os.path.basename(self.mount_path)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, CustomConfig):
            return (
                self.content == other.content
                and self.mount_path == other.mount_path
            )
        return NotImplemented

    def __repr__(self) -> str:
        return f'CustomConfig({self.mount_path})'

    def validate(self) -> None:
        if not isinstance(self.content, str):
            raise SpecValidationError(
                    f'CustomConfig content must be a string. Got {type(self.content)}')
        if not isinstance(self.mount_path, str):
            raise SpecValidationError(
                    f'CustomConfig content must be a string. Got {type(self.mount_path)}')


@contextmanager
def service_spec_allow_invalid_from_json() -> Iterator[None]:
    """
    I know this is evil, but unfortunately `ceph orch ls`
    may return invalid OSD specs for OSDs not associated to
    and specs. If you have a better idea, please!
    """
    global _service_spec_from_json_validate
    _service_spec_from_json_validate = False
    yield
    _service_spec_from_json_validate = True


class ArgumentSpec:
    """The ArgumentSpec type represents an argument that can be
    passed to an underyling subsystem, like a container engine or
    another command line tool.

    The ArgumentSpec aims to be backwards compatible with the previous
    form of argument, a single string. The string was always assumed
    to be indentended to be split on spaces. For example:
    `--cpus 8` becomes `["--cpus", "8"]`. This type is converted from
    either a string or an json/yaml object. In the object form you
    can choose if the string part should be split so an argument like
    `--migrate-from=//192.168.5.22/My Documents` can be expressed.
    """
    _fields = ['argument', 'split']

    class OriginalType(enum.Enum):
        OBJECT = 0
        STRING = 1

    def __init__(
        self,
        argument: str,
        split: bool = False,
        *,
        origin: OriginalType = OriginalType.OBJECT,
    ) -> None:
        self.argument = argument
        self.split = bool(split)
        # origin helps with round-tripping between inputs that
        # are simple strings or objects (dicts)
        self._origin = origin
        self.validate()

    def to_json(self) -> Union[str, Dict[str, Any]]:
        """Return a json-safe represenation of the ArgumentSpec."""
        if self._origin == self.OriginalType.STRING:
            return self.argument
        return {
            'argument': self.argument,
            'split': self.split,
        }

    def to_args(self) -> List[str]:
        """Convert this ArgumentSpec into a list of arguments suitable for
        adding to an argv-style command line.
        """
        if not self.split:
            return [self.argument]
        return [part for part in self.argument.split(" ") if part]

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ArgumentSpec):
            return (
                self.argument == other.argument
                and self.split == other.split
            )
        if isinstance(other, object):
            # This is a workaround for silly ceph mgr object/type identity
            # mismatches due to multiple python interpreters in use.
            try:
                argument = getattr(other, 'argument')
                split = getattr(other, 'split')
                return (self.argument == argument and self.split == split)
            except AttributeError:
                pass
        return NotImplemented

    def __repr__(self) -> str:
        return f'ArgumentSpec({self.argument!r}, {self.split!r})'

    def validate(self) -> None:
        if not isinstance(self.argument, str):
            raise SpecValidationError(
                    f'ArgumentSpec argument must be a string. Got {type(self.argument)}')
        if not isinstance(self.split, bool):
            raise SpecValidationError(
                    f'ArgumentSpec split must be a boolean. Got {type(self.split)}')

    @classmethod
    def from_json(cls, data: Union[str, Dict[str, Any]]) -> "ArgumentSpec":
        """Convert a json-object (dict) to an ArgumentSpec."""
        if isinstance(data, str):
            return cls(data, split=True, origin=cls.OriginalType.STRING)
        if 'argument' not in data:
            raise SpecValidationError(f'ArgumentSpec must have an "argument" field')
        for k in data.keys():
            if k not in cls._fields:
                raise SpecValidationError(f'ArgumentSpec got an unknown field {k!r}')
        return cls(**data)

    @staticmethod
    def map_json(
        values: Optional["ArgumentList"]
    ) -> Optional[List[Union[str, Dict[str, Any]]]]:
        """Given a list of ArgumentSpec objects return a json-safe
        representation.of them."""
        if values is None:
            return None
        return [v.to_json() for v in values]

    @classmethod
    def from_general_args(cls, data: "GeneralArgList") -> "ArgumentList":
        """Convert a list of strs, dicts, or existing ArgumentSpec objects
        to a list of only ArgumentSpec objects.
        """
        out: ArgumentList = []
        for item in data:
            if isinstance(item, (str, dict)):
                out.append(cls.from_json(item))
            elif isinstance(item, cls):
                out.append(item)
            elif hasattr(item, 'to_json'):
                # This is a workaround for silly ceph mgr object/type identity
                # mismatches due to multiple python interpreters in use.
                # It should be safe because we already have to be able to
                # round-trip between json/yaml.
                out.append(cls.from_json(item.to_json()))
            else:
                raise SpecValidationError(f"Unknown type for argument: {type(item)}")
        return out


ArgumentList = List[ArgumentSpec]
GeneralArgList = List[Union[str, Dict[str, Any], "ArgumentSpec"]]


class ServiceSpec(object):
    """
    Details of service creation.

    Request to the orchestrator for a cluster of daemons
    such as MDS, RGW, iscsi gateway, nvmeof gateway, MONs, MGRs, Prometheus

    This structure is supposed to be enough information to
    start the services.
    """

    # list of all service type names that a ServiceSpec can be cast info
    KNOWN_SERVICE_TYPES = [
        'agent',
        'alertmanager',
        'ceph-exporter',
        'cephfs-mirror',
        'container',
        'crash',
        'elasticsearch',
        'grafana',
        'ingress',
        'mgmt-gateway',
        'oauth2-proxy',
        'iscsi',
        'jaeger-agent',
        'jaeger-collector',
        'jaeger-query',
        'jaeger-tracing',
        'loki',
        'mds',
        'mgr',
        'mon',
        'nfs',
        'node-exporter',
        'node-proxy',
        'nvmeof',
        'osd',
        'prometheus',
        'promtail',
        'rbd-mirror',
        'rgw',
        'smb',
        'snmp-gateway',
    ]

    # list of all service type names that require/get assigned a service_id value.
    # if a service is not listed here it *will not* be assigned a service_id even
    # if it is present in the JSON/YAML input
    REQUIRES_SERVICE_ID = [
        'container',
        'ingress',
        'iscsi',
        'mds',
        'nfs',
        'nvmeof',
        'rgw',
        'smb',
    ]

    MANAGED_CONFIG_OPTIONS = [
        'mds_join_fs',
    ]

    @classmethod
    def _cls(cls: Type[ServiceSpecT], service_type: str) -> Type[ServiceSpecT]:
        from ceph.deployment.drive_group import DriveGroupSpec

        ret = {
            'mon': MONSpec,
            'rgw': RGWSpec,
            'nfs': NFSServiceSpec,
            'osd': DriveGroupSpec,
            'mds': MDSSpec,
            'iscsi': IscsiServiceSpec,
            'nvmeof': NvmeofServiceSpec,
            'alertmanager': AlertManagerSpec,
            'ingress': IngressSpec,
            'mgmt-gateway': MgmtGatewaySpec,
            'oauth2-proxy': OAuth2ProxySpec,
            'container': CustomContainerSpec,
            'grafana': GrafanaSpec,
            'node-exporter': MonitoringSpec,
            'ceph-exporter': CephExporterSpec,
            'prometheus': PrometheusSpec,
            'loki': MonitoringSpec,
            'promtail': MonitoringSpec,
            'snmp-gateway': SNMPGatewaySpec,
            'elasticsearch': TracingSpec,
            'jaeger-agent': TracingSpec,
            'jaeger-collector': TracingSpec,
            'jaeger-query': TracingSpec,
            'jaeger-tracing': TracingSpec,
            SMBSpec.service_type: SMBSpec,
        }.get(service_type, cls)
        if ret == ServiceSpec and not service_type:
            raise SpecValidationError('Spec needs a "service_type" key.')
        return ret

    def __new__(cls: Type[ServiceSpecT], *args: Any, **kwargs: Any) -> ServiceSpecT:
        """
        Some Python foo to make sure, we don't have an object
        like `ServiceSpec('rgw')` of type `ServiceSpec`. Now we have:

        >>> type(ServiceSpec('rgw')) == type(RGWSpec('rgw'))
        True

        """
        if cls != ServiceSpec:
            return object.__new__(cls)
        service_type = kwargs.get('service_type', args[0] if args else None)
        sub_cls: Any = cls._cls(service_type)
        return object.__new__(sub_cls)

    def __init__(self,
                 service_type: str,
                 service_id: Optional[str] = None,
                 placement: Optional[PlacementSpec] = None,
                 count: Optional[int] = None,
                 config: Optional[Dict[str, str]] = None,
                 unmanaged: bool = False,
                 preview_only: bool = False,
                 networks: Optional[List[str]] = None,
                 targets: Optional[List[str]] = None,
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 custom_configs: Optional[List[CustomConfig]] = None,
                 ):

        #: See :ref:`orchestrator-cli-placement-spec`.
        self.placement = PlacementSpec() if placement is None else placement  # type: PlacementSpec

        assert service_type in ServiceSpec.KNOWN_SERVICE_TYPES, service_type
        #: The type of the service. Needs to be either a Ceph
        #: service (``mon``, ``crash``, ``mds``, ``mgr``, ``osd`` or
        #: ``rbd-mirror``), a gateway (``nfs`` or ``rgw``), part of the
        #: monitoring stack (``alertmanager``, ``grafana``, ``node-exporter`` or
        #: ``prometheus``) or (``container``) for custom containers.
        self.service_type = service_type

        #: The name of the service. Required for ``iscsi``, ``nvmeof``, ``mds``, ``nfs``, ``osd``,
        #: ``rgw``, ``container``, ``ingress``
        self.service_id = None

        if self.service_type in self.REQUIRES_SERVICE_ID or self.service_type == 'osd':
            self.service_id = service_id

        #: If set to ``true``, the orchestrator will not deploy nor remove
        #: any daemon associated with this service. Placement and all other properties
        #: will be ignored. This is useful, if you do not want this service to be
        #: managed temporarily. For cephadm, See :ref:`cephadm-spec-unmanaged`
        self.unmanaged = unmanaged
        self.preview_only = preview_only

        #: A list of network identities instructing the daemons to only bind
        #: on the particular networks in that list. In case the cluster is distributed
        #: across multiple networks, you can add multiple networks. See
        #: :ref:`cephadm-monitoring-networks-ports`,
        #: :ref:`cephadm-rgw-networks` and :ref:`cephadm-mgr-networks`.
        self.networks: List[str] = networks or []
        self.targets: List[str] = targets or []

        self.config: Optional[Dict[str, str]] = None
        if config:
            self.config = {k.replace(' ', '_'): v for k, v in config.items()}

        self.extra_container_args: Optional[ArgumentList] = None
        self.extra_entrypoint_args: Optional[ArgumentList] = None
        if extra_container_args:
            self.extra_container_args = ArgumentSpec.from_general_args(
                extra_container_args)
        if extra_entrypoint_args:
            self.extra_entrypoint_args = ArgumentSpec.from_general_args(
                extra_entrypoint_args)
        self.custom_configs: Optional[List[CustomConfig]] = custom_configs

    def __setattr__(self, name: str, value: Any) -> None:
        if value is not None and name in ('extra_container_args', 'extra_entrypoint_args'):
            for v in value:
                tname = str(type(v))
                if 'ArgumentSpec' not in tname:
                    raise TypeError(
                        f"{name} is not all ArgumentSpec values:"
                        f" {v!r}(is {type(v)} in {value!r}")

        super().__setattr__(name, value)

    @classmethod
    @handle_type_error
    def from_json(cls: Type[ServiceSpecT], json_spec: Dict) -> ServiceSpecT:
        """
        Initialize 'ServiceSpec' object data from a json structure

        There are two valid styles for service specs:

        the "old" style:

        .. code:: yaml

            service_type: nfs
            service_id: foo
            pool: mypool
            namespace: myns

        and the "new" style:

        .. code:: yaml

            service_type: nfs
            service_id: foo
            config:
              some_option: the_value
            networks: [10.10.0.0/16]
            spec:
              pool: mypool
              namespace: myns

        In https://tracker.ceph.com/issues/45321 we decided that we'd like to
        prefer the new style as it is more readable and provides a better
        understanding of what fields are special for a give service type.

        Note, we'll need to stay compatible with both versions for the
        the next two major releases (octopus, pacific).

        :param json_spec: A valid dict with ServiceSpec

        :meta private:
        """
        if not isinstance(json_spec, dict):
            raise SpecValidationError(
                f'Service Spec is not an (JSON or YAML) object. got "{str(json_spec)}"')

        json_spec = cls.normalize_json(json_spec)

        c = json_spec.copy()

        # kludge to make `from_json` compatible to `Orchestrator.describe_service`
        # Open question: Remove `service_id` form to_json?
        if c.get('service_name', ''):
            service_type_id = c['service_name'].split('.', 1)

            if not c.get('service_type', ''):
                c['service_type'] = service_type_id[0]
            if not c.get('service_id', '') and len(service_type_id) > 1:
                c['service_id'] = service_type_id[1]
            del c['service_name']

        service_type = c.get('service_type', '')
        _cls = cls._cls(service_type)

        if 'status' in c:
            del c['status']  # kludge to make us compatible to `ServiceDescription.to_json()`

        return _cls._from_json_impl(c)  # type: ignore

    @staticmethod
    def normalize_json(json_spec: dict) -> dict:
        networks = json_spec.get('networks')
        if networks is None:
            return json_spec
        if isinstance(networks, list):
            return json_spec
        if not isinstance(networks, str):
            raise SpecValidationError(f'Networks ({networks}) must be a string or list of strings')
        json_spec['networks'] = [networks]
        return json_spec

    @classmethod
    def _from_json_impl(cls: Type[ServiceSpecT], json_spec: dict) -> ServiceSpecT:
        args = {}  # type: Dict[str, Any]
        for k, v in json_spec.items():
            if k == 'placement':
                v = PlacementSpec.from_json(v)
            if k == 'custom_configs':
                v = [CustomConfig.from_json(c) for c in v]
            if k == 'spec':
                args.update(v)
                continue
            args.update({k: v})
        _cls = cls(**args)
        if _service_spec_from_json_validate:
            _cls.validate()
        return _cls

    def service_name(self) -> str:
        n = self.service_type
        if self.service_id:
            n += '.' + self.service_id
        return n

    def get_port_start(self) -> List[int]:
        # If defined, we will allocate and number ports starting at this
        # point.
        return []

    def get_virtual_ip(self) -> Optional[str]:
        return None

    def to_json(self):
        # type: () -> OrderedDict[str, Any]
        ret: OrderedDict[str, Any] = OrderedDict()
        ret['service_type'] = self.service_type
        if self.service_id:
            ret['service_id'] = self.service_id
        ret['service_name'] = self.service_name()
        if self.placement.to_json():
            ret['placement'] = self.placement.to_json()
        if self.unmanaged:
            ret['unmanaged'] = self.unmanaged
        if self.networks:
            ret['networks'] = self.networks
        if self.extra_container_args:
            ret['extra_container_args'] = ArgumentSpec.map_json(
                self.extra_container_args
            )
        if self.extra_entrypoint_args:
            ret['extra_entrypoint_args'] = ArgumentSpec.map_json(
                self.extra_entrypoint_args
            )
        if self.custom_configs:
            ret['custom_configs'] = [c.to_json() for c in self.custom_configs]

        c = {}
        for key, val in sorted(self.__dict__.items(), key=lambda tpl: tpl[0]):
            if key in ret:
                continue
            if hasattr(val, 'to_json'):
                val = val.to_json()
            if val:
                c[key] = val
        if c:
            ret['spec'] = c
        return ret

    def validate(self) -> None:
        if not self.service_type:
            raise SpecValidationError('Cannot add Service: type required')

        if self.service_type != 'osd':
            if self.service_type in self.REQUIRES_SERVICE_ID and not self.service_id:
                raise SpecValidationError('Cannot add Service: id required')
            if self.service_type not in self.REQUIRES_SERVICE_ID and self.service_id:
                raise SpecValidationError(
                        f'Service of type \'{self.service_type}\' should not contain a service id')

        if self.service_id:
            if not re.match('^[a-zA-Z0-9_.-]+$', str(self.service_id)):
                raise SpecValidationError('Service id contains invalid characters, '
                                          'only [a-zA-Z0-9_.-] allowed')

        if self.placement is not None:
            self.placement.validate()
        if self.config:
            for k, v in self.config.items():
                if k in self.MANAGED_CONFIG_OPTIONS:
                    raise SpecValidationError(
                        f'Cannot set config option {k} in spec: it is managed by cephadm'
                    )
        for network in self.networks or []:
            try:
                ip_network(network)
            except ValueError as e:
                raise SpecValidationError(
                    f'Cannot parse network {network}: {e}'
                )

    def __repr__(self) -> str:
        y = yaml.dump(cast(dict, self), default_flow_style=False)
        return f"{self.__class__.__name__}.from_json(yaml.safe_load('''{y}'''))"

    def __eq__(self, other: Any) -> bool:
        return (self.__class__ == other.__class__
                and
                self.__dict__ == other.__dict__)

    def one_line_str(self) -> str:
        return '<{} for service_name={}>'.format(self.__class__.__name__, self.service_name())

    @staticmethod
    def yaml_representer(dumper: 'yaml.SafeDumper', data: 'ServiceSpec') -> Any:
        return dumper.represent_dict(cast(Mapping, data.to_json().items()))


yaml.add_representer(ServiceSpec, ServiceSpec.yaml_representer)


class NFSServiceSpec(ServiceSpec):
    def __init__(self,
                 service_type: str = 'nfs',
                 service_id: Optional[str] = None,
                 placement: Optional[PlacementSpec] = None,
                 unmanaged: bool = False,
                 preview_only: bool = False,
                 config: Optional[Dict[str, str]] = None,
                 networks: Optional[List[str]] = None,
                 port: Optional[int] = None,
                 virtual_ip: Optional[str] = None,
                 enable_nlm: bool = False,
                 enable_haproxy_protocol: bool = False,
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 idmap_conf: Optional[Dict[str, Dict[str, str]]] = None,
                 custom_configs: Optional[List[CustomConfig]] = None,
                 ):
        assert service_type == 'nfs'
        super(NFSServiceSpec, self).__init__(
            'nfs', service_id=service_id,
            placement=placement, unmanaged=unmanaged, preview_only=preview_only,
            config=config, networks=networks, extra_container_args=extra_container_args,
            extra_entrypoint_args=extra_entrypoint_args, custom_configs=custom_configs)

        self.port = port
        self.virtual_ip = virtual_ip
        self.enable_haproxy_protocol = enable_haproxy_protocol
        self.idmap_conf = idmap_conf
        self.enable_nlm = enable_nlm

    def get_port_start(self) -> List[int]:
        if self.port:
            return [self.port]
        return []

    def rados_config_name(self):
        # type: () -> str
        return 'conf-' + self.service_name()


yaml.add_representer(NFSServiceSpec, ServiceSpec.yaml_representer)


class RGWSpec(ServiceSpec):
    """
    Settings to configure a (multisite) Ceph RGW

    .. code-block:: yaml

        service_type: rgw
        service_id: myrealm.myzone
        spec:
            rgw_realm: myrealm
            rgw_zonegroup: myzonegroup
            rgw_zone: myzone
            ssl: true
            rgw_frontend_port: 1234
            rgw_frontend_type: beast
            rgw_frontend_ssl_certificate: ...

    See also: :ref:`orchestrator-cli-service-spec`
    """

    MANAGED_CONFIG_OPTIONS = ServiceSpec.MANAGED_CONFIG_OPTIONS + [
        'rgw_zone',
        'rgw_realm',
        'rgw_zonegroup',
        'rgw_frontends',
    ]

    def __init__(self,
                 service_type: str = 'rgw',
                 service_id: Optional[str] = None,
                 placement: Optional[PlacementSpec] = None,
                 rgw_realm: Optional[str] = None,
                 rgw_zonegroup: Optional[str] = None,
                 rgw_zone: Optional[str] = None,
                 rgw_frontend_port: Optional[int] = None,
                 rgw_frontend_ssl_certificate: Optional[Union[str, List[str]]] = None,
                 rgw_frontend_type: Optional[str] = None,
                 rgw_frontend_extra_args: Optional[List[str]] = None,
                 unmanaged: bool = False,
                 ssl: bool = False,
                 preview_only: bool = False,
                 config: Optional[Dict[str, str]] = None,
                 networks: Optional[List[str]] = None,
                 subcluster: Optional[str] = None,  # legacy, only for from_json on upgrade
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 custom_configs: Optional[List[CustomConfig]] = None,
                 rgw_realm_token: Optional[str] = None,
                 update_endpoints: Optional[bool] = False,
                 zone_endpoints: Optional[str] = None,  # comma separated endpoints list
                 zonegroup_hostnames: Optional[List[str]] = None,
                 rgw_user_counters_cache: Optional[bool] = False,
                 rgw_user_counters_cache_size: Optional[int] = None,
                 rgw_bucket_counters_cache: Optional[bool] = False,
                 rgw_bucket_counters_cache_size: Optional[int] = None,
                 generate_cert: bool = False,
                 ):
        assert service_type == 'rgw', service_type

        # for backward compatibility with octopus spec files,
        if not service_id and (rgw_realm and rgw_zone):
            service_id = rgw_realm + '.' + rgw_zone

        super(RGWSpec, self).__init__(
            'rgw', service_id=service_id,
            placement=placement, unmanaged=unmanaged,
            preview_only=preview_only, config=config, networks=networks,
            extra_container_args=extra_container_args, extra_entrypoint_args=extra_entrypoint_args,
            custom_configs=custom_configs)

        #: The RGW realm associated with this service. Needs to be manually created
        #: if the spec is being applied directly to cephdam. In case of rgw module
        #: the realm is created automatically.
        self.rgw_realm: Optional[str] = rgw_realm
        #: The RGW zonegroup associated with this service. Needs to be manually created
        #: if the spec is being applied directly to cephdam. In case of rgw module
        #: the zonegroup is created automatically.
        self.rgw_zonegroup: Optional[str] = rgw_zonegroup
        #: The RGW zone associated with this service. Needs to be manually created
        #: if the spec is being applied directly to cephdam. In case of rgw module
        #: the zone is created automatically.
        self.rgw_zone: Optional[str] = rgw_zone
        #: Port of the RGW daemons
        self.rgw_frontend_port: Optional[int] = rgw_frontend_port
        #: List of SSL certificates
        self.rgw_frontend_ssl_certificate: Optional[Union[str, List[str]]] \
            = rgw_frontend_ssl_certificate
        #: civetweb or beast (default: beast). See :ref:`rgw_frontends`
        self.rgw_frontend_type: Optional[str] = rgw_frontend_type
        #: List of extra arguments for rgw_frontend in the form opt=value. See :ref:`rgw_frontends`
        self.rgw_frontend_extra_args: Optional[List[str]] = rgw_frontend_extra_args
        #: enable SSL
        self.ssl = ssl
        self.rgw_realm_token = rgw_realm_token
        self.update_endpoints = update_endpoints
        self.zone_endpoints = zone_endpoints
        self.zonegroup_hostnames = zonegroup_hostnames

        #: To track op metrics by user config value rgw_user_counters_cache must be set to true
        self.rgw_user_counters_cache = rgw_user_counters_cache
        #: Used to set number of entries in each cache of user counters
        self.rgw_user_counters_cache_size = rgw_user_counters_cache_size
        #: To track op metrics by bucket config value rgw_bucket_counters_cache must be set to true
        self.rgw_bucket_counters_cache = rgw_bucket_counters_cache
        #: Used to set number of entries in each cache of bucket counters
        self.rgw_bucket_counters_cache_size = rgw_bucket_counters_cache_size
        #: Whether we should generate a cert/key for the user if not provided
        self.generate_cert = generate_cert

    def get_port_start(self) -> List[int]:
        return [self.get_port()]

    def get_port(self) -> int:
        if self.rgw_frontend_port:
            return self.rgw_frontend_port
        if self.ssl:
            return 443
        else:
            return 80

    def validate(self) -> None:
        super(RGWSpec, self).validate()

        if self.rgw_realm and not self.rgw_zone:
            raise SpecValidationError(
                    'Cannot add RGW: Realm specified but no zone specified')
        if self.rgw_zone and not self.rgw_realm:
            raise SpecValidationError('Cannot add RGW: Zone specified but no realm specified')

        if self.rgw_frontend_type is not None:
            if self.rgw_frontend_type not in ['beast', 'civetweb']:
                raise SpecValidationError(
                    'Invalid rgw_frontend_type value. Valid values are: beast, civetweb.\n'
                    'Additional rgw type parameters can be passed using rgw_frontend_extra_args.'
                )

        if self.generate_cert and not self.ssl:
            raise SpecValidationError('"ssl" field must be set to true when "generate_cert" '
                                      'is set to true')


yaml.add_representer(RGWSpec, ServiceSpec.yaml_representer)


class NvmeofServiceSpec(ServiceSpec):
    def __init__(self,
                 service_type: str = 'nvmeof',
                 service_id: Optional[str] = None,
                 name: Optional[str] = None,
                 group: Optional[str] = None,
                 addr: Optional[str] = None,
                 port: Optional[int] = None,
                 pool: Optional[str] = None,
                 enable_auth: bool = False,
                 state_update_notify: Optional[bool] = True,
                 state_update_interval_sec: Optional[int] = 5,
                 enable_spdk_discovery_controller: Optional[bool] = False,
                 omap_file_lock_duration: Optional[int] = 20,
                 omap_file_lock_retries: Optional[int] = 30,
                 omap_file_lock_retry_sleep_interval: Optional[float] = 1.0,
                 omap_file_update_reloads: Optional[int] = 10,
                 enable_prometheus_exporter: Optional[bool] = True,
                 bdevs_per_cluster: Optional[int] = 32,
                 verify_nqns: Optional[bool] = True,
                 allowed_consecutive_spdk_ping_failures: Optional[int] = 1,
                 spdk_ping_interval_in_seconds: Optional[float] = 2.0,
                 ping_spdk_under_lock: Optional[bool] = False,
                 server_key: Optional[str] = None,
                 server_cert: Optional[str] = None,
                 client_key: Optional[str] = None,
                 client_cert: Optional[str] = None,
                 root_ca_cert: Optional[str] = None,
                 spdk_path: Optional[str] = None,
                 tgt_path: Optional[str] = None,
                 spdk_timeout: Optional[float] = 60.0,
                 spdk_log_level: Optional[str] = 'WARNING',
                 rpc_socket_dir: Optional[str] = '/var/tmp/',
                 rpc_socket_name: Optional[str] = 'spdk.sock',
                 conn_retries: Optional[int] = 10,
                 transports: Optional[str] = 'tcp',
                 transport_tcp_options: Optional[Dict[str, int]] =
                 {"in_capsule_data_size": 8192, "max_io_qpairs_per_ctrlr": 7},
                 tgt_cmd_extra_args: Optional[str] = None,
                 discovery_addr: Optional[str] = None,
                 discovery_port: Optional[int] = None,
                 log_level: Optional[str] = 'INFO',
                 log_files_enabled: Optional[bool] = True,
                 log_files_rotation_enabled: Optional[bool] = True,
                 verbose_log_messages: Optional[bool] = True,
                 max_log_file_size_in_mb: Optional[int] = 10,
                 max_log_files_count: Optional[int] = 20,
                 max_log_directory_backups: Optional[int] = 10,
                 log_directory: Optional[str] = '/var/log/ceph/',
                 monitor_timeout: Optional[float] = 1.0,
                 enable_monitor_client: bool = True,
                 placement: Optional[PlacementSpec] = None,
                 unmanaged: bool = False,
                 preview_only: bool = False,
                 config: Optional[Dict[str, str]] = None,
                 networks: Optional[List[str]] = None,
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 custom_configs: Optional[List[CustomConfig]] = None,
                 ):
        assert service_type == 'nvmeof'
        super(NvmeofServiceSpec, self).__init__('nvmeof', service_id=service_id,
                                                placement=placement, unmanaged=unmanaged,
                                                preview_only=preview_only,
                                                config=config, networks=networks,
                                                extra_container_args=extra_container_args,
                                                extra_entrypoint_args=extra_entrypoint_args,
                                                custom_configs=custom_configs)

        #: RADOS pool where ceph-nvmeof config data is stored.
        self.pool = pool
        #: ``addr`` address of the nvmeof gateway
        self.addr = addr
        #: ``port`` port of the nvmeof gateway
        self.port = port or 5500
        #: ``name`` name of the nvmeof gateway
        self.name = name
        #: ``group`` name of the nvmeof gateway
        self.group = group or ''
        #: ``enable_auth`` enables user authentication on nvmeof gateway
        self.enable_auth = enable_auth
        #: ``state_update_notify`` enables automatic update from OMAP in nvmeof gateway
        self.state_update_notify = state_update_notify
        #: ``state_update_interval_sec`` number of seconds to check for updates in OMAP
        self.state_update_interval_sec = state_update_interval_sec
        #: ``enable_spdk_discovery_controller`` SPDK or ceph-nvmeof discovery service
        self.enable_spdk_discovery_controller = enable_spdk_discovery_controller
        #: ``enable_prometheus_exporter`` enables Prometheus exporter
        self.enable_prometheus_exporter = enable_prometheus_exporter
        #: ``verify_nqns`` enables verification of subsystem and host NQNs for validity
        self.verify_nqns = verify_nqns
        #: ``omap_file_lock_duration`` number of seconds before automatically unlock OMAP file lock
        self.omap_file_lock_duration = omap_file_lock_duration
        #: ``omap_file_lock_retries`` number of retries to lock OMAP file before giving up
        self.omap_file_lock_retries = omap_file_lock_retries
        #: ``omap_file_lock_retry_sleep_interval`` seconds to wait before retrying to lock OMAP
        self.omap_file_lock_retry_sleep_interval = omap_file_lock_retry_sleep_interval
        #: ``omap_file_update_reloads`` number of attempt to reload OMAP when it differs from local
        self.omap_file_update_reloads = omap_file_update_reloads
        #: ``allowed_consecutive_spdk_ping_failures`` # of ping failures before aborting gateway
        self.allowed_consecutive_spdk_ping_failures = allowed_consecutive_spdk_ping_failures
        #: ``spdk_ping_interval_in_seconds`` sleep interval in seconds between SPDK pings
        self.spdk_ping_interval_in_seconds = spdk_ping_interval_in_seconds
        #: ``ping_spdk_under_lock`` whether or not we should perform SPDK ping under the RPC lock
        self.ping_spdk_under_lock = ping_spdk_under_lock
        #: ``bdevs_per_cluster`` number of bdevs per cluster
        self.bdevs_per_cluster = bdevs_per_cluster
        #: ``server_key`` gateway server key
        self.server_key = server_key
        #: ``server_cert`` gateway server certificate
        self.server_cert = server_cert
        #: ``client_key`` client key
        self.client_key = client_key
        #: ``client_cert`` client certificate
        self.client_cert = client_cert
        #: ``root_ca_cert`` CA cert for server/client certs
        self.root_ca_cert = root_ca_cert
        #: ``spdk_path`` path to SPDK
        self.spdk_path = spdk_path or '/usr/local/bin/nvmf_tgt'
        #: ``tgt_path`` nvmeof target path
        self.tgt_path = tgt_path or '/usr/local/bin/nvmf_tgt'
        #: ``spdk_timeout`` SPDK connectivity timeout
        self.spdk_timeout = spdk_timeout
        #: ``spdk_log_level`` the SPDK log level
        self.spdk_log_level = spdk_log_level or 'WARNING'
        #: ``rpc_socket_dir`` the SPDK RPC socket file directory
        self.rpc_socket_dir = rpc_socket_dir or '/var/tmp/'
        #: ``rpc_socket_name`` the SPDK RPC socket file name
        self.rpc_socket_name = rpc_socket_name or 'spdk.sock'
        #: ``conn_retries`` ceph connection retries number
        self.conn_retries = conn_retries
        #: ``transports`` tcp
        self.transports = transports
        #: List of extra arguments for transports in the form opt=value
        self.transport_tcp_options: Optional[Dict[str, int]] = transport_tcp_options
        #: ``tgt_cmd_extra_args`` extra arguments for the nvmf_tgt process
        self.tgt_cmd_extra_args = tgt_cmd_extra_args
        #: ``discovery_addr`` address of the discovery service
        self.discovery_addr = discovery_addr
        #: ``discovery_port`` port of the discovery service
        self.discovery_port = discovery_port or 8009
        #: ``log_level`` the nvmeof gateway log level
        self.log_level = log_level or 'INFO'
        #: ``log_files_enabled`` enables the usage of files to keep the nameof gateway log
        self.log_files_enabled = log_files_enabled
        #: ``log_files_rotation_enabled`` enables rotation of log files when pass the size limit
        self.log_files_rotation_enabled = log_files_rotation_enabled
        #: ``verbose_log_messages`` add more details to the nvmeof gateway log message
        self.verbose_log_messages = verbose_log_messages
        #: ``max_log_file_size_in_mb`` max size in MB before starting a new log file
        self.max_log_file_size_in_mb = max_log_file_size_in_mb
        #: ``max_log_files_count`` max log files to keep before overriding them
        self.max_log_files_count = max_log_files_count
        #: ``max_log_directory_backups`` max directories for old gateways with same name to keep
        self.max_log_directory_backups = max_log_directory_backups
        #: ``log_directory`` directory for keeping nameof gateway log files
        self.log_directory = log_directory or '/var/log/ceph/'
        #: ``monitor_timeout`` monitor connectivity timeout
        self.monitor_timeout = monitor_timeout
        #: ``enable_monitor_client`` whether to connect to the ceph monitor or not
        self.enable_monitor_client = enable_monitor_client

    def get_port_start(self) -> List[int]:
        return [5500, 4420, 8009]

    def validate(self) -> None:
        #  TODO: what other parameters should be validated as part of this function?
        super(NvmeofServiceSpec, self).validate()

        if not self.pool:
            raise SpecValidationError('Cannot add NVMEOF: No Pool specified')

        if self.enable_auth:
            if not all([self.server_key, self.server_cert, self.client_key,
                        self.client_cert, self.root_ca_cert]):
                err_msg = 'enable_auth is true but '
                for cert_key_attr in ['server_key', 'server_cert', 'client_key',
                                      'client_cert', 'root_ca_cert']:
                    if not hasattr(self, cert_key_attr):
                        err_msg += f'{cert_key_attr}, '
                err_msg += 'attribute(s) not set in the spec'
                raise SpecValidationError(err_msg)

        if self.transports not in ['tcp']:
            raise SpecValidationError('Invalid transport. Valid values are tcp')

        if self.log_level:
            if self.log_level.lower() not in ['debug',
                                              'info',
                                              'warning',
                                              'error',
                                              'critical']:
                raise SpecValidationError(
                    'Invalid log level. Valid values are: debug, info, warning, error, critial')

        if self.spdk_log_level:
            if self.spdk_log_level.lower() not in ['debug',
                                                   'info',
                                                   'warning',
                                                   'error',
                                                   'notice']:
                raise SpecValidationError(
                    'Invalid SPDK log level. Valid values are: '
                    'DEBUG, INFO, WARNING, ERROR, NOTICE')

        if (
            self.spdk_ping_interval_in_seconds
            and self.spdk_ping_interval_in_seconds < 1.0
        ):
            raise SpecValidationError("SPDK ping interval should be at least 1 second")

        if (
            self.allowed_consecutive_spdk_ping_failures
            and self.allowed_consecutive_spdk_ping_failures < 1
        ):
            raise SpecValidationError("Allowed consecutive SPDK ping failures should be at least 1")

        if (
            self.state_update_interval_sec
            and self.state_update_interval_sec < 0
        ):
            raise SpecValidationError("State update interval can't be negative")

        if (
            self.omap_file_lock_duration
            and self.omap_file_lock_duration < 0
        ):
            raise SpecValidationError("OMAP file lock duration can't be negative")

        if (
            self.omap_file_lock_retries
            and self.omap_file_lock_retries < 0
        ):
            raise SpecValidationError("OMAP file lock retries can't be negative")

        if (
            self.omap_file_update_reloads
            and self.omap_file_update_reloads < 0
        ):
            raise SpecValidationError("OMAP file reloads can't be negative")

        if (
            self.spdk_timeout
            and self.spdk_timeout < 0.0
        ):
            raise SpecValidationError("SPDK timeout can't be negative")

        if (
            self.conn_retries
            and self.conn_retries < 0
        ):
            raise SpecValidationError("Connection retries can't be negative")

        if (
            self.max_log_file_size_in_mb
            and self.max_log_file_size_in_mb < 0
        ):
            raise SpecValidationError("Log file size can't be negative")

        if (
            self.max_log_files_count
            and self.max_log_files_count < 0
        ):
            raise SpecValidationError("Log files count can't be negative")

        if (
            self.max_log_directory_backups
            and self.max_log_directory_backups < 0
        ):
            raise SpecValidationError("Log file directory backups can't be negative")

        if (
            self.monitor_timeout
            and self.monitor_timeout < 0.0
        ):
            raise SpecValidationError("Monitor timeout can't be negative")

        if self.port and self.port < 0:
            raise SpecValidationError("Port can't be negative")

        if self.discovery_port and self.discovery_port < 0:
            raise SpecValidationError("Discovery port can't be negative")


yaml.add_representer(NvmeofServiceSpec, ServiceSpec.yaml_representer)


class IscsiServiceSpec(ServiceSpec):
    def __init__(self,
                 service_type: str = 'iscsi',
                 service_id: Optional[str] = None,
                 pool: Optional[str] = None,
                 trusted_ip_list: Optional[str] = None,
                 api_port: Optional[int] = 5000,
                 api_user: Optional[str] = 'admin',
                 api_password: Optional[str] = 'admin',
                 api_secure: Optional[bool] = None,
                 ssl_cert: Optional[str] = None,
                 ssl_key: Optional[str] = None,
                 placement: Optional[PlacementSpec] = None,
                 unmanaged: bool = False,
                 preview_only: bool = False,
                 config: Optional[Dict[str, str]] = None,
                 networks: Optional[List[str]] = None,
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 custom_configs: Optional[List[CustomConfig]] = None,
                 ):
        assert service_type == 'iscsi'
        super(IscsiServiceSpec, self).__init__('iscsi', service_id=service_id,
                                               placement=placement, unmanaged=unmanaged,
                                               preview_only=preview_only,
                                               config=config, networks=networks,
                                               extra_container_args=extra_container_args,
                                               extra_entrypoint_args=extra_entrypoint_args,
                                               custom_configs=custom_configs)

        #: RADOS pool where ceph-iscsi config data is stored.
        self.pool = pool
        #: list of trusted IP addresses
        self.trusted_ip_list = trusted_ip_list
        #: ``api_port`` as defined in the ``iscsi-gateway.cfg``
        self.api_port = api_port
        #: ``api_user`` as defined in the ``iscsi-gateway.cfg``
        self.api_user = api_user
        #: ``api_password`` as defined in the ``iscsi-gateway.cfg``
        self.api_password = api_password
        #: ``api_secure`` as defined in the ``iscsi-gateway.cfg``
        self.api_secure = api_secure
        #: SSL certificate
        self.ssl_cert = ssl_cert
        #: SSL private key
        self.ssl_key = ssl_key

        if not self.api_secure and self.ssl_cert and self.ssl_key:
            self.api_secure = True

    def get_port_start(self) -> List[int]:
        return [self.api_port or 5000]

    def validate(self) -> None:
        super(IscsiServiceSpec, self).validate()

        if not self.pool:
            raise SpecValidationError(
                'Cannot add ISCSI: No Pool specified')

        # Do not need to check for api_user and api_password as they
        # now default to 'admin' when setting up the gateway url. Older
        # iSCSI specs from before this change should be fine as they will
        # have been required to have an api_user and api_password set and
        # will be unaffected by the new default value.


yaml.add_representer(IscsiServiceSpec, ServiceSpec.yaml_representer)


class IngressSpec(ServiceSpec):
    def __init__(self,
                 service_type: str = 'ingress',
                 service_id: Optional[str] = None,
                 config: Optional[Dict[str, str]] = None,
                 networks: Optional[List[str]] = None,
                 placement: Optional[PlacementSpec] = None,
                 backend_service: Optional[str] = None,
                 frontend_port: Optional[int] = None,
                 ssl_cert: Optional[str] = None,
                 ssl_key: Optional[str] = None,
                 ssl_dh_param: Optional[str] = None,
                 ssl_ciphers: Optional[List[str]] = None,
                 ssl_options: Optional[List[str]] = None,
                 monitor_port: Optional[int] = None,
                 monitor_user: Optional[str] = None,
                 monitor_password: Optional[str] = None,
                 enable_stats: Optional[bool] = None,
                 keepalived_password: Optional[str] = None,
                 virtual_ip: Optional[str] = None,
                 virtual_ips_list: Optional[List[str]] = None,
                 virtual_interface_networks: Optional[List[str]] = [],
                 use_keepalived_multicast: Optional[bool] = False,
                 vrrp_interface_network: Optional[str] = None,
                 first_virtual_router_id: Optional[int] = 50,
                 unmanaged: bool = False,
                 ssl: bool = False,
                 keepalive_only: bool = False,
                 enable_haproxy_protocol: bool = False,
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 custom_configs: Optional[List[CustomConfig]] = None,
                 health_check_interval: Optional[str] = None,
                 ):
        assert service_type == 'ingress'

        super(IngressSpec, self).__init__(
            'ingress', service_id=service_id,
            placement=placement, config=config,
            networks=networks,
            extra_container_args=extra_container_args,
            extra_entrypoint_args=extra_entrypoint_args,
            custom_configs=custom_configs
        )
        self.backend_service = backend_service
        self.frontend_port = frontend_port
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key
        self.ssl_dh_param = ssl_dh_param
        self.ssl_ciphers = ssl_ciphers
        self.ssl_options = ssl_options
        self.monitor_port = monitor_port
        self.monitor_user = monitor_user
        self.monitor_password = monitor_password
        self.keepalived_password = keepalived_password
        self.virtual_ip = virtual_ip
        self.virtual_ips_list = virtual_ips_list
        self.virtual_interface_networks = virtual_interface_networks or []
        self.use_keepalived_multicast = use_keepalived_multicast
        self.vrrp_interface_network = vrrp_interface_network
        self.first_virtual_router_id = first_virtual_router_id
        self.unmanaged = unmanaged
        self.ssl = ssl
        self.keepalive_only = keepalive_only
        self.enable_haproxy_protocol = enable_haproxy_protocol
        self.health_check_interval = health_check_interval.strip(
        ) if health_check_interval else None

    def get_port_start(self) -> List[int]:
        ports = []
        if self.frontend_port is not None:
            ports.append(cast(int, self.frontend_port))
        if self.monitor_port is not None:
            ports.append(cast(int, self.monitor_port))
        return ports

    def get_virtual_ip(self) -> Optional[str]:
        return self.virtual_ip

    def validate(self) -> None:
        super(IngressSpec, self).validate()

        if not self.backend_service:
            raise SpecValidationError(
                'Cannot add ingress: No backend_service specified')
        if not self.keepalive_only and not self.frontend_port:
            raise SpecValidationError(
                'Cannot add ingress: No frontend_port specified')
        if not self.monitor_port:
            raise SpecValidationError(
                'Cannot add ingress: No monitor_port specified')
        if not self.virtual_ip and not self.virtual_ips_list:
            raise SpecValidationError(
                'Cannot add ingress: No virtual_ip provided')
        if self.virtual_ip is not None and self.virtual_ips_list is not None:
            raise SpecValidationError(
                'Cannot add ingress: Single and multiple virtual IPs specified')
        if self.health_check_interval:
            valid_units = ['s', 'm', 'h']
            m = re.search(rf"^(\d+)({'|'.join(valid_units)})$", self.health_check_interval)
            if not m:
                raise SpecValidationError(
                    f'Cannot add ingress: Invalid health_check_interval specified. '
                    f'Valid units are: {valid_units}')


yaml.add_representer(IngressSpec, ServiceSpec.yaml_representer)


class MgmtGatewaySpec(ServiceSpec):
    def __init__(self,
                 service_type: str = 'mgmt-gateway',
                 service_id: Optional[str] = None,
                 config: Optional[Dict[str, str]] = None,
                 networks: Optional[List[str]] = None,
                 placement: Optional[PlacementSpec] = None,
                 disable_https: Optional[bool] = False,
                 enable_auth: Optional[bool] = False,
                 port: Optional[int] = None,
                 ssl_certificate: Optional[str] = None,
                 ssl_certificate_key: Optional[str] = None,
                 ssl_prefer_server_ciphers: Optional[str] = None,
                 ssl_session_tickets: Optional[str] = None,
                 ssl_session_timeout: Optional[str] = None,
                 ssl_session_cache: Optional[str] = None,
                 server_tokens: Optional[str] = None,
                 ssl_stapling: Optional[str] = None,
                 ssl_stapling_verify: Optional[str] = None,
                 ssl_protocols: Optional[List[str]] = None,
                 ssl_ciphers: Optional[List[str]] = None,
                 enable_health_check_endpoint: bool = False,
                 preview_only: bool = False,
                 unmanaged: bool = False,
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 custom_configs: Optional[List[CustomConfig]] = None,
                 ):
        assert service_type == 'mgmt-gateway'

        super(MgmtGatewaySpec, self).__init__(
            'mgmt-gateway', service_id=service_id,
            placement=placement, config=config,
            networks=networks,
            preview_only=preview_only,
            extra_container_args=extra_container_args,
            extra_entrypoint_args=extra_entrypoint_args,
            custom_configs=custom_configs
        )
        #: Is a flag to disable HTTPS. If True, the server will use unsecure HTTP
        self.disable_https = disable_https
        #: Is a flag to enable SSO auth. Requires oauth2-proxy to be active for SSO authentication.
        self.enable_auth = enable_auth
        #: The port number on which the server will listen
        self.port = port
        #: A multi-line string that contains the SSL certificate
        self.ssl_certificate = ssl_certificate
        #: A multi-line string that contains the SSL key
        self.ssl_certificate_key = ssl_certificate_key
        #: Prefer server ciphers over client ciphers: on | off
        self.ssl_prefer_server_ciphers = ssl_prefer_server_ciphers
        #: A multioption flag to control session tickets: on | off
        self.ssl_session_tickets = ssl_session_tickets
        #: The duration for SSL session timeout. Syntax: time (i.e: 5m)
        self.ssl_session_timeout = ssl_session_timeout
        #: Duration an SSL/TLS session is cached: off | none | [builtin[:size]] [shared:name:size]
        self.ssl_session_cache = ssl_session_cache
        #: Flag control server tokens in responses:  on | off | build | string
        self.server_tokens = server_tokens
        #: Flag to enable or disable SSL stapling: on | off
        self.ssl_stapling = ssl_stapling
        #: Flag to control verification of SSL stapling: on | off
        self.ssl_stapling_verify = ssl_stapling_verify
        #: A list of supported SSL protocols (as supported by nginx)
        self.ssl_protocols = ssl_protocols
        #: List of supported secure SSL ciphers. Changing this list may reduce system security.
        self.ssl_ciphers = ssl_ciphers
        self.enable_health_check_endpoint = enable_health_check_endpoint

    def get_port_start(self) -> List[int]:
        ports = []
        if self.port is not None:
            ports.append(cast(int, self.port))
        return ports

    def validate(self) -> None:
        super(MgmtGatewaySpec, self).validate()
        self._validate_port(self.port)
        self._validate_certificate(self.ssl_certificate, "ssl_certificate")
        self._validate_private_key(self.ssl_certificate_key, "ssl_certificate_key")
        self._validate_boolean_switch(self.ssl_prefer_server_ciphers, "ssl_prefer_server_ciphers")
        self._validate_boolean_switch(self.ssl_session_tickets, "ssl_session_tickets")
        self._validate_session_timeout(self.ssl_session_timeout)
        self._validate_session_cache(self.ssl_session_cache)
        self._validate_server_tokens(self.server_tokens)
        self._validate_boolean_switch(self.ssl_stapling, "ssl_stapling")
        self._validate_boolean_switch(self.ssl_stapling_verify, "ssl_stapling_verify")
        self._validate_ssl_protocols(self.ssl_protocols)

    def _validate_port(self, port: Optional[int]) -> None:
        if port is not None and not (1 <= port <= 65535):
            raise SpecValidationError(f"Invalid port: {port}. Must be between 1 and 65535.")

    def _validate_certificate(self, cert: Optional[str], name: str) -> None:
        if cert is not None and not isinstance(cert, str):
            raise SpecValidationError(f"Invalid {name}. Must be a string.")

    def _validate_private_key(self, key: Optional[str], name: str) -> None:
        if key is not None and not isinstance(key, str):
            raise SpecValidationError(f"Invalid {name}. Must be a string.")

    def _validate_boolean_switch(self, value: Optional[str], name: str) -> None:
        if value is not None and value not in ['on', 'off']:
            raise SpecValidationError(f"Invalid {name}: {value}. Supported values: on | off.")

    def _validate_session_timeout(self, timeout: Optional[str]) -> None:
        if timeout is not None and not re.match(r'^\d+[smhd]$', timeout):
            raise SpecValidationError(f"Invalid SSL Session Timeout: {timeout}. \
            Value must be a number followed by 's', 'm', 'h', or 'd'.")

    def _validate_session_cache(self, cache: Optional[str]) -> None:
        valid_caches = ['none', 'off', 'builtin', 'shared']
        if cache is not None and not any(cache.startswith(vc) for vc in valid_caches):
            raise SpecValidationError(f"Invalid SSL Session Cache: {cache}. Supported values are: \
            off | none | [builtin[:size]] [shared:name:size]")

    def _validate_server_tokens(self, tokens: Optional[str]) -> None:
        if tokens is not None and tokens not in ['on', 'off', 'build', 'string']:
            raise SpecValidationError(f"Invalid Server Tokens: {tokens}. Must be one of \
            ['on', 'off', 'build', 'version'].")

    def _validate_ssl_protocols(self, protocols: Optional[List[str]]) -> None:
        if protocols is None:
            return
        valid_protocols = ['TLSv1.2', 'TLSv1.3']
        for protocol in protocols:
            if protocol not in valid_protocols:
                raise SpecValidationError(f"Invalid SSL Protocol: {protocol}. \
                Must be one of {valid_protocols}.")


yaml.add_representer(MgmtGatewaySpec, ServiceSpec.yaml_representer)


class OAuth2ProxySpec(ServiceSpec):
    def __init__(self,
                 service_type: str = 'oauth2-proxy',
                 service_id: Optional[str] = None,
                 config: Optional[Dict[str, str]] = None,
                 networks: Optional[List[str]] = None,
                 placement: Optional[PlacementSpec] = None,
                 https_address: Optional[str] = None,
                 provider_display_name: Optional[str] = None,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 oidc_issuer_url: Optional[str] = None,
                 redirect_url: Optional[str] = None,
                 cookie_secret: Optional[str] = None,
                 ssl_certificate: Optional[str] = None,
                 ssl_certificate_key: Optional[str] = None,
                 allowlist_domains: Optional[List[str]] = None,
                 unmanaged: bool = False,
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 custom_configs: Optional[List[CustomConfig]] = None,
                 ):
        assert service_type == 'oauth2-proxy'

        super(OAuth2ProxySpec, self).__init__(
            'oauth2-proxy', service_id=service_id,
            placement=placement, config=config,
            networks=networks,
            extra_container_args=extra_container_args,
            extra_entrypoint_args=extra_entrypoint_args,
            custom_configs=custom_configs
        )
        #: The address for HTTPS connections, formatted as 'host:port'.
        self.https_address = https_address
        #: The display name for the identity provider (IDP) in the UI.
        self.provider_display_name = provider_display_name
        #: The client ID for authenticating with the identity provider.
        self.client_id = client_id
        #: The client secret for authenticating with the identity provider.
        self.client_secret = client_secret
        #: The URL of the OpenID Connect (OIDC) issuer.
        self.oidc_issuer_url = oidc_issuer_url
        #: The URL oauth2-proxy will redirect to after a successful login. If not provided
        # cephadm will calculate automatically the value of this url.
        self.redirect_url = redirect_url
        #: The secret key used for signing cookies. Its length must be 16,
        # 24, or 32 bytes to create an AES cipher.
        self.cookie_secret = cookie_secret
        #: The multi-line SSL certificate for encrypting communications.
        self.ssl_certificate = ssl_certificate
        #: The multi-line SSL certificate private key for decrypting communications.
        self.ssl_certificate_key = ssl_certificate_key
        #: List of allowed domains for safe redirection after login or logout,
        # preventing unauthorized redirects.
        self.allowlist_domains = allowlist_domains
        self.unmanaged = unmanaged

    def get_port_start(self) -> List[int]:
        ports = [4180]
        return ports

    def validate(self) -> None:
        super(OAuth2ProxySpec, self).validate()
        self._validate_non_empty_string(self.provider_display_name, "provider_display_name")
        self._validate_non_empty_string(self.client_id, "client_id")
        self._validate_non_empty_string(self.client_secret, "client_secret")
        self._validate_cookie_secret(self.cookie_secret)
        self._validate_url(self.oidc_issuer_url, "oidc_issuer_url")
        if self.redirect_url is not None:
            self._validate_url(self.redirect_url, "redirect_url")
        if self.https_address is not None:
            self._validate_https_address(self.https_address)

    def _validate_non_empty_string(self, value: Optional[str], field_name: str) -> None:
        if not value or not isinstance(value, str) or not value.strip():
            raise SpecValidationError(f"Invalid {field_name}: Must be a non-empty string.")

    def _validate_url(self, url: Optional[str], field_name: str) -> None:
        from urllib.parse import urlparse
        try:
            result = urlparse(url)
        except Exception as e:
            raise SpecValidationError(f"Invalid {field_name}: {e}. Must be a valid URL.")
        else:
            if not all([result.scheme, result.netloc]):
                raise SpecValidationError(f"Error parsing {field_name} field: Must be a valid URL.")

    def _validate_https_address(self, https_address: Optional[str]) -> None:
        from urllib.parse import urlparse
        result = urlparse(f'http://{https_address}')
        # Check if netloc contains a valid IP or hostname and a port
        if not result.netloc or ':' not in result.netloc:
            raise SpecValidationError("Invalid https_address: Valid format [IP|hostname]:port.")
        # Split netloc into hostname and port
        hostname, port = result.netloc.rsplit(':', 1)
        # Validate port
        if not port.isdigit() or not (0 <= int(port) <= 65535):
            raise SpecValidationError("Invalid https_address: Port must be between 0 and 65535.")

    def _validate_cookie_secret(self, cookie_secret: Optional[str]) -> None:
        if cookie_secret is None:
            return
        if not isinstance(cookie_secret, str):
            raise SpecValidationError("Invalid cookie_secret: Must be a non-empty string.")

        import base64
        import binascii
        try:
            # Try decoding the cookie_secret as base64
            decoded_secret = base64.urlsafe_b64decode(cookie_secret)
            length = len(decoded_secret)
        except binascii.Error:
            # If decoding fails, consider it as a plain string
            length = len(cookie_secret.encode('utf-8'))

        if length not in [16, 24, 32]:
            raise SpecValidationError(f"cookie_secret is {length} bytes "
                                      "but must be 16, 24, or 32 bytes to create an AES cipher.")


yaml.add_representer(OAuth2ProxySpec, ServiceSpec.yaml_representer)


class InitContainerSpec(object):
    """An init container is not a service that lives on its own, but rather
    is used to run and exit prior to a service container starting in order
    to help initialize some aspect of the container environment.
    For example: a command to pre-populate a DB file with expected values
    before the server starts.
    """

    _basic_fields = [
        'image',
        'entrypoint',
        'volume_mounts',
        'envs',
        'privileged',
    ]
    _fields = _basic_fields + ['entrypoint_args']

    def __init__(
        self,
        image: Optional[str] = None,
        entrypoint: Optional[str] = None,
        entrypoint_args: Optional[GeneralArgList] = None,
        volume_mounts: Optional[Dict[str, str]] = None,
        envs: Optional[List[str]] = None,
        privileged: Optional[bool] = None,
    ):
        self.image = image
        self.entrypoint = entrypoint
        self.entrypoint_args: Optional[ArgumentList] = None
        if entrypoint_args:
            self.entrypoint_args = ArgumentSpec.from_general_args(
                entrypoint_args
            )
        self.volume_mounts = volume_mounts
        self.envs = envs
        self.privileged = privileged
        self.validate()

    def validate(self) -> None:
        if all(getattr(self, key) is None for key in self._fields):
            raise SpecValidationError(
                'At least one parameter must be set (no values were specified)'
            )

    def to_json(self, flatten_args: bool = False) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        for key in self._basic_fields:
            value = getattr(self, key, None)
            if value is not None:
                data[key] = value
        if self.entrypoint_args and flatten_args:
            data['entrypoint_args'] = sum(
                (ea.to_args() for ea in self.entrypoint_args), []
            )
        elif self.entrypoint_args:
            data['entrypoint_args'] = ArgumentSpec.map_json(
                self.entrypoint_args
            )
        return data

    def __repr__(self) -> str:
        vals = ((key, getattr(self, key)) for key in self._fields)
        contents = ", ".join(f'{key}={val!r}' for key, val in vals if val)
        return f'InitContainerSpec({contents})'

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> 'InitContainerSpec':
        return cls(
            image=data.get('image'),
            entrypoint=data.get('entrypoint'),
            entrypoint_args=data.get('entrypoint_args'),
            volume_mounts=data.get('volume_mounts'),
            envs=data.get('envs'),
            privileged=data.get('privileged'),
        )

    @classmethod
    def import_values(
        cls, values: List[Union['InitContainerSpec', Dict[str, Any]]]
    ) -> List['InitContainerSpec']:
        out: List[InitContainerSpec] = []
        for value in values:
            if isinstance(value, dict):
                out.append(cls.from_json(value))
            elif isinstance(value, cls):
                out.append(value)
            elif hasattr(value, 'to_json'):
                # This is a workaround for silly ceph mgr object/type identity
                # mismatches due to multiple python interpreters in use.
                out.append(cls.from_json(value.to_json()))
            else:
                raise SpecValidationError(
                    f"Unknown type for InitContainerSpec: {type(value)}"
                )
        return out


class CustomContainerSpec(ServiceSpec):
    def __init__(self,
                 service_type: str = 'container',
                 service_id: Optional[str] = None,
                 config: Optional[Dict[str, str]] = None,
                 networks: Optional[List[str]] = None,
                 placement: Optional[PlacementSpec] = None,
                 unmanaged: bool = False,
                 preview_only: bool = False,
                 image: Optional[str] = None,
                 entrypoint: Optional[str] = None,
                 uid: Optional[int] = None,
                 gid: Optional[int] = None,
                 volume_mounts: Optional[Dict[str, str]] = {},
                 # args are for the container runtime, not entrypoint
                 args: Optional[GeneralArgList] = [],
                 envs: Optional[List[str]] = [],
                 privileged: Optional[bool] = False,
                 bind_mounts: Optional[List[List[str]]] = None,
                 ports: Optional[List[int]] = [],
                 dirs: Optional[List[str]] = [],
                 files: Optional[Dict[str, Any]] = {},
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 custom_configs: Optional[List[CustomConfig]] = None,
                 init_containers: Optional[List[Union['InitContainerSpec', Dict[str, Any]]]] = None,
                 ):
        assert service_type == 'container'
        assert service_id is not None
        assert image is not None

        super(CustomContainerSpec, self).__init__(
            service_type, service_id,
            placement=placement, unmanaged=unmanaged,
            preview_only=preview_only, config=config,
            networks=networks, extra_container_args=extra_container_args,
            extra_entrypoint_args=extra_entrypoint_args,
            custom_configs=custom_configs)

        self.image = image
        self.entrypoint = entrypoint
        self.uid = uid
        self.gid = gid
        self.volume_mounts = volume_mounts
        self.args = args
        self.envs = envs
        self.privileged = privileged
        self.bind_mounts = bind_mounts
        self.ports = ports
        self.dirs = dirs
        self.files = files
        self.init_containers: Optional[List['InitContainerSpec']] = None
        if init_containers:
            self.init_containers = InitContainerSpec.import_values(
                init_containers
            )

    def config_json(self) -> Dict[str, Any]:
        """
        Helper function to get the value of the `--config-json` cephadm
        command line option. It will contain all specification properties
        that haven't a `None` value. Such properties will get default
        values in cephadm.
        :return: Returns a dictionary containing all specification
            properties.
        """
        config_json = {}
        for prop in ['image', 'entrypoint', 'uid', 'gid', 'args',
                     'envs', 'volume_mounts', 'privileged',
                     'bind_mounts', 'ports', 'dirs', 'files']:
            value = getattr(self, prop)
            if value is not None:
                config_json[prop] = value
        return config_json

    def validate(self) -> None:
        super(CustomContainerSpec, self).validate()

        if self.args and self.extra_container_args:
            raise SpecValidationError(
                '"args" and "extra_container_args" are mutually exclusive '
                '(and both serve the same purpose)')

        if self.files and self.custom_configs:
            raise SpecValidationError(
                '"files" and "custom_configs" are mutually exclusive '
                '(and both serve the same purpose)')

    # use quotes for OrderedDict, getting this to work across py 3.6, 3.7
    # and 3.7+ is suprisingly difficult
    def to_json(self) -> "OrderedDict[str, Any]":
        data = super().to_json()
        ics = data.get('spec', {}).get('init_containers')
        if ics:
            data['spec']['init_containers'] = [ic.to_json() for ic in ics]
        return data


yaml.add_representer(CustomContainerSpec, ServiceSpec.yaml_representer)


class MonitoringSpec(ServiceSpec):
    def __init__(self,
                 service_type: str,
                 service_id: Optional[str] = None,
                 config: Optional[Dict[str, str]] = None,
                 networks: Optional[List[str]] = None,
                 placement: Optional[PlacementSpec] = None,
                 unmanaged: bool = False,
                 preview_only: bool = False,
                 port: Optional[int] = None,
                 targets: Optional[List[str]] = None,
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 custom_configs: Optional[List[CustomConfig]] = None,
                 ):
        assert service_type in ['grafana', 'node-exporter', 'prometheus', 'alertmanager',
                                'loki', 'promtail']

        super(MonitoringSpec, self).__init__(
            service_type, service_id,
            placement=placement, unmanaged=unmanaged,
            preview_only=preview_only, config=config,
            networks=networks, extra_container_args=extra_container_args,
            extra_entrypoint_args=extra_entrypoint_args,
            custom_configs=custom_configs, targets=targets)

        self.service_type = service_type
        self.port = port

    def get_port_start(self) -> List[int]:
        return [self.get_port()]

    def get_port(self) -> int:
        if self.port:
            return self.port
        else:
            return {'prometheus': 9095,
                    'node-exporter': 9100,
                    'alertmanager': 9093,
                    'grafana': 3000,
                    'loki': 3100,
                    'promtail': 9080}[self.service_type]


yaml.add_representer(MonitoringSpec, ServiceSpec.yaml_representer)


class AlertManagerSpec(MonitoringSpec):
    def __init__(self,
                 service_type: str = 'alertmanager',
                 service_id: Optional[str] = None,
                 placement: Optional[PlacementSpec] = None,
                 unmanaged: bool = False,
                 preview_only: bool = False,
                 user_data: Optional[Dict[str, Any]] = None,
                 config: Optional[Dict[str, str]] = None,
                 networks: Optional[List[str]] = None,
                 port: Optional[int] = None,
                 secure: bool = False,
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 custom_configs: Optional[List[CustomConfig]] = None,
                 ):
        assert service_type == 'alertmanager'
        super(AlertManagerSpec, self).__init__(
            'alertmanager', service_id=service_id,
            placement=placement, unmanaged=unmanaged,
            preview_only=preview_only, config=config, networks=networks, port=port,
            extra_container_args=extra_container_args, extra_entrypoint_args=extra_entrypoint_args,
            custom_configs=custom_configs)

        # Custom configuration.
        #
        # Example:
        # service_type: alertmanager
        # service_id: xyz
        # user_data:
        #   default_webhook_urls:
        #   - "https://foo"
        #   - "https://bar"
        #
        # Documentation:
        # default_webhook_urls - A list of additional URL's that are
        #                        added to the default receivers'
        #                        <webhook_configs> configuration.
        self.user_data = user_data or {}
        self.secure = secure

    def get_port_start(self) -> List[int]:
        return [self.get_port(), 9094]

    def validate(self) -> None:
        super(AlertManagerSpec, self).validate()

        if self.port == 9094:
            raise SpecValidationError(
                'Port 9094 is reserved for AlertManager cluster listen address')


yaml.add_representer(AlertManagerSpec, ServiceSpec.yaml_representer)


class GrafanaSpec(MonitoringSpec):
    def __init__(self,
                 service_type: str = 'grafana',
                 service_id: Optional[str] = None,
                 placement: Optional[PlacementSpec] = None,
                 unmanaged: bool = False,
                 preview_only: bool = False,
                 config: Optional[Dict[str, str]] = None,
                 networks: Optional[List[str]] = None,
                 only_bind_port_on_networks: bool = False,
                 port: Optional[int] = None,
                 protocol: Optional[str] = 'https',
                 initial_admin_password: Optional[str] = None,
                 anonymous_access: bool = True,
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 custom_configs: Optional[List[CustomConfig]] = None,
                 ):
        assert service_type == 'grafana'
        super(GrafanaSpec, self).__init__(
            'grafana', service_id=service_id,
            placement=placement, unmanaged=unmanaged,
            preview_only=preview_only, config=config, networks=networks, port=port,
            extra_container_args=extra_container_args, extra_entrypoint_args=extra_entrypoint_args,
            custom_configs=custom_configs)

        self.initial_admin_password = initial_admin_password
        self.anonymous_access = anonymous_access
        self.protocol = protocol

        # whether ports daemons for this service bind to should
        # bind to only hte networks listed in networks param, or
        # to all networks. Defaults to false which is saying to bind
        # on all networks.
        self.only_bind_port_on_networks = only_bind_port_on_networks

    def validate(self) -> None:
        super(GrafanaSpec, self).validate()
        if self.protocol not in ['http', 'https']:
            err_msg = f"Invalid protocol '{self.protocol}'. Valid values are: 'http', 'https'."
            raise SpecValidationError(err_msg)

        if not self.anonymous_access and not self.initial_admin_password:
            err_msg = ('Either initial_admin_password must be set or anonymous_access '
                       'must be set to true. Otherwise the grafana dashboard will '
                       'be inaccessible.')
            raise SpecValidationError(err_msg)

    def to_json(self) -> "OrderedDict[str, Any]":
        json_dict = super(GrafanaSpec, self).to_json()
        if not self.anonymous_access:
            # This field was added as a boolean that defaults
            # to True, which makes it get dropped when the user
            # sets it to False and it is converted to json. This means
            # the in memory version of the spec will have the option set
            # correctly, but the persistent version we store in the config-key
            # store will always drop this option. It's already been backported to
            # some release versions, or we'd probably just rename it to
            # no_anonymous_access and default it to False. This block is to
            # handle this option specially and in the future, we should avoid
            # boolean fields that default to True.
            if 'spec' not in json_dict:
                json_dict['spec'] = {}
            json_dict['spec']['anonymous_access'] = False
        return json_dict


yaml.add_representer(GrafanaSpec, ServiceSpec.yaml_representer)


class PrometheusSpec(MonitoringSpec):
    def __init__(self,
                 service_type: str = 'prometheus',
                 service_id: Optional[str] = None,
                 placement: Optional[PlacementSpec] = None,
                 unmanaged: bool = False,
                 preview_only: bool = False,
                 config: Optional[Dict[str, str]] = None,
                 networks: Optional[List[str]] = None,
                 only_bind_port_on_networks: bool = False,
                 port: Optional[int] = None,
                 retention_time: Optional[str] = None,
                 retention_size: Optional[str] = None,
                 targets: Optional[List[str]] = None,
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 custom_configs: Optional[List[CustomConfig]] = None,
                 ):
        assert service_type == 'prometheus'
        super(PrometheusSpec, self).__init__(
            'prometheus', service_id=service_id,
            placement=placement, unmanaged=unmanaged,
            preview_only=preview_only, config=config, networks=networks, port=port, targets=targets,
            extra_container_args=extra_container_args, extra_entrypoint_args=extra_entrypoint_args,
            custom_configs=custom_configs)

        self.retention_time = retention_time.strip() if retention_time else None
        self.retention_size = retention_size.strip() if retention_size else None
        self.only_bind_port_on_networks = only_bind_port_on_networks

    def validate(self) -> None:
        super(PrometheusSpec, self).validate()

        if self.retention_time:
            valid_units = ['y', 'w', 'd', 'h', 'm', 's']
            m = re.search(rf"^(\d+)({'|'.join(valid_units)})$", self.retention_time)
            if not m:
                units = ', '.join(valid_units)
                raise SpecValidationError(f"Invalid retention time. Valid units are: {units}")
        if self.retention_size:
            valid_units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']
            m = re.search(rf"^(\d+)({'|'.join(valid_units)})$", self.retention_size)
            if not m:
                units = ', '.join(valid_units)
                raise SpecValidationError(f"Invalid retention size. Valid units are: {units}")


yaml.add_representer(PrometheusSpec, ServiceSpec.yaml_representer)


class SNMPGatewaySpec(ServiceSpec):
    class SNMPVersion(str, enum.Enum):
        V2c = 'V2c'
        V3 = 'V3'

        def to_json(self) -> str:
            return self.value

    class SNMPAuthType(str, enum.Enum):
        MD5 = 'MD5'
        SHA = 'SHA'

        def to_json(self) -> str:
            return self.value

    class SNMPPrivacyType(str, enum.Enum):
        DES = 'DES'
        AES = 'AES'

        def to_json(self) -> str:
            return self.value

    valid_destination_types = [
        'Name:Port',
        'IPv4:Port'
    ]

    def __init__(self,
                 service_type: str = 'snmp-gateway',
                 snmp_version: Optional[SNMPVersion] = None,
                 snmp_destination: str = '',
                 credentials: Dict[str, str] = {},
                 engine_id: Optional[str] = None,
                 auth_protocol: Optional[SNMPAuthType] = None,
                 privacy_protocol: Optional[SNMPPrivacyType] = None,
                 placement: Optional[PlacementSpec] = None,
                 unmanaged: bool = False,
                 preview_only: bool = False,
                 port: Optional[int] = None,
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 custom_configs: Optional[List[CustomConfig]] = None,
                 ):
        assert service_type == 'snmp-gateway'

        super(SNMPGatewaySpec, self).__init__(
            service_type,
            placement=placement,
            unmanaged=unmanaged,
            preview_only=preview_only,
            extra_container_args=extra_container_args,
            extra_entrypoint_args=extra_entrypoint_args,
            custom_configs=custom_configs)

        self.service_type = service_type
        self.snmp_version = snmp_version
        self.snmp_destination = snmp_destination
        self.port = port
        self.credentials = credentials
        self.engine_id = engine_id
        self.auth_protocol = auth_protocol
        self.privacy_protocol = privacy_protocol

    @classmethod
    def _from_json_impl(cls, json_spec: dict) -> 'SNMPGatewaySpec':

        cpy = json_spec.copy()
        types = [
            ('snmp_version', SNMPGatewaySpec.SNMPVersion),
            ('auth_protocol', SNMPGatewaySpec.SNMPAuthType),
            ('privacy_protocol', SNMPGatewaySpec.SNMPPrivacyType),
        ]
        for d in cpy, cpy.get('spec', {}):
            for key, enum_cls in types:
                try:
                    if key in d:
                        d[key] = enum_cls(d[key])
                except ValueError:
                    raise SpecValidationError(f'{key} unsupported. Must be one of '
                                              f'{", ".join(enum_cls)}')
        return super(SNMPGatewaySpec, cls)._from_json_impl(cpy)

    @property
    def ports(self) -> List[int]:
        return [self.port or 9464]

    def get_port_start(self) -> List[int]:
        return self.ports

    def validate(self) -> None:
        super(SNMPGatewaySpec, self).validate()

        if not self.credentials:
            raise SpecValidationError(
                'Missing authentication information (credentials). '
                'SNMP V2c and V3 require credential information'
            )
        elif not self.snmp_version:
            raise SpecValidationError(
                'Missing SNMP version (snmp_version)'
            )

        creds_requirement = {
            'V2c': ['snmp_community'],
            'V3': ['snmp_v3_auth_username', 'snmp_v3_auth_password']
        }
        if self.privacy_protocol:
            creds_requirement['V3'].append('snmp_v3_priv_password')

        missing = [parm for parm in creds_requirement[self.snmp_version]
                   if parm not in self.credentials]
        # check that credentials are correct for the version
        if missing:
            raise SpecValidationError(
                f'SNMP {self.snmp_version} credentials are incomplete. Missing {", ".join(missing)}'
            )

        if self.engine_id:
            if 10 <= len(self.engine_id) <= 64 and \
               is_hex(self.engine_id) and \
               len(self.engine_id) % 2 == 0:
                pass
            else:
                raise SpecValidationError(
                    'engine_id must be a string containing 10-64 hex characters. '
                    'Its length must be divisible by 2'
                )

        else:
            if self.snmp_version == 'V3':
                raise SpecValidationError(
                    'Must provide an engine_id for SNMP V3 notifications'
                )

        if not self.snmp_destination:
            raise SpecValidationError(
                'SNMP destination (snmp_destination) must be provided'
            )
        else:
            valid, description = valid_addr(self.snmp_destination)
            if not valid:
                raise SpecValidationError(
                    f'SNMP destination (snmp_destination) is invalid: {description}'
                )
            if description not in self.valid_destination_types:
                raise SpecValidationError(
                    f'SNMP destination (snmp_destination) type ({description}) is invalid. '
                    f'Must be either: {", ".join(sorted(self.valid_destination_types))}'
                )


yaml.add_representer(SNMPGatewaySpec, ServiceSpec.yaml_representer)


class MDSSpec(ServiceSpec):
    def __init__(self,
                 service_type: str = 'mds',
                 service_id: Optional[str] = None,
                 placement: Optional[PlacementSpec] = None,
                 config: Optional[Dict[str, str]] = None,
                 unmanaged: bool = False,
                 preview_only: bool = False,
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 custom_configs: Optional[List[CustomConfig]] = None,
                 ):
        assert service_type == 'mds'
        super(MDSSpec, self).__init__('mds', service_id=service_id,
                                      placement=placement,
                                      config=config,
                                      unmanaged=unmanaged,
                                      preview_only=preview_only,
                                      extra_container_args=extra_container_args,
                                      extra_entrypoint_args=extra_entrypoint_args,
                                      custom_configs=custom_configs)

    def validate(self) -> None:
        super(MDSSpec, self).validate()

        if str(self.service_id)[0].isdigit():
            raise SpecValidationError('MDS service id cannot start with a numeric digit')


yaml.add_representer(MDSSpec, ServiceSpec.yaml_representer)


class MONSpec(ServiceSpec):
    def __init__(self,
                 service_type: str,
                 service_id: Optional[str] = None,
                 placement: Optional[PlacementSpec] = None,
                 count: Optional[int] = None,
                 config: Optional[Dict[str, str]] = None,
                 unmanaged: bool = False,
                 preview_only: bool = False,
                 networks: Optional[List[str]] = None,
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 custom_configs: Optional[List[CustomConfig]] = None,
                 crush_locations: Optional[Dict[str, List[str]]] = None,
                 ):
        assert service_type == 'mon'
        super(MONSpec, self).__init__('mon', service_id=service_id,
                                      placement=placement,
                                      count=count,
                                      config=config,
                                      unmanaged=unmanaged,
                                      preview_only=preview_only,
                                      networks=networks,
                                      extra_container_args=extra_container_args,
                                      extra_entrypoint_args=extra_entrypoint_args,
                                      custom_configs=custom_configs)

        self.crush_locations = crush_locations
        self.validate()

    def validate(self) -> None:
        if self.crush_locations:
            for host, crush_locs in self.crush_locations.items():
                try:
                    assert_valid_host(host)
                except SpecValidationError as e:
                    err_str = f'Invalid hostname found in spec crush locations: {e}'
                    raise SpecValidationError(err_str)
                for cloc in crush_locs:
                    if '=' not in cloc or len(cloc.split('=')) != 2:
                        err_str = ('Crush locations must be of form <bucket>=<location>. '
                                   f'Found crush location: {cloc}')
                        raise SpecValidationError(err_str)


yaml.add_representer(MONSpec, ServiceSpec.yaml_representer)


class TracingSpec(ServiceSpec):
    SERVICE_TYPES = ['elasticsearch', 'jaeger-collector', 'jaeger-query', 'jaeger-agent']

    def __init__(self,
                 service_type: str,
                 es_nodes: Optional[str] = None,
                 without_query: bool = False,
                 service_id: Optional[str] = None,
                 config: Optional[Dict[str, str]] = None,
                 networks: Optional[List[str]] = None,
                 placement: Optional[PlacementSpec] = None,
                 unmanaged: bool = False,
                 preview_only: bool = False
                 ):
        assert service_type in TracingSpec.SERVICE_TYPES + ['jaeger-tracing']

        super(TracingSpec, self).__init__(
            service_type, service_id,
            placement=placement, unmanaged=unmanaged,
            preview_only=preview_only, config=config,
            networks=networks)
        self.without_query = without_query
        self.es_nodes = es_nodes

    def get_port_start(self) -> List[int]:
        return [self.get_port()]

    def get_port(self) -> int:
        return {'elasticsearch': 9200,
                'jaeger-agent': 6799,
                'jaeger-collector': 14250,
                'jaeger-query': 16686}[self.service_type]

    def get_tracing_specs(self) -> List[ServiceSpec]:
        assert self.service_type == 'jaeger-tracing'
        specs: List[ServiceSpec] = []
        daemons: Dict[str, Optional[PlacementSpec]] = {
            daemon: None for daemon in TracingSpec.SERVICE_TYPES}

        if self.es_nodes:
            del daemons['elasticsearch']
        if self.without_query:
            del daemons['jaeger-query']
        if self.placement:
            daemons.update({'jaeger-collector': self.placement})

        for daemon, daemon_placement in daemons.items():
            specs.append(TracingSpec(service_type=daemon,
                                     es_nodes=self.es_nodes,
                                     placement=daemon_placement,
                                     unmanaged=self.unmanaged,
                                     config=self.config,
                                     networks=self.networks,
                                     preview_only=self.preview_only
                                     ))
        return specs


yaml.add_representer(TracingSpec, ServiceSpec.yaml_representer)


class TunedProfileSpec():
    def __init__(self,
                 profile_name: str,
                 placement: Optional[PlacementSpec] = None,
                 settings: Optional[Dict[str, str]] = None,
                 ):
        self.profile_name = profile_name
        self.placement = placement or PlacementSpec(host_pattern='*')
        self.settings = settings or {}
        self._last_updated: str = ''

    @classmethod
    def from_json(cls, spec: Dict[str, Any]) -> 'TunedProfileSpec':
        data = {}
        if 'profile_name' not in spec:
            raise SpecValidationError('Tuned profile spec must include "profile_name" field')
        data['profile_name'] = spec['profile_name']
        if not isinstance(data['profile_name'], str):
            raise SpecValidationError('"profile_name" field must be a string')
        if 'placement' in spec:
            data['placement'] = PlacementSpec.from_json(spec['placement'])
        if 'settings' in spec:
            data['settings'] = spec['settings']
        return cls(**data)

    def to_json(self) -> Dict[str, Any]:
        res: Dict[str, Any] = {}
        res['profile_name'] = self.profile_name
        res['placement'] = self.placement.to_json()
        res['settings'] = self.settings
        return res

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, TunedProfileSpec):
            if (
                self.placement == other.placement
                and self.profile_name == other.profile_name
                and self.settings == other.settings
            ):
                return True
            return False
        return NotImplemented

    def __repr__(self) -> str:
        return f'TunedProfile({self.profile_name})'

    def copy(self) -> 'TunedProfileSpec':
        # for making deep copies so you can edit the settings in one without affecting the other
        # mostly for testing purposes
        return TunedProfileSpec(self.profile_name, self.placement, self.settings.copy())


class CephExporterSpec(ServiceSpec):
    def __init__(self,
                 service_type: str = 'ceph-exporter',
                 sock_dir: Optional[str] = None,
                 addrs: str = '',
                 port: Optional[int] = None,
                 prio_limit: Optional[int] = 5,
                 stats_period: Optional[int] = 5,
                 placement: Optional[PlacementSpec] = None,
                 unmanaged: bool = False,
                 preview_only: bool = False,
                 extra_container_args: Optional[GeneralArgList] = None,
                 extra_entrypoint_args: Optional[GeneralArgList] = None,
                 ):
        assert service_type == 'ceph-exporter'

        super(CephExporterSpec, self).__init__(
            service_type,
            placement=placement,
            unmanaged=unmanaged,
            preview_only=preview_only,
            extra_container_args=extra_container_args,
            extra_entrypoint_args=extra_entrypoint_args)

        self.service_type = service_type
        self.sock_dir = sock_dir
        self.addrs = addrs
        self.port = port
        self.prio_limit = prio_limit
        self.stats_period = stats_period

    def get_port_start(self) -> List[int]:
        return [self.port or 9926]

    def validate(self) -> None:
        super(CephExporterSpec, self).validate()

        if not isinstance(self.prio_limit, int):
            raise SpecValidationError(
                    f'prio_limit must be an integer. Got {type(self.prio_limit)}')
        if not isinstance(self.stats_period, int):
            raise SpecValidationError(
                    f'stats_period must be an integer. Got {type(self.stats_period)}')


yaml.add_representer(CephExporterSpec, ServiceSpec.yaml_representer)


class SMBClusterPublicIPSpec:
    # The SMBClusterIPSpec must be able to translate between what cephadm
    # knows about the system, networks using network addresses, and what
    # ctdb wants, an IP combined with a prefixlen and device names.
    def __init__(
        self,
        address: str,
        destination: Union[str, List[str], None] = None,
    ) -> None:
        self.address = address
        self.destination = destination
        self.validate()

    def validate(self) -> None:
        if not self.address:
            raise SpecValidationError('address value missing')
        if '/' not in self.address:
            raise SpecValidationError(
                'a combined address and prefix length is required'
            )
        # in the future we may want to enhance this to take IPs only and figure
        # out the prefixlen automatically. However, we going to start simple and
        # require prefix lengths just like ctdb itself does.
        try:
            # cache the parsed interface address internally
            self._addr_iface = ip_interface(self.address)
        except ValueError as err:
            raise SpecValidationError(
                f'Cannot parse interface address {self.address}'
            ) from err
        # we strongly prefer /{prefixlen} form, even if the user supplied
        # a netmask
        self.address = self._addr_iface.with_prefixlen

        self._destinations = []
        if not self.destination:
            return
        if isinstance(self.destination, str):
            _dests = [self.destination]
        elif isinstance(self.destination, list) and all(
            isinstance(v, str) for v in self.destination
        ):
            _dests = self.destination
        else:
            raise ValueError(
                'destination field must be a string or list of strings'
            )
        for dest in _dests:
            try:
                dnet = ip_network(dest)
            except ValueError as err:
                raise SpecValidationError(
                    f'Cannot parse network value {self.address}'
                ) from err
            self._destinations.append(dnet)

    def __eq__(self, other: Any) -> bool:
        try:
            return (
                other.address == self.address
                and other.destination == self.destination
            )
        except AttributeError:
            return NotImplemented

    def __repr__(self) -> str:
        return (
            f'SMBClusterPublicIPSpec({self.address!r}, {self.destination!r})'
        )

    def to_json(self) -> Dict[str, Any]:
        """Return a JSON-compatible representation of the SMBClusterPublicIPSpec."""
        out: Dict[str, Any] = {'address': self.address}
        if self.destination:
            out['destination'] = self.destination
        return out

    def to_strict(self) -> Dict[str, Any]:
        """Return a strictly formed expanded JSON-compatible representation of
        the spec. This is not round-trip-able.
        """
        # The strict form always contains destination as a list of strings.
        dests = [n.with_prefixlen for n in self._destinations]
        if not dests:
            dests = [self._addr_iface.network.with_prefixlen]
        return {
            'address': self.address,
            'destinations': dests,
        }

    @classmethod
    def from_json(cls, spec: Dict[str, Any]) -> 'SMBClusterPublicIPSpec':
        if 'address' not in spec:
            raise SpecValidationError(
                'SMB cluster public IP spec missing required field: address'
            )
        return cls(spec['address'], spec.get('destination'))

    @classmethod
    def convert_list(
        cls, arg: Optional[List[Any]]
    ) -> Optional[List['SMBClusterPublicIPSpec']]:
        if arg is None:
            return None
        assert isinstance(arg, list)
        out = []
        for value in arg:
            if isinstance(value, cls):
                out.append(value)
            elif hasattr(value, 'to_json'):
                out.append(cls.from_json(value.to_json()))
            elif isinstance(value, dict):
                out.append(cls.from_json(value))
            else:
                raise SpecValidationError(
                    f"Unknown type for SMBClusterPublicIPSpec: {type(value)}"
                )
        return out


class SMBSpec(ServiceSpec):
    service_type = 'smb'
    _valid_features = {'domain', 'clustered'}
    _default_cluster_meta_obj = 'cluster.meta.json'
    _default_cluster_lock_obj = 'cluster.meta.lock'

    def __init__(
        self,
        # --- common service spec args ---
        service_type: str = 'smb',
        service_id: Optional[str] = None,
        placement: Optional[PlacementSpec] = None,
        count: Optional[int] = None,
        config: Optional[Dict[str, str]] = None,
        unmanaged: bool = False,
        preview_only: bool = False,
        networks: Optional[List[str]] = None,
        # --- smb specific values ---
        # cluster_id - a name identifying the smb "cluster" this daemon
        # is part of. A cluster may be made up of one or more services
        # sharing a common configuration.
        cluster_id: str = '',
        # features - a list of terms enabling specific deployment features.
        # terms include: 'domain' to enable Active Dir. Domain membership.
        features: Optional[List[str]] = None,
        # config_uri - a pseudo-uri that resolves to a configuration source
        # that the samba-container can load. A ceph based samba container will
        # be typically storing configuration in rados (rados:// prefix)
        config_uri: str = '',
        # join_sources - a list of pseudo-uris that resolve to a (JSON) blob
        # containing data the samba-container can use to join a domain. A ceph
        # based samba container may typically use a rados uri or a mon
        # config-key store uri (example:
        # `rados:mon-config-key:smb/config/mycluster/join1.json`).
        join_sources: Optional[List[str]] = None,
        # user_sources - a list of pseudo-uris that resolve to a (JSON) blob
        # containing data the samba-container can use to create users (and/or
        # groups). A ceph based samba container may typically use a rados uri
        # or a mon config-key store uri (example:
        # `rados:mon-config-key:smb/config/mycluster/join1.json`).
        user_sources: Optional[List[str]] = None,
        # custom_dns -  a list of IP addresses that will be set up as custom
        # dns servers for the samba container.
        custom_dns: Optional[List[str]] = None,
        # include_ceph_users - A list of ceph auth entity names that will be
        # automatically added to the ceph keyring provided to the samba
        # container.
        include_ceph_users: Optional[List[str]] = None,
        # cluster_meta_uri - a pseudo-uri that resolves to a (rados) object
        # that will store information about the state of samba cluster members
        cluster_meta_uri: Optional[str] = None,
        # cluster_lock_uri - a pseudo-uri that resolves to a (rados) object
        # that will be used by CTDB for a cluster leader / recovery lock.
        cluster_lock_uri: Optional[str] = None,
        # cluster_public_addrs - A list of SMB cluster public IP specs.
        # If supplied, these will be used to esatablish floating virtual ips
        # managed by Samba CTDB cluster subsystem.
        cluster_public_addrs: Optional[List[SMBClusterPublicIPSpec]] = None,
        # --- genearal tweaks ---
        extra_container_args: Optional[GeneralArgList] = None,
        extra_entrypoint_args: Optional[GeneralArgList] = None,
        custom_configs: Optional[List[CustomConfig]] = None,
    ) -> None:
        if service_type != self.service_type:
            raise ValueError(f'invalid service_type: {service_type!r}')
        super().__init__(
            self.service_type,
            service_id=service_id,
            placement=placement,
            count=count,
            config=config,
            unmanaged=unmanaged,
            preview_only=preview_only,
            networks=networks,
            extra_container_args=extra_container_args,
            extra_entrypoint_args=extra_entrypoint_args,
            custom_configs=custom_configs,
        )
        self.cluster_id = cluster_id
        self.features = features or []
        self.config_uri = config_uri
        self.join_sources = join_sources or []
        self.user_sources = user_sources or []
        self.custom_dns = custom_dns or []
        self.include_ceph_users = include_ceph_users or []
        self.cluster_meta_uri = cluster_meta_uri
        self.cluster_lock_uri = cluster_lock_uri
        self.cluster_public_addrs = SMBClusterPublicIPSpec.convert_list(
            cluster_public_addrs
        )
        self.validate()

    def validate(self) -> None:
        if not self.cluster_id:
            raise ValueError('a valid cluster_id is required')
        if not self.config_uri:
            raise ValueError('a valid config_uri is required')
        if self.features:
            invalid = set(self.features).difference(self._valid_features)
            if invalid:
                raise ValueError(
                    f'invalid feature flags: {", ".join(invalid)}'
                )
        if 'clustered' in self.features and not self.cluster_meta_uri:
            # derive a cluster meta uri from config uri by default (if possible)
            self.cluster_meta_uri = self._derive_cluster_uri(
                self.config_uri,
                self._default_cluster_meta_obj,
            )
        if 'clustered' not in self.features and self.cluster_meta_uri:
            raise ValueError(
                'cluster meta uri unsupported when "clustered" feature not set'
            )
        if 'clustered' in self.features and not self.cluster_lock_uri:
            # derive a cluster meta uri from config uri by default (if possible)
            self.cluster_lock_uri = self._derive_cluster_uri(
                self.config_uri,
                self._default_cluster_lock_obj,
            )
        if 'clustered' not in self.features and self.cluster_lock_uri:
            raise ValueError(
                'cluster lock uri unsupported when "clustered" feature not set'
            )
        for spec in self.cluster_public_addrs or []:
            spec.validate()

    def _derive_cluster_uri(self, uri: str, objname: str) -> str:
        if not uri.startswith('rados://'):
            raise ValueError('invalid uri scheme for cluster metadata')
        parts = uri[8:].split('/')
        parts[-1] = objname
        uri = 'rados://' + '/'.join(parts)
        return uri

    def strict_cluster_ip_specs(self) -> List[Dict[str, Any]]:
        return [s.to_strict() for s in (self.cluster_public_addrs or [])]

    def to_json(self) -> "OrderedDict[str, Any]":
        obj = super().to_json()
        spec = obj.get('spec')
        if spec and spec.get('cluster_public_addrs'):
            spec['cluster_public_addrs'] = [
                a.to_json() for a in spec['cluster_public_addrs']
            ]
        return obj


yaml.add_representer(SMBSpec, ServiceSpec.yaml_representer)
