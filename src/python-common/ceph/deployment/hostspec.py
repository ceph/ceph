from collections import OrderedDict
import errno
import re
from typing import Optional, List, Any, Dict


def assert_valid_host(name: str) -> None:
    p = re.compile('^[a-zA-Z0-9-]+$')
    try:
        assert len(name) <= 250, 'name is too long (max 250 chars)'
        for part in name.split('.'):
            assert len(part) > 0, '.-delimited name component must not be empty'
            assert len(part) <= 63, '.-delimited name component must not be more than 63 chars'
            assert p.match(part), 'name component must include only a-z, 0-9, and -'
    except AssertionError as e:
        raise SpecValidationError(str(e) + f'. Got "{name}"')


def assert_valid_oob(oob: Dict[str, str]) -> None:
    fields = ['username', 'password']
    try:
        for field in fields:
            assert field in oob.keys()
    except AssertionError as e:
        raise SpecValidationError(str(e))


class SpecValidationError(Exception):
    """
    Defining an exception here is a bit problematic, cause you cannot properly catch it,
    if it was raised in a different mgr module.
    """
    def __init__(self,
                 msg: str,
                 errno: int = -errno.EINVAL):
        super(SpecValidationError, self).__init__(msg)
        self.errno = errno


class HostSpec(object):
    """
    Information about hosts. Like e.g. ``kubectl get nodes``
    """
    def __init__(self,
                 hostname: str,
                 addr: Optional[str] = None,
                 labels: Optional[List[str]] = None,
                 status: Optional[str] = None,
                 location: Optional[Dict[str, str]] = None,
                 oob: Optional[Dict[str, str]] = None,
                 ):
        self.service_type = 'host'

        #: the bare hostname on the host. Not the FQDN.
        self.hostname = hostname  # type: str

        #: DNS name or IP address to reach it
        self.addr = addr or hostname  # type: str

        #: label(s), if any
        self.labels = labels or []  # type: List[str]

        #: human readable status
        self.status = status or ''  # type: str

        self.location = location

        #: oob details, if provided
        self.oob = oob

    def validate(self) -> None:
        assert_valid_host(self.hostname)
        if self.oob:
            assert_valid_oob(self.oob)

    def to_json(self) -> Dict[str, Any]:
        r: Dict[str, Any] = {
            'hostname': self.hostname,
            'addr': self.addr,
            'labels': list(OrderedDict.fromkeys((self.labels))),
            'status': self.status,
        }
        if self.location:
            r['location'] = self.location
        if self.oob:
            r['oob'] = self.oob
        return r

    @classmethod
    def from_json(cls, host_spec: dict) -> 'HostSpec':
        host_spec = cls.normalize_json(host_spec)
        _cls = cls(
            host_spec['hostname'],
            host_spec['addr'] if 'addr' in host_spec else None,
            list(OrderedDict.fromkeys(
                host_spec['labels'])) if 'labels' in host_spec else None,
            host_spec['status'] if 'status' in host_spec else None,
            host_spec.get('location'),
            host_spec['oob'] if 'oob' in host_spec else None,
        )
        return _cls

    @staticmethod
    def normalize_json(host_spec: dict) -> dict:
        labels = host_spec.get('labels')
        if labels is not None:
            if isinstance(labels, str):
                host_spec['labels'] = [labels]
            elif (
                    not isinstance(labels, list)
                    or any(not isinstance(v, str) for v in labels)
            ):
                raise SpecValidationError(
                    f'Labels ({labels}) must be a string or list of strings'
                )

        loc = host_spec.get('location')
        if loc is not None:
            if (
                    not isinstance(loc, dict)
                    or any(not isinstance(k, str) for k in loc.keys())
                    or any(not isinstance(v, str) for v in loc.values())
            ):
                raise SpecValidationError(
                    f'Location ({loc}) must be a dictionary of strings to strings'
                )

        return host_spec

    def __repr__(self) -> str:
        args = [self.hostname]  # type: List[Any]
        if self.addr is not None:
            args.append(self.addr)
        if self.labels:
            args.append(self.labels)
        if self.status:
            args.append(self.status)
        if self.location:
            args.append(self.location)

        return "HostSpec({})".format(', '.join(map(repr, args)))

    def __str__(self) -> str:
        if self.hostname != self.addr:
            return f'{self.hostname} ({self.addr})'
        return self.hostname

    def __eq__(self, other: Any) -> bool:
        # Let's omit `status` for the moment, as it is still the very same host.
        if not isinstance(other, HostSpec):
            return NotImplemented
        return self.hostname == other.hostname and \
            self.addr == other.addr and \
            sorted(self.labels) == sorted(other.labels) and \
            self.location == other.location
