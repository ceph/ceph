try:
    from typing import Optional, List, Any
except ImportError:
    pass  # just for type checking


class HostSpec(object):
    """
    Information about hosts. Like e.g. ``kubectl get nodes``
    """
    def __init__(self,
                 hostname,  # type: str
                 addr=None,  # type: Optional[str]
                 labels=None,  # type: Optional[List[str]]
                 status=None,  # type: Optional[str]
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

    def to_json(self):
        return {
            'hostname': self.hostname,
            'addr': self.addr,
            'labels': self.labels,
            'status': self.status,
        }

    @classmethod
    def from_json(cls, host_spec):
        _cls = cls(host_spec['hostname'],
                   host_spec['addr'] if 'addr' in host_spec else None,
                   host_spec['labels'] if 'labels' in host_spec else None)
        return _cls

    def __repr__(self):
        args = [self.hostname]  # type: List[Any]
        if self.addr is not None:
            args.append(self.addr)
        if self.labels:
            args.append(self.labels)
        if self.status:
            args.append(self.status)

        return "HostSpec({})".format(', '.join(map(repr, args)))

    def __str__(self):
        if self.hostname != self.addr:
            return f'{self.hostname} ({self.addr})'
        return self.hostname

    def __eq__(self, other):
        # Let's omit `status` for the moment, as it is still the very same host.
        return self.hostname == other.hostname and \
               self.addr == other.addr and \
               self.labels == other.labels
