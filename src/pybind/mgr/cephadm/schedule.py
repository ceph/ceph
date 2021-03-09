import logging
import random
from typing import List, Optional, Callable, Iterable, TypeVar, Set

import orchestrator
from ceph.deployment.service_spec import HostPlacementSpec, ServiceSpec
from orchestrator._interface import DaemonDescription
from orchestrator import OrchestratorValidationError

logger = logging.getLogger(__name__)
T = TypeVar('T')


class BaseScheduler(object):
    """
    Base Scheduler Interface

    * requires a ServiceSpec

    `place(host_pool)` needs to return a List[HostPlacementSpec, ..]
    """

    def __init__(self, spec):
        # type: (ServiceSpec) -> None
        self.spec = spec

    def place(self, host_pool, count=None):
        # type: (List[T], Optional[int]) -> List[T]
        raise NotImplementedError


class SimpleScheduler(BaseScheduler):
    """
    The most simple way to pick/schedule a set of hosts.
    1) Shuffle the provided host_pool
    2) Select from list up to :count
    """

    def __init__(self, spec: ServiceSpec):
        super(SimpleScheduler, self).__init__(spec)

    def place(self, host_pool, count=None):
        # type: (List[T], Optional[int]) -> List[T]
        if not host_pool:
            return []
        host_pool = [x for x in host_pool]
        return host_pool[:count]


class HostAssignment(object):

    def __init__(self,
                 spec,  # type: ServiceSpec
                 hosts: List[orchestrator.HostSpec],
                 daemons: List[orchestrator.DaemonDescription],
                 filter_new_host=None,  # type: Optional[Callable[[str],bool]]
                 scheduler=None,  # type: Optional[BaseScheduler]
                 allow_colo: bool = False,
                 ):
        assert spec
        self.spec = spec  # type: ServiceSpec
        self.scheduler = scheduler if scheduler else SimpleScheduler(self.spec)
        self.hosts: List[orchestrator.HostSpec] = hosts
        self.filter_new_host = filter_new_host
        self.service_name = spec.service_name()
        self.daemons = daemons
        self.allow_colo = allow_colo

    def hosts_by_label(self, label: str) -> List[orchestrator.HostSpec]:
        return [h for h in self.hosts if label in h.labels]

    def get_hostnames(self) -> List[str]:
        return [h.hostname for h in self.hosts]

    def validate(self) -> None:
        self.spec.validate()

        if self.spec.placement.count == 0:
            raise OrchestratorValidationError(
                f'<count> can not be 0 for {self.spec.one_line_str()}')

        if (
                self.spec.placement.count_per_host is not None
                and self.spec.placement.count_per_host > 1
                and not self.allow_colo
        ):
            raise OrchestratorValidationError(
                f'Cannot place more than one {self.spec.service_type} per host'
            )

        if self.spec.placement.hosts:
            explicit_hostnames = {h.hostname for h in self.spec.placement.hosts}
            unknown_hosts = explicit_hostnames.difference(set(self.get_hostnames()))
            if unknown_hosts:
                raise OrchestratorValidationError(
                    f'Cannot place {self.spec.one_line_str()} on {", ".join(sorted(unknown_hosts))}: Unknown hosts')

        if self.spec.placement.host_pattern:
            pattern_hostnames = self.spec.placement.filter_matching_hostspecs(self.hosts)
            if not pattern_hostnames:
                raise OrchestratorValidationError(
                    f'Cannot place {self.spec.one_line_str()}: No matching hosts')

        if self.spec.placement.label:
            label_hosts = self.hosts_by_label(self.spec.placement.label)
            if not label_hosts:
                raise OrchestratorValidationError(
                    f'Cannot place {self.spec.one_line_str()}: No matching '
                    f'hosts for label {self.spec.placement.label}')

    def place(self):
        # type: () -> List[HostPlacementSpec]
        """
        Generate a list of HostPlacementSpec taking into account:

        * all known hosts
        * hosts with existing daemons
        * placement spec
        * self.filter_new_host
        """

        self.validate()

        count = self.spec.placement.count

        # get candidate hosts based on [hosts, label, host_pattern]
        candidates = self.get_candidates()  # type: List[HostPlacementSpec]
        if not candidates:
            return []  # sigh

        # If we don't have <count> the list of candidates is definitive.
        if count is None:
            logger.debug('Provided hosts: %s' % candidates)
            if self.spec.placement.count_per_host:
                per_host = self.spec.placement.count_per_host
            else:
                per_host = 1
            return candidates * per_host

        if self.allow_colo:
            # consider enough slots to fit target count
            per_host = 1 + ((count - 1) // len(candidates))
            candidates = candidates * per_host

        # consider active daemons first
        daemons = [
            d for d in self.daemons if d.is_active
        ] + [
            d for d in self.daemons if not d.is_active
        ]

        # sort candidates into existing/used slots that already have a
        # daemon, and others (the rest)
        existing_active: List[HostPlacementSpec] = []
        existing_standby: List[HostPlacementSpec] = []
        existing: List[HostPlacementSpec] = []
        others = candidates
        for d in daemons:
            hs = d.get_host_placement()
            for i in others:
                if i == hs:
                    others.remove(i)
                    if d.is_active:
                        existing_active.append(hs)
                    else:
                        existing_standby.append(hs)
                    existing.append(hs)
                    break
        logger.debug('Hosts with existing active daemons: {}'.format(existing_active))
        logger.debug('Hosts with existing standby daemons: {}'.format(existing_standby))

        # The number of new slots that need to be selected in order to fulfill count
        need = count - len(existing)

        # we don't need any additional placements
        if need <= 0:
            del existing[count:]
            return existing

        # ask the scheduler to select additional slots
        chosen = self.scheduler.place(others, need)
        logger.debug('Combine hosts with existing daemons %s + new hosts %s' % (
            existing, chosen))
        return existing + chosen

    def add_daemon_hosts(self, host_pool: List[HostPlacementSpec]) -> List[HostPlacementSpec]:
        hosts_with_daemons = {d.hostname for d in self.daemons}
        _add_daemon_hosts = []  # type: List[HostPlacementSpec]
        for host in host_pool:
            if host.hostname not in hosts_with_daemons:
                _add_daemon_hosts.append(host)
        return _add_daemon_hosts

    def remove_daemon_hosts(self, host_pool: List[HostPlacementSpec]) -> Set[DaemonDescription]:
        target_hosts = [h.hostname for h in host_pool]
        _remove_daemon_hosts = set()
        for d in self.daemons:
            if d.hostname not in target_hosts:
                _remove_daemon_hosts.add(d)
            else:
                target_hosts.remove(d.hostname)
        return _remove_daemon_hosts

    def get_candidates(self) -> List[HostPlacementSpec]:
        if self.spec.placement.hosts:
            hosts = self.spec.placement.hosts
        elif self.spec.placement.label:
            hosts = [
                HostPlacementSpec(x.hostname, '', '')
                for x in self.hosts_by_label(self.spec.placement.label)
            ]
        elif self.spec.placement.host_pattern:
            hosts = [
                HostPlacementSpec(x, '', '')
                for x in self.spec.placement.filter_matching_hostspecs(self.hosts)
            ]
        # If none of the above and also no <count>
        elif self.spec.placement.count is not None:
            # backward compatibility: consider an empty placements to be the same pattern = *
            hosts = [
                HostPlacementSpec(x.hostname, '', '')
                for x in self.hosts
            ]
        else:
            raise OrchestratorValidationError(
                "placement spec is empty: no hosts, no label, no pattern, no count")

        if self.filter_new_host:
            old = hosts.copy()
            hosts = [h for h in hosts if self.filter_new_host(h.hostname)]
            for h in list(set(old) - set(hosts)):
                logger.info(
                    f"Filtered out host {h.hostname}: could not verify host allowed virtual ips")
                logger.debug('Filtered %s down to %s' % (old, hosts))

        # shuffle for pseudo random selection
        # gen seed off of self.spec to make shuffling deterministic
        seed = hash(self.spec.service_name())
        random.Random(seed).shuffle(hosts)

        return hosts


def merge_hostspecs(
        lh: List[HostPlacementSpec],
        rh: List[HostPlacementSpec]
) -> Iterable[HostPlacementSpec]:
    """
    Merge two lists of HostPlacementSpec by hostname. always returns `lh` first.

    >>> list(merge_hostspecs([HostPlacementSpec(hostname='h', name='x', network='')],
    ...                      [HostPlacementSpec(hostname='h', name='y', network='')]))
    [HostPlacementSpec(hostname='h', network='', name='x')]

    """
    lh_names = {h.hostname for h in lh}
    yield from lh
    yield from (h for h in rh if h.hostname not in lh_names)


def difference_hostspecs(
        lh: List[HostPlacementSpec],
        rh: List[HostPlacementSpec]
) -> List[HostPlacementSpec]:
    """
    returns lh "minus" rh by hostname.

    >>> list(difference_hostspecs([HostPlacementSpec(hostname='h1', name='x', network=''),
    ...                           HostPlacementSpec(hostname='h2', name='y', network='')],
    ...                           [HostPlacementSpec(hostname='h2', name='', network='')]))
    [HostPlacementSpec(hostname='h1', network='', name='x')]

    """
    rh_names = {h.hostname for h in rh}
    return [h for h in lh if h.hostname not in rh_names]
