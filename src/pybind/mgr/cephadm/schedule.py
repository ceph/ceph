import logging
import random
from typing import List, Optional, Callable, Iterable, Tuple, TypeVar, Set

import orchestrator
from ceph.deployment.service_spec import PlacementSpec, HostPlacementSpec, ServiceSpec
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
    def __init__(self, spec):
        super(SimpleScheduler, self).__init__(spec)

    def place(self, host_pool, count=None):
        # type: (List[T], Optional[int]) -> List[T]
        if not host_pool:
            return []
        host_pool = [x for x in host_pool]
        # gen seed off of self.spec to make shuffling deterministic
        seed = hash(self.spec.service_name())
        # shuffle for pseudo random selection
        random.Random(seed).shuffle(host_pool)
        return host_pool[:count]


class HostAssignment(object):

    def __init__(self,
                 spec,  # type: ServiceSpec
                 get_hosts_func,  # type: Callable
                 get_daemons_func, # type: Callable[[str],List[orchestrator.DaemonDescription]]
                 filter_new_host=None, # type: Optional[Callable[[str],bool]]
                 scheduler=None,  # type: Optional[BaseScheduler]
                 ):
        assert spec and get_hosts_func and get_daemons_func
        self.spec = spec  # type: ServiceSpec
        self.scheduler = scheduler if scheduler else SimpleScheduler(self.spec)
        self.get_hosts_func = get_hosts_func
        self.filter_new_host = filter_new_host
        self.service_name = spec.service_name()
        self.daemons = get_daemons_func(self.service_name)

    def validate(self):
        self.spec.validate()

        if self.spec.placement.count == 0:
            raise OrchestratorValidationError(
                f'<count> can not be 0 for {self.spec.one_line_str()}')

        if self.spec.placement.hosts:
            explicit_hostnames = {h.hostname for h in self.spec.placement.hosts}
            unknown_hosts = explicit_hostnames.difference(set(self.get_hosts_func()))
            if unknown_hosts:
                raise OrchestratorValidationError(
                    f'Cannot place {self.spec.one_line_str()} on {", ".join(sorted(unknown_hosts))}: Unknown hosts')

        if self.spec.placement.host_pattern:
            pattern_hostnames = self.spec.placement.filter_matching_hosts(self.get_hosts_func)
            if not pattern_hostnames:
                raise OrchestratorValidationError(
                    f'Cannot place {self.spec.one_line_str()}: No matching hosts')

        if self.spec.placement.label:
            label_hostnames = self.get_hosts_func(label=self.spec.placement.label)
            if not label_hostnames:
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

        # get candidates based on [hosts, label, host_pattern]
        candidates = self.get_candidates()

        # If we don't have <count> the list of candidates is definitive.
        if count is None:
            logger.debug('Provided hosts: %s' % candidates)
            return candidates

        # prefer hosts that already have services.
        # this avoids re-assigning to _new_ hosts
        # and constant re-distribution of hosts when new nodes are
        # added to the cluster
        hosts_with_daemons = self.hosts_with_daemons(candidates)

        # The amount of hosts that need to be selected in order to fulfill count.
        need = count - len(hosts_with_daemons)

        # hostspecs that are do not have daemons on them but are still candidates.
        others = difference_hostspecs(candidates, hosts_with_daemons)

        # we don't need any additional hosts
        if need < 0:
            # ask the scheduler to return a set of hosts with a up to the value of <count>
            return self.scheduler.place(hosts_with_daemons, count)
        else:
            # exclusive to 'mon' daemons. Filter out hosts that don't have a public network assigned
            if self.filter_new_host:
                old = others
                others = [h for h in others if self.filter_new_host(h.hostname)]
                logger.debug('filtered %s down to %s' % (old, candidates))

            # ask the scheduler to return a set of hosts with a up to the value of <count>
            others = self.scheduler.place(others, need)
            logger.debug('Combine hosts with existing daemons %s + new hosts %s' % (
                hosts_with_daemons, others))
            # if a host already has the anticipated daemon, merge it with the candidates
            # to get a list of HostPlacementSpec that can be deployed on.
            return list(merge_hostspecs(hosts_with_daemons, others))

    def add_daemon_hosts(self, host_pool: List[HostPlacementSpec]) -> Set[HostPlacementSpec]:
        hosts_with_daemons = {d.hostname for d in self.daemons}
        _add_daemon_hosts = set()
        for host in host_pool:
            if host.hostname not in hosts_with_daemons:
                _add_daemon_hosts.add(host)
        return _add_daemon_hosts

    def remove_daemon_hosts(self, host_pool: List[HostPlacementSpec]) -> Set[DaemonDescription]:
        target_hosts = [h.hostname for h in host_pool]
        _remove_daemon_hosts = set()
        for d in self.daemons:
            if d.hostname not in target_hosts:
                _remove_daemon_hosts.add(d)
        return _remove_daemon_hosts

    def get_candidates(self) -> List[HostPlacementSpec]:
        if self.spec.placement.hosts:
            return self.spec.placement.hosts
        elif self.spec.placement.label:
            return [
                HostPlacementSpec(x, '', '')
                for x in self.get_hosts_func(label=self.spec.placement.label)
            ]
        elif self.spec.placement.host_pattern:
            return [
                HostPlacementSpec(x, '', '')
                for x in self.spec.placement.filter_matching_hosts(self.get_hosts_func)
            ]
        # If none of the above and also no <count>
        if self.spec.placement.count is None:
            raise OrchestratorValidationError("placement spec is empty: no hosts, no label, no pattern, no count")
        # backward compatibility: consider an empty placements to be the same pattern = *
        return [
            HostPlacementSpec(x, '', '')
            for x in self.get_hosts_func()
        ]

    def hosts_with_daemons(self, candidates: List[HostPlacementSpec]) -> List[HostPlacementSpec]:
        """
        Prefer hosts with daemons. Otherwise we'll constantly schedule daemons
        on different hosts all the time. This is about keeping daemons where
        they are. This isn't about co-locating.
        """
        hosts_with_daemons = {d.hostname for d in self.daemons}

        # calc existing daemons (that aren't already in chosen)
        existing = [hs for hs in candidates if hs.hostname in hosts_with_daemons]

        logger.debug('Hosts with existing daemons: {}'.format(existing))
        return existing


def merge_hostspecs(l: List[HostPlacementSpec], r: List[HostPlacementSpec]) -> Iterable[HostPlacementSpec]:
    """
    Merge two lists of HostPlacementSpec by hostname. always returns `l` first.

    >>> list(merge_hostspecs([HostPlacementSpec(hostname='h', name='x', network='')],
    ...                      [HostPlacementSpec(hostname='h', name='y', network='')]))
    [HostPlacementSpec(hostname='h', network='', name='x')]

    """
    l_names = {h.hostname for h in l}
    yield from l
    yield from (h for h in r if h.hostname not in l_names)


def difference_hostspecs(l: List[HostPlacementSpec], r: List[HostPlacementSpec]) -> List[HostPlacementSpec]:
    """
    returns l "minus" r by hostname.

    >>> list(difference_hostspecs([HostPlacementSpec(hostname='h1', name='x', network=''),
    ...                           HostPlacementSpec(hostname='h2', name='y', network='')],
    ...                           [HostPlacementSpec(hostname='h2', name='', network='')]))
    [HostPlacementSpec(hostname='h1', network='', name='x')]

    """
    r_names = {h.hostname for h in r}
    return [h for h in l if h.hostname not in r_names]


