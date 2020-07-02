import logging
import random
from typing import List, Optional, Callable, Iterable, Tuple, TypeVar

import orchestrator
from ceph.deployment.service_spec import PlacementSpec, HostPlacementSpec, ServiceSpec
from orchestrator import OrchestratorValidationError

logger = logging.getLogger(__name__)
T = TypeVar('T')

class BaseScheduler(object):
    """
    Base Scheduler Interface

    * requires a placement_spec

    `place(host_pool)` needs to return a List[HostPlacementSpec, ..]
    """

    def __init__(self, placement_spec):
        # type: (PlacementSpec) -> None
        self.placement_spec = placement_spec

    def place(self, host_pool, count=None):
        # type: (List[T], Optional[int]) -> List[T]
        raise NotImplementedError


class SimpleScheduler(BaseScheduler):
    """
    The most simple way to pick/schedule a set of hosts.
    1) Shuffle the provided host_pool
    2) Select from list up to :count
    """
    def __init__(self, placement_spec):
        super(SimpleScheduler, self).__init__(placement_spec)

    def place(self, host_pool, count=None):
        # type: (List[T], Optional[int]) -> List[T]
        if not host_pool:
            return []
        host_pool = [x for x in host_pool]
        # shuffle for pseudo random selection
        random.shuffle(host_pool)
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
        self.scheduler = scheduler if scheduler else SimpleScheduler(self.spec.placement)
        self.get_hosts_func = get_hosts_func
        self.filter_new_host = filter_new_host
        self.service_name = spec.service_name()
        self.daemons = get_daemons_func(self.service_name)

    def validate(self):
        self.spec.validate()

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
        assert count != 0

        chosen = self.get_candidates()
        if count is None:
            logger.debug('Provided hosts: %s' % self.spec.placement.hosts)
            return chosen

        # prefer hosts that already have services
        existing = self.hosts_with_daemons(chosen)

        need = count - len(existing)
        others = difference_hostspecs(chosen, existing)

        if need < 0:
            return self.scheduler.place(existing, count)
        else:
            if self.filter_new_host:
                old = others
                others = [h for h in others if self.filter_new_host(h.hostname)]
                logger.debug('filtered %s down to %s' % (old, chosen))

            others = self.scheduler.place(others, need)
            logger.debug('Combine hosts with existing daemons %s + new hosts %s' % (
                existing, others))
            return list(merge_hostspecs(existing, others))

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
        if self.spec.placement.count is None:
            raise OrchestratorValidationError("placement spec is empty: no hosts, no label, no pattern, no count")
        # backward compatibility: consider an empty placements to be the same pattern = *
        return [
            HostPlacementSpec(x, '', '')
            for x in self.get_hosts_func()
        ]

    def hosts_with_daemons(self, chosen: List[HostPlacementSpec]) -> List[HostPlacementSpec]:
        """
        Prefer hosts with daemons. Otherwise we'll constantly schedule daemons
        on different hosts all the time. This is about keeping daemons where
        they are. This isn't about co-locating.
        """
        hosts = {d.hostname for d in self.daemons}

        # calc existing daemons (that aren't already in chosen)
        existing = [hs for hs in chosen if hs.hostname in hosts]

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


