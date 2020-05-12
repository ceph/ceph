import logging
import random
from typing import List, Optional, Callable

import orchestrator
from ceph.deployment.service_spec import PlacementSpec, HostPlacementSpec, ServiceSpec
from orchestrator import OrchestratorValidationError

logger = logging.getLogger(__name__)

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
        # type: (List, Optional[int]) -> List[HostPlacementSpec]
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
        # type: (List, Optional[int]) -> List[HostPlacementSpec]
        if not host_pool:
            return []
        host_pool = [x for x in host_pool]
        # shuffle for pseudo random selection
        random.shuffle(host_pool)
        return host_pool[:count]


class HostAssignment(object):
    """
    A class to detect if hosts are being passed imperative or declarative
    If the spec is populated via the `hosts/hosts` field it will not load
    any hosts into the list.
    If the spec isn't populated, i.e. when only num or label is present (declarative)
    it will use the provided `get_host_func` to load it from the inventory.

    Schedulers can be assigned to pick hosts from the pool.
    """

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
        self.get_daemons_func = get_daemons_func
        self.filter_new_host = filter_new_host
        self.service_name = spec.service_name()


    def validate(self):
        self.spec.validate()

        if self.spec.placement.hosts:
            explicit_hostnames = {h.hostname for h in self.spec.placement.hosts}
            unknown_hosts = explicit_hostnames.difference(set(self.get_hosts_func()))
            if unknown_hosts:
                raise OrchestratorValidationError(
                    f'Cannot place {self.spec.one_line_str()} on {unknown_hosts}: Unknown hosts')

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
        Load hosts into the spec.placement.hosts container.
        """

        self.validate()

        # count == 0
        if self.spec.placement.count == 0:
            return []

        # respect any explicit host list
        if self.spec.placement.hosts and not self.spec.placement.count:
            logger.debug('Provided hosts: %s' % self.spec.placement.hosts)
            return self.spec.placement.hosts

        # respect host_pattern
        if self.spec.placement.host_pattern:
            candidates = [
                HostPlacementSpec(x, '', '')
                for x in self.spec.placement.filter_matching_hosts(self.get_hosts_func)
            ]
            logger.debug('All hosts: {}'.format(candidates))
            return candidates

        count = 0
        if self.spec.placement.hosts and \
           self.spec.placement.count and \
           len(self.spec.placement.hosts) >= self.spec.placement.count:
            hosts = self.spec.placement.hosts
            logger.debug('place %d over provided host list: %s' % (
                count, hosts))
            count = self.spec.placement.count
        elif self.spec.placement.label:
            hosts = [
                HostPlacementSpec(x, '', '')
                for x in self.get_hosts_func(label=self.spec.placement.label)
            ]
            if not self.spec.placement.count:
                logger.debug('Labeled hosts: {}'.format(hosts))
                return hosts
            count = self.spec.placement.count
            logger.debug('place %d over label %s: %s' % (
                count, self.spec.placement.label, hosts))
        else:
            hosts = [
                HostPlacementSpec(x, '', '')
                for x in self.get_hosts_func()
            ]
            if self.spec.placement.count:
                count = self.spec.placement.count
            else:
                # this should be a totally empty spec given all of the
                # alternative paths above.
                assert self.spec.placement.count is None
                assert not self.spec.placement.hosts
                assert not self.spec.placement.label
                count = 1
            logger.debug('place %d over all hosts: %s' % (count, hosts))

        # we need to select a subset of the candidates

        # if a partial host list is provided, always start with that
        if len(self.spec.placement.hosts) < count:
            chosen = self.spec.placement.hosts
        else:
            chosen = []

        # prefer hosts that already have services
        daemons = self.get_daemons_func(self.service_name)
        hosts_with_daemons = {d.hostname for d in daemons}
        # calc existing daemons (that aren't already in chosen)
        chosen_hosts = [hs.hostname for hs in chosen]
        existing = [hs for hs in hosts
                    if hs.hostname in hosts_with_daemons and \
                    hs.hostname not in chosen_hosts]
        if len(chosen + existing) >= count:
            chosen = chosen + self.scheduler.place(
                existing,
                count - len(chosen))
            logger.debug('Hosts with existing daemons: {}'.format(chosen))
            return chosen

        need = count - len(existing + chosen)
        others = [hs for hs in hosts
                  if hs.hostname not in hosts_with_daemons]
        if self.filter_new_host:
            old = others
            others = [h for h in others if self.filter_new_host(h.hostname)]
            logger.debug('filtered %s down to %s' % (old, hosts))
        chosen = chosen + self.scheduler.place(others, need)
        logger.debug('Combine hosts with existing daemons %s + new hosts %s' % (
            existing, chosen))
        return existing + chosen
