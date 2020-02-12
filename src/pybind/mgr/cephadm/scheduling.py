import logging
import random
from typing import List, Optional, Callable

import orchestrator
from orchestrator import HostPlacementSpec, OrchestratorValidationError

logger = logging.getLogger(__name__)


class BaseScheduler(object):
    """
    Base Scheduler Interface

    * requires a placement_spec

    `place(host_pool)` needs to return a List[HostPlacementSpec, ..]
    """

    def __init__(self, placement_spec):
        # type: (orchestrator.PlacementSpec) -> None
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
            raise Exception('List of host candidates is empty')
        host_pool = [HostPlacementSpec(x, '', '') for x in host_pool]
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
                 spec=None,  # type: Optional[orchestrator.ServiceSpec]
                 scheduler=None,  # type: Optional[BaseScheduler]
                 get_hosts_func=None,  # type: Optional[Callable[[Optional[str]],List[str]]]
                 service_type=None,  # type: Optional[str]
                 ):
        assert spec and get_hosts_func and service_type
        self.spec = spec  # type: orchestrator.ServiceSpec
        self.scheduler = scheduler if scheduler else SimpleScheduler(self.spec.placement)
        self.get_hosts_func = get_hosts_func
        self.daemon_type = service_type

    def load(self):
        # type: () -> orchestrator.ServiceSpec
        """
        Load hosts into the spec.placement.hosts container.
        """
        self.load_labeled_hosts()
        self.assign_hosts()
        return self.spec

    def load_labeled_hosts(self):
        # type: () -> None
        """
        Assign hosts based on their label
        """
        if self.spec.placement.label:
            logger.info("Matching label '%s'" % self.spec.placement.label)
            candidates = [
                HostPlacementSpec(x, '', '')
                for x in self.get_hosts_func(self.spec.placement.label)
            ]
            logger.info('Assigning hostss to spec: {}'.format(candidates))
            self.spec.placement.set_hosts(candidates)

    def assign_hosts(self):
        # type: () -> None
        """
        Use the assigned scheduler to load hosts into the spec.placement.hosts container
        """
        # If no imperative or declarative host assignments, use the scheduler to pick from the
        # host pool (assuming `count` is set)
        if not self.spec.placement.label and not self.spec.placement.hosts and self.spec.placement.count:
            logger.info("Found num spec. Looking for labeled hosts.")
            # TODO: actually query for labels (self.daemon_type)
            candidates = self.scheduler.place([x for x in self.get_hosts_func(None)],
                                              count=self.spec.placement.count)
            # Not enough hosts to deploy on
            if len(candidates) != self.spec.placement.count:
                logger.warning("Did not find enough labeled hosts to \
                               scale to <{}> services. Falling back to unlabeled hosts.".
                               format(self.spec.placement.count))
            else:
                logger.info('Assigning hosts to spec: {}'.format(candidates))
                self.spec.placement.set_hosts(candidates)
                return None

            candidates = self.scheduler.place([x for x in self.get_hosts_func(None)], count=self.spec.placement.count)
            # Not enough hosts to deploy on
            if len(candidates) != self.spec.placement.count:
                raise OrchestratorValidationError("Cannot place {} daemons on {} hosts.".
                                                  format(self.spec.placement.count, len(candidates)))

            logger.info('Assigning hosts to spec: {}'.format(candidates))
            self.spec.placement.set_hosts(candidates)
            return None
