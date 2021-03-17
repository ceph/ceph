import logging
import random
from typing import List, Optional, Callable, TypeVar, Tuple, NamedTuple, Dict

import orchestrator
from ceph.deployment.service_spec import ServiceSpec
from orchestrator._interface import DaemonDescription
from orchestrator import OrchestratorValidationError

logger = logging.getLogger(__name__)
T = TypeVar('T')


class DaemonPlacement(NamedTuple):
    hostname: str
    network: str = ''   # for mons only
    name: str = ''
    ip: Optional[str] = None
    port: Optional[int] = None

    def __str__(self) -> str:
        res = self.hostname
        other = []
        if self.network:
            other.append(f'network={self.network}')
        if self.name:
            other.append(f'name={self.name}')
        if self.port:
            other.append(f'{self.ip or "*"}:{self.port}')
        if other:
            res += '(' + ' '.join(other) + ')'
        return res

    def renumber_port(self, n: int) -> 'DaemonPlacement':
        return DaemonPlacement(
            self.hostname,
            self.network,
            self.name,
            self.ip,
            (self.port + n) if self.port is not None else None
        )

    def matches_daemon(self, dd: DaemonDescription) -> bool:
        if self.hostname != dd.hostname:
            return False
        # fixme: how to match against network?
        if self.name and self.name != dd.daemon_id:
            return False
        if self.port:
            if [self.port] != dd.ports:
                return False
            if self.ip != dd.ip:
                return False
        return True


class HostAssignment(object):

    def __init__(self,
                 spec,  # type: ServiceSpec
                 hosts: List[orchestrator.HostSpec],
                 daemons: List[orchestrator.DaemonDescription],
                 networks: Dict[str, Dict[str, List[str]]] = {},
                 filter_new_host=None,  # type: Optional[Callable[[str],bool]]
                 allow_colo: bool = False,
                 ):
        assert spec
        self.spec = spec  # type: ServiceSpec
        self.hosts: List[orchestrator.HostSpec] = hosts
        self.filter_new_host = filter_new_host
        self.service_name = spec.service_name()
        self.daemons = daemons
        self.networks = networks
        self.allow_colo = allow_colo
        self.port_start = spec.get_port_start()

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
        # type: () -> Tuple[List[DaemonPlacement], List[DaemonPlacement], List[orchestrator.DaemonDescription]]
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
        candidates = self.get_candidates()  # type: List[DaemonPlacement]

        def expand_candidates(ls: List[DaemonPlacement], num: int) -> List[DaemonPlacement]:
            r = []
            for offset in range(num):
                r.extend([dp.renumber_port(offset) for dp in ls])
            return r

        # consider enough slots to fulfill target count-per-host or count
        if count is None:
            if self.spec.placement.count_per_host:
                per_host = self.spec.placement.count_per_host
            else:
                per_host = 1
            candidates = expand_candidates(candidates, per_host)
        elif self.allow_colo and candidates:
            per_host = 1 + ((count - 1) // len(candidates))
            candidates = expand_candidates(candidates, per_host)

        # consider active daemons first
        daemons = [
            d for d in self.daemons if d.is_active
        ] + [
            d for d in self.daemons if not d.is_active
        ]

        # sort candidates into existing/used slots that already have a
        # daemon, and others (the rest)
        existing_active: List[orchestrator.DaemonDescription] = []
        existing_standby: List[orchestrator.DaemonDescription] = []
        existing_slots: List[DaemonPlacement] = []
        to_remove: List[orchestrator.DaemonDescription] = []
        others = candidates.copy()
        for dd in daemons:
            found = False
            for p in others:
                if p.matches_daemon(dd):
                    others.remove(p)
                    if dd.is_active:
                        existing_active.append(dd)
                    else:
                        existing_standby.append(dd)
                    existing_slots.append(p)
                    found = True
                    break
            if not found:
                to_remove.append(dd)

        existing = existing_active + existing_standby

        # If we don't have <count> the list of candidates is definitive.
        if count is None:
            logger.debug('Provided hosts: %s' % candidates)
            return candidates, others, to_remove

        # The number of new slots that need to be selected in order to fulfill count
        need = count - len(existing)

        # we don't need any additional placements
        if need <= 0:
            to_remove.extend(existing[count:])
            del existing_slots[count:]
            return existing_slots, [], to_remove

        # ask the scheduler to select additional slots
        to_add = others[:need]
        logger.debug('Combine hosts with existing daemons %s + new hosts %s' % (
            existing, to_add))
        return existing_slots + to_add, to_add, to_remove

    def find_ip_on_host(self, hostname: str, subnets: List[str]) -> Optional[str]:
        for subnet in subnets:
            ips = self.networks.get(hostname, {}).get(subnet, [])
            if ips:
                return sorted(ips)[0]
        return None

    def get_candidates(self) -> List[DaemonPlacement]:
        if self.spec.placement.hosts:
            ls = [
                DaemonPlacement(hostname=h.hostname, network=h.network, name=h.name,
                                port=self.port_start)
                for h in self.spec.placement.hosts
            ]
        elif self.spec.placement.label:
            ls = [
                DaemonPlacement(hostname=x.hostname, port=self.port_start)
                for x in self.hosts_by_label(self.spec.placement.label)
            ]
        elif self.spec.placement.host_pattern:
            ls = [
                DaemonPlacement(hostname=x, port=self.port_start)
                for x in self.spec.placement.filter_matching_hostspecs(self.hosts)
            ]
        elif (
                self.spec.placement.count is not None
                or self.spec.placement.count_per_host is not None
        ):
            ls = [
                DaemonPlacement(hostname=x.hostname, port=self.port_start)
                for x in self.hosts
            ]
        else:
            raise OrchestratorValidationError(
                "placement spec is empty: no hosts, no label, no pattern, no count")

        # allocate an IP?
        if self.spec.networks:
            orig = ls.copy()
            ls = []
            for p in orig:
                ip = self.find_ip_on_host(p.hostname, self.spec.networks)
                if ip:
                    ls.append(DaemonPlacement(hostname=p.hostname, network=p.network,
                                              name=p.name, port=p.port, ip=ip))
                else:
                    logger.debug(
                        f'Skipping {p.hostname} with no IP in network(s) {self.spec.networks}'
                    )

        if self.filter_new_host:
            old = ls.copy()
            ls = [h for h in ls if self.filter_new_host(h.hostname)]
            for h in list(set(old) - set(ls)):
                logger.info(
                    f"Filtered out host {h.hostname}: could not verify host allowed virtual ips")
                logger.debug('Filtered %s down to %s' % (old, ls))

        # shuffle for pseudo random selection
        # gen seed off of self.spec to make shuffling deterministic
        seed = hash(self.spec.service_name())
        random.Random(seed).shuffle(ls)

        return ls
