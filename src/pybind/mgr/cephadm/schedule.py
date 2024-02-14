import ipaddress
import hashlib
import logging
import random
from typing import List, Optional, Callable, TypeVar, Tuple, NamedTuple, Dict

import orchestrator
from ceph.deployment.service_spec import ServiceSpec
from orchestrator._interface import DaemonDescription
from orchestrator import OrchestratorValidationError
from .utils import RESCHEDULE_FROM_OFFLINE_HOSTS_TYPES

logger = logging.getLogger(__name__)
T = TypeVar('T')


class DaemonPlacement(NamedTuple):
    daemon_type: str
    hostname: str
    network: str = ''   # for mons only
    name: str = ''
    ip: Optional[str] = None
    ports: List[int] = []
    rank: Optional[int] = None
    rank_generation: Optional[int] = None

    def __str__(self) -> str:
        res = self.daemon_type + ':' + self.hostname
        other = []
        if self.rank is not None:
            other.append(f'rank={self.rank}.{self.rank_generation}')
        if self.network:
            other.append(f'network={self.network}')
        if self.name:
            other.append(f'name={self.name}')
        if self.ports:
            other.append(f'{self.ip or "*"}:{",".join(map(str, self.ports))}')
        if other:
            res += '(' + ' '.join(other) + ')'
        return res

    def renumber_ports(self, n: int) -> 'DaemonPlacement':
        return DaemonPlacement(
            self.daemon_type,
            self.hostname,
            self.network,
            self.name,
            self.ip,
            [p + n for p in self.ports],
            self.rank,
            self.rank_generation,
        )

    def assign_rank(self, rank: int, gen: int) -> 'DaemonPlacement':
        return DaemonPlacement(
            self.daemon_type,
            self.hostname,
            self.network,
            self.name,
            self.ip,
            self.ports,
            rank,
            gen,
        )

    def assign_name(self, name: str) -> 'DaemonPlacement':
        return DaemonPlacement(
            self.daemon_type,
            self.hostname,
            self.network,
            name,
            self.ip,
            self.ports,
            self.rank,
            self.rank_generation,
        )

    def assign_rank_generation(
            self,
            rank: int,
            rank_map: Dict[int, Dict[int, Optional[str]]]
    ) -> 'DaemonPlacement':
        if rank not in rank_map:
            rank_map[rank] = {}
            gen = 0
        else:
            gen = max(rank_map[rank].keys()) + 1
        rank_map[rank][gen] = None
        return DaemonPlacement(
            self.daemon_type,
            self.hostname,
            self.network,
            self.name,
            self.ip,
            self.ports,
            rank,
            gen,
        )

    def matches_daemon(self, dd: DaemonDescription) -> bool:
        if self.daemon_type != dd.daemon_type:
            return False
        if self.hostname != dd.hostname:
            return False
        # fixme: how to match against network?
        if self.name and self.name != dd.daemon_id:
            return False
        if self.ports:
            if self.ports != dd.ports and dd.ports:
                return False
            if self.ip != dd.ip and dd.ip:
                return False
        return True

    def matches_rank_map(
            self,
            dd: DaemonDescription,
            rank_map: Optional[Dict[int, Dict[int, Optional[str]]]],
            ranks: List[int]
    ) -> bool:
        if rank_map is None:
            # daemon should have no rank
            return dd.rank is None

        if dd.rank is None:
            return False

        if dd.rank not in rank_map:
            return False
        if dd.rank not in ranks:
            return False

        # must be the highest/newest rank_generation
        if dd.rank_generation != max(rank_map[dd.rank].keys()):
            return False

        # must be *this* daemon
        return rank_map[dd.rank][dd.rank_generation] == dd.daemon_id


class HostAssignment(object):

    def __init__(self,
                 spec: ServiceSpec,
                 hosts: List[orchestrator.HostSpec],
                 unreachable_hosts: List[orchestrator.HostSpec],
                 draining_hosts: List[orchestrator.HostSpec],
                 daemons: List[orchestrator.DaemonDescription],
                 related_service_daemons: Optional[List[DaemonDescription]] = None,
                 networks: Dict[str, Dict[str, Dict[str, List[str]]]] = {},
                 filter_new_host: Optional[Callable[[str], bool]] = None,
                 allow_colo: bool = False,
                 primary_daemon_type: Optional[str] = None,
                 per_host_daemon_type: Optional[str] = None,
                 rank_map: Optional[Dict[int, Dict[int, Optional[str]]]] = None,
                 ):
        assert spec
        self.spec = spec  # type: ServiceSpec
        self.primary_daemon_type = primary_daemon_type or spec.service_type
        self.hosts: List[orchestrator.HostSpec] = hosts
        self.unreachable_hosts: List[orchestrator.HostSpec] = unreachable_hosts
        self.draining_hosts: List[orchestrator.HostSpec] = draining_hosts
        self.filter_new_host = filter_new_host
        self.service_name = spec.service_name()
        self.daemons = daemons
        self.related_service_daemons = related_service_daemons
        self.networks = networks
        self.allow_colo = allow_colo
        self.per_host_daemon_type = per_host_daemon_type
        self.ports_start = spec.get_port_start()
        self.rank_map = rank_map

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
            known_hosts = self.get_hostnames() + [h.hostname for h in self.draining_hosts]
            unknown_hosts = explicit_hostnames.difference(set(known_hosts))
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

    def place_per_host_daemons(
            self,
            slots: List[DaemonPlacement],
            to_add: List[DaemonPlacement],
            to_remove: List[orchestrator.DaemonDescription],
    ) -> Tuple[List[DaemonPlacement], List[DaemonPlacement], List[orchestrator.DaemonDescription]]:
        if self.per_host_daemon_type:
            host_slots = [
                DaemonPlacement(daemon_type=self.per_host_daemon_type,
                                hostname=hostname)
                for hostname in set([s.hostname for s in slots])
            ]
            existing = [
                d for d in self.daemons if d.daemon_type == self.per_host_daemon_type
            ]
            slots += host_slots
            for dd in existing:
                found = False
                for p in host_slots:
                    if p.matches_daemon(dd):
                        host_slots.remove(p)
                        found = True
                        break
                if not found:
                    to_remove.append(dd)
            to_add += host_slots

        to_remove = [d for d in to_remove if d.hostname not in [
            h.hostname for h in self.unreachable_hosts]]

        return slots, to_add, to_remove

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
        if self.primary_daemon_type in RESCHEDULE_FROM_OFFLINE_HOSTS_TYPES:
            # remove unreachable hosts that are not in maintenance so daemons
            # on these hosts will be rescheduled
            candidates = self.remove_non_maintenance_unreachable_candidates(candidates)

        def expand_candidates(ls: List[DaemonPlacement], num: int) -> List[DaemonPlacement]:
            r = []
            for offset in range(num):
                r.extend([dp.renumber_ports(offset) for dp in ls])
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

        # consider (preserve) existing daemons in a particular order...
        daemons = sorted(
            [
                d for d in self.daemons if d.daemon_type == self.primary_daemon_type
            ],
            key=lambda d: (
                not d.is_active,               # active before standby
                d.rank is not None,            # ranked first, then non-ranked
                d.rank,                        # low ranks
                0 - (d.rank_generation or 0),  # newer generations first
            )
        )

        # sort candidates into existing/used slots that already have a
        # daemon, and others (the rest)
        existing_active: List[orchestrator.DaemonDescription] = []
        existing_standby: List[orchestrator.DaemonDescription] = []
        existing_slots: List[DaemonPlacement] = []
        to_add: List[DaemonPlacement] = []
        to_remove: List[orchestrator.DaemonDescription] = []
        ranks: List[int] = list(range(len(candidates)))
        others: List[DaemonPlacement] = candidates.copy()
        for dd in daemons:
            found = False
            for p in others:
                if p.matches_daemon(dd) and p.matches_rank_map(dd, self.rank_map, ranks):
                    others.remove(p)
                    if dd.is_active:
                        existing_active.append(dd)
                    else:
                        existing_standby.append(dd)
                    if dd.rank is not None:
                        assert dd.rank_generation is not None
                        p = p.assign_rank(dd.rank, dd.rank_generation)
                        ranks.remove(dd.rank)
                    existing_slots.append(p)
                    found = True
                    break
            if not found:
                to_remove.append(dd)

        # TODO: At some point we want to deploy daemons that are on offline hosts
        # at what point we do this differs per daemon type. Stateless daemons we could
        # do quickly to improve availability. Steful daemons we might want to wait longer
        # to see if the host comes back online

        existing = existing_active + existing_standby

        # build to_add
        if not count:
            to_add = [dd for dd in others if dd.hostname not in [
                h.hostname for h in self.unreachable_hosts]]
        else:
            # The number of new slots that need to be selected in order to fulfill count
            need = count - len(existing)

            # we don't need any additional placements
            if need <= 0:
                to_remove.extend(existing[count:])
                del existing_slots[count:]
                return self.place_per_host_daemons(existing_slots, [], to_remove)

            if self.related_service_daemons:
                # prefer to put daemons on the same host(s) as daemons of the related service
                # Note that we are only doing this over picking arbitrary hosts to satisfy
                # the count. We are not breaking any deterministic placements in order to
                # match the placement with a related service.
                related_service_hosts = list(set(dd.hostname for dd in self.related_service_daemons))
                matching_dps = [dp for dp in others if dp.hostname in related_service_hosts]
                for dp in matching_dps:
                    if need <= 0:
                        break
                    if dp.hostname in related_service_hosts and dp.hostname not in [h.hostname for h in self.unreachable_hosts]:
                        logger.debug(f'Preferring {dp.hostname} for service {self.service_name} as related daemons have been placed there')
                        to_add.append(dp)
                        need -= 1  # this is last use of need so it can work as a counter
                # at this point, we've either met our placement quota entirely using hosts with related
                # service daemons, or we still need to place more. If we do need to place more,
                # we should make sure not to re-use hosts with related service daemons by filtering
                # them out from the "others" list
                if need > 0:
                    others = [dp for dp in others if dp.hostname not in related_service_hosts]

            for dp in others:
                if need <= 0:
                    break
                if dp.hostname not in [h.hostname for h in self.unreachable_hosts]:
                    to_add.append(dp)
                    need -= 1  # this is last use of need in this function so it can work as a counter

        if self.rank_map is not None:
            # assign unused ranks (and rank_generations) to to_add
            assert len(ranks) >= len(to_add)
            for i in range(len(to_add)):
                to_add[i] = to_add[i].assign_rank_generation(ranks[i], self.rank_map)

        logger.debug('Combine hosts with existing daemons %s + new hosts %s' % (existing, to_add))
        return self.place_per_host_daemons(existing_slots + to_add, to_add, to_remove)

    def find_ip_on_host(self, hostname: str, subnets: List[str]) -> Optional[str]:
        for subnet in subnets:
            ips: List[str] = []
            # following is to allow loopback interfaces for both ipv4 and ipv6. Since we
            # only have the subnet (and no IP) we assume default loopback IP address.
            if ipaddress.ip_network(subnet).is_loopback:
                if ipaddress.ip_network(subnet).version == 4:
                    ips.append('127.0.0.1')
                else:
                    ips.append('::1')
            for iface, iface_ips in self.networks.get(hostname, {}).get(subnet, {}).items():
                ips.extend(iface_ips)
            if ips:
                return sorted(ips)[0]
        return None

    def get_candidates(self) -> List[DaemonPlacement]:
        if self.spec.placement.hosts:
            ls = [
                DaemonPlacement(daemon_type=self.primary_daemon_type,
                                hostname=h.hostname, network=h.network, name=h.name,
                                ports=self.ports_start)
                for h in self.spec.placement.hosts if h.hostname not in [dh.hostname for dh in self.draining_hosts]
            ]
        elif self.spec.placement.label:
            ls = [
                DaemonPlacement(daemon_type=self.primary_daemon_type,
                                hostname=x.hostname, ports=self.ports_start)
                for x in self.hosts_by_label(self.spec.placement.label)
            ]
            if self.spec.placement.host_pattern:
                ls = [h for h in ls if h.hostname in self.spec.placement.filter_matching_hostspecs(self.hosts)]
        elif self.spec.placement.host_pattern:
            ls = [
                DaemonPlacement(daemon_type=self.primary_daemon_type,
                                hostname=x, ports=self.ports_start)
                for x in self.spec.placement.filter_matching_hostspecs(self.hosts)
            ]
        elif (
                self.spec.placement.count is not None
                or self.spec.placement.count_per_host is not None
        ):
            ls = [
                DaemonPlacement(daemon_type=self.primary_daemon_type,
                                hostname=x.hostname, ports=self.ports_start)
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
                    ls.append(DaemonPlacement(daemon_type=self.primary_daemon_type,
                                              hostname=p.hostname, network=p.network,
                                              name=p.name, ports=p.ports, ip=ip))
                else:
                    logger.debug(
                        f'Skipping {p.hostname} with no IP in network(s) {self.spec.networks}'
                    )

        if self.filter_new_host:
            old = ls.copy()
            ls = []
            for h in old:
                if self.filter_new_host(h.hostname):
                    ls.append(h)
            if len(old) > len(ls):
                logger.debug('Filtered %s down to %s' % (old, ls))

        # now that we have the list of nodes candidates based on the configured
        # placement, let's shuffle the list for node pseudo-random selection. For this,
        # we generate a seed from the service name and we use to shuffle the candidates.
        # This makes shuffling deterministic for the same service name.
        seed = int(
            hashlib.sha1(self.spec.service_name().encode('utf-8')).hexdigest(),
            16
        ) % (2 ** 32)  # truncate result to 32 bits
        final = sorted(ls)
        random.Random(seed).shuffle(final)
        return final

    def remove_non_maintenance_unreachable_candidates(self, candidates: List[DaemonPlacement]) -> List[DaemonPlacement]:
        in_maintenance: Dict[str, bool] = {}
        for h in self.hosts:
            if h.status.lower() == 'maintenance':
                in_maintenance[h.hostname] = True
                continue
            in_maintenance[h.hostname] = False
        unreachable_hosts = [h.hostname for h in self.unreachable_hosts]
        candidates = [
            c for c in candidates if c.hostname not in unreachable_hosts or in_maintenance[c.hostname]]
        return candidates
