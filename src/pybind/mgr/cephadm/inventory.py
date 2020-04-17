import json
import logging
from typing import TYPE_CHECKING, Dict, List, Iterator

from orchestrator import OrchestratorError, HostSpec

if TYPE_CHECKING:
    from .module import CephadmOrchestrator


logger = logging.getLogger(__name__)

class Inventory:
    def __init__(self, mgr: 'CephadmOrchestrator'):
        self.mgr = mgr
        # load inventory
        i = self.mgr.get_store('inventory')
        if i:
            self._inventory: Dict[str, dict] = json.loads(i)
        else:
            self._inventory = dict()
        logger.debug('Loaded inventory %s' % self._inventory)

    def keys(self) -> List[str]:
        return list(self._inventory.keys())

    def __contains__(self, host: str) -> bool:
        return host in self._inventory

    def assert_host(self, host):
        if host not in self._inventory:
            raise OrchestratorError('host %s does not exist' % host)

    def add_host(self, spec: HostSpec):
        self._inventory[spec.hostname] = spec.to_json()
        self.save()

    def rm_host(self, host: str):
        self.assert_host(host)
        del self._inventory[host]
        self.save()

    def set_addr(self, host, addr):
        self.assert_host(host)
        self._inventory[host]['addr'] = addr
        self.save()

    def add_label(self, host, label):
        self.assert_host(host)

        if 'labels' not in self._inventory[host]:
            self._inventory[host]['labels'] = list()
        if label not in self._inventory[host]['labels']:
            self._inventory[host]['labels'].append(label)
        self.save()

    def rm_label(self, host, label):
        self.assert_host(host)

        if 'labels' not in self._inventory[host]:
            self._inventory[host]['labels'] = list()
        if label in self._inventory[host]['labels']:
            self._inventory[host]['labels'].remove(label)
        self.save()

    def get_addr(self, host) -> str:
        self.assert_host(host)
        return self._inventory[host].get('addr', host)

    def filter_by_label(self, label=None) -> Iterator[str]:
        for h, hostspec in self._inventory.items():
            if not label or label in hostspec.get('labels', []):
                yield h

    def spec_from_dict(self, info):
        hostname = info['hostname']
        return HostSpec(
                hostname,
                addr=info.get('addr', hostname),
                labels=info.get('labels', []),
                status='Offline' if hostname in self.mgr.offline_hosts else info.get('status', ''),
            )

    def all_specs(self) -> Iterator[HostSpec]:
        return map(self.spec_from_dict, self._inventory.values())

    def save(self):
        self.mgr.set_store('inventory', json.dumps(self._inventory))
