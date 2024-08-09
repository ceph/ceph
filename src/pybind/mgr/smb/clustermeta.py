from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypedDict,
)

import contextlib
import logging
import operator

from . import rados_store
from .proto import Simplified

if TYPE_CHECKING:  # pragma: no cover
    from mgr_module import MgrModule


log = logging.getLogger(__name__)


ClusterNodeEntry = TypedDict(
    'ClusterNodeEntry',
    {'pnn': int, 'identity': str, 'node': str, 'state': str},
)

CephDaemonInfo = TypedDict(
    'CephDaemonInfo',
    {'daemon_type': str, 'daemon_id': str, 'hostname': str, 'host_ip': str},
)

RankMap = Dict[int, Dict[int, Optional[str]]]
DaemonMap = Dict[str, CephDaemonInfo]


def _current_generation(
    generations: Dict[int, Optional[str]]
) -> Tuple[int, Optional[str]]:
    max_gen = max(generations.keys())
    return max_gen, generations[max_gen]


class ClusterMeta:
    def __init__(self) -> None:
        self._data: Simplified = {'nodes': [], '_source': 'cephadm'}
        self._orig = self._data

    def to_simplified(self) -> Simplified:
        return self._data

    def load(self, data: Simplified) -> None:
        if not data:
            return
        assert 'nodes' in data
        self._data = data
        self._orig = data

    def modified(self) -> bool:
        return self._data == self._orig

    def sync_ranks(self, rank_map: RankMap, daemon_map: DaemonMap) -> None:
        """Convert cephadm's ranks and node info into something sambacc
        can understand and manage for ctdb.
        """
        log.info('rank_map=%r, daemon_map=%r', rank_map, daemon_map)
        log.info('current data: %r', self._data)
        if not (rank_map and daemon_map):
            return
        missing = set()
        rank_max = -1
        for rank, rankval in rank_map.items():
            rank_max = max(rank_max, rank)
            curr_entry = self._get_pnn(rank)
            if not curr_entry:
                missing.add(rank)
                continue
            # "reconcile" existing rank-pnn values
            try:
                ceph_entry = self._to_entry(
                    rank, *_current_generation(rankval), daemon_map
                )
            except KeyError as err:
                log.warning(
                    'daemon not available: %s not in %r', err, daemon_map
                )
                continue
            if ceph_entry != curr_entry:
                # TODO do proper state value transitions
                log.warning("updating entry %r", ceph_entry)
                self._replace_entry(ceph_entry)
        if missing:
            log.warning('adding new entries')
            entries = []
            for rank in missing:
                try:
                    entries.append(
                        self._to_entry(
                            rank,
                            *_current_generation(rank_map[rank]),
                            daemon_map,
                        )
                    )
                except KeyError as err:
                    log.warning(
                        'daemon not available: %s not in %r', err, daemon_map
                    )
                    continue
            self._append_entries(entries)
        pnn_max = self._pnn_max()
        if pnn_max > rank_max:
            log.warning('removing extra entries')
            # need to "prune" entries
            for pnn in range(rank_max + 1, pnn_max + 1):
                entry = self._get_pnn(pnn)
                assert entry
                entry['state'] = 'gone'
                self._replace_entry(entry)
        log.info('synced data: %r; modified=%s', self._data, self.modified())

    def _nodes(self) -> List[ClusterNodeEntry]:
        log.warning('XXX _nodes data=%r', self._data)
        return [node for node in self._data['nodes']]

    def _pnn_max(self) -> int:
        log.warning('XXX _pnn_max data=%r', self._data)
        return max((n['pnn'] for n in self._nodes()), default=0)

    def _get_pnn(self, pnn: int) -> Optional[ClusterNodeEntry]:
        nodes = self._nodes()
        log.warning('nodes=%r', nodes)
        for value in nodes:
            assert isinstance(value, dict)
            if value['pnn'] == pnn:
                return value
        return None

    def _sort_nodes(self) -> None:
        self._data['nodes'].sort(key=operator.itemgetter('pnn'))

    def _replace_entry(self, entry: ClusterNodeEntry) -> None:
        assert isinstance(entry, dict)
        pnn = entry['pnn']
        self._data['nodes'] = [e for e in self._nodes() if e['pnn'] != pnn]
        self._data['nodes'].append(entry)
        self._sort_nodes()
        log.warning('XXX _replace_entry data=%r', self._data)

    def _append_entries(
        self, new_entries: Iterable[ClusterNodeEntry]
    ) -> None:
        self._data['nodes'].extend(new_entries)
        self._sort_nodes()
        log.warning('XXX _append_entries data=%r', self._data)

    def _to_entry(
        self, rank: int, gen: int, name: Optional[str], daemon_map: DaemonMap
    ) -> ClusterNodeEntry:
        assert name
        name = f'smb.{name}'
        di = daemon_map[name]
        return {
            'pnn': rank,
            'identity': name,
            'node': di['host_ip'],
            'state': 'ready',
        }


_LOCK_NAME = "cluster_meta"


@contextlib.contextmanager
def rados_object(mgr: 'MgrModule', uri: str) -> Iterator[ClusterMeta]:
    """Return a cluster meta object that will store persistent data in rados."""
    pool, ns, objname = rados_store.parse_uri(uri)
    store = rados_store.RADOSConfigStore.init(mgr, pool)

    cmeta = ClusterMeta()
    previous = {}
    entry = store[ns, objname]
    try:
        with entry.locked(_LOCK_NAME):
            previous = entry.get()
    except KeyError:
        log.debug('no previous object %s found', uri)
    cmeta.load(previous)
    yield cmeta
    with entry.locked(_LOCK_NAME):
        entry.set(cmeta.to_simplified())
