import logging
from typing import List, Optional, Callable, Any, Tuple

from orchestrator._interface import DaemonDescription

logger = logging.getLogger(__name__)


class MemoryAutotuner(object):

    min_size_by_type = {
        'mds': 4096 * 1048576,
        'mgr': 4096 * 1048576,
        'mon': 1024 * 1048576,
        'crash': 128 * 1048576,
        'keepalived': 128 * 1048576,
        'haproxy': 128 * 1048576,
        'nvmeof': 4096 * 1048576,
    }
    default_size = 1024 * 1048576

    def __init__(
            self,
            daemons: List[DaemonDescription],
            config_get: Callable[[str, str], Any],
            total_mem: int,
    ):
        self.daemons = daemons
        self.config_get = config_get
        self.total_mem = total_mem

    def tune(self) -> Tuple[Optional[int], List[str]]:
        tuned_osds: List[str] = []
        total = self.total_mem
        for d in self.daemons:
            if d.daemon_type == 'mds':
                total -= self.config_get(d.name(), 'mds_cache_memory_limit')
                continue
            if d.daemon_type != 'osd':
                assert d.daemon_type
                total -= max(
                    self.min_size_by_type.get(d.daemon_type, self.default_size),
                    d.memory_usage or 0
                )
                continue
            if not self.config_get(d.name(), 'osd_memory_target_autotune'):
                total -= self.config_get(d.name(), 'osd_memory_target')
                continue
            tuned_osds.append(d.name())
        if total < 0:
            return None, []
        if not tuned_osds:
            return None, []
        per = total // len(tuned_osds)
        return int(per), tuned_osds
