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
        logger.debug('Autotuning OSD memory with given parameters:\n'
                     f'Total memory: {total}\nDaemons: {self.daemons}')
        for d in self.daemons:
            if d.daemon_type == 'mds':
                mds_mem = self.config_get(d.name(), 'mds_cache_memory_limit')
                logger.debug(f'Subtracting {mds_mem} from total for mds daemon')
                total -= mds_mem
                logger.debug(f'new total: {total}')
                continue
            if d.daemon_type != 'osd':
                assert d.daemon_type
                daemon_mem = max(
                    self.min_size_by_type.get(d.daemon_type, self.default_size),
                    d.memory_usage or 0
                )
                logger.debug(f'Subtracting {daemon_mem} from total for {d.daemon_type} daemon')
                total -= daemon_mem
                logger.debug(f'new total: {total}')
                continue
            if not self.config_get(d.name(), 'osd_memory_target_autotune'):
                osd_mem = self.config_get(d.name(), 'osd_memory_target')
                logger.debug('osd_memory_target_autotune disabled. '
                             f'Subtracting {osd_mem} from total for osd daemon')
                total -= osd_mem
                logger.debug(f'new total: {total}')
                continue
            tuned_osds.append(d.name())
        if total < 0:
            return None, []
        if not tuned_osds:
            return None, []
        logger.debug(f'Final total is {total} to be split among {len(tuned_osds)} OSDs')
        per = total // len(tuned_osds)
        logger.debug(f'Result is {per} per OSD')
        return int(per), tuned_osds
