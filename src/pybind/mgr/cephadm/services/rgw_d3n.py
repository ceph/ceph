import logging

from typing import List, Dict, Tuple, TYPE_CHECKING

from dataclasses import dataclass
from orchestrator import (
    OrchestratorError,
    DaemonDescription,
)
from cephadm import utils
from ceph.cephadm.d3n_types import d3n_parse_dev_from_path
if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


logger = logging.getLogger(__name__)


@dataclass
class D3NDevicePlanner:
    mgr: "CephadmOrchestrator"

    def _d3n_fail_if_devs_used_by_other_rgw_service(self, host: str, devs: List[str], service_name: str) -> None:
        logger.info("1361")
        wanted = set(devs)

        # check rgw daemons on this host
        rgw_daemons = self.mgr.cache.get_daemons_by_type('rgw', host=host)

        for dd in rgw_daemons:
            # skip daemons that belong to the same service
            other_service = dd.service_name()

            if other_service == service_name:
                continue

            who = utils.name_to_config_section(dd.name())
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config get',
                'who': who,
                'name': 'rgw_d3n_l1_datacache_persistent_path',
            })
            if ret != 0:
                continue

            p = out or ''
            p = p.strip()
            if not p:
                continue

            used_dev = d3n_parse_dev_from_path(p)
            if used_dev and used_dev in wanted:
                raise OrchestratorError(
                    f'D3N device conflict on host "{host}": device "{used_dev}" is already used by RGW service "{other_service}" '
                    f'(daemon {dd.name()}). Refuse to reuse across services.'
                )

    def _d3n_gc_and_prune_alloc(
            self,
            alloc: Dict[str, str],
            devs: list[str],
            daemon_details: list[DaemonDescription],
            current_daemon_id: str,
            key: tuple[str, str],
    ) -> None:

        invalid = [did for did, dev in alloc.items() if dev not in devs]
        for did in invalid:
            del alloc[did]
        logger.debug(f"[D3N][alloc] prune-invalid: removed={invalid} devs={devs} alloc_now={alloc}")
        if not daemon_details:
            if alloc:
                logger.info(f"[D3N][alloc] clear-stale: key={key} alloc_was={alloc}")
                alloc.clear()
            return

        live_daemon_ids: set[str] = set()
        for dd in daemon_details:
            if dd.daemon_id:
                live_daemon_ids.add(dd.daemon_id)

        if current_daemon_id:
            live_daemon_ids.add(current_daemon_id)

        stale = [did for did in list(alloc.keys()) if did not in live_daemon_ids]
        for did in stale:
            del alloc[did]
        logger.debug(
            f"gc: key={key} live={sorted(live_daemon_ids)} "
            f"removed={stale} alloc_now={alloc}"
        )

    def _d3n_get_allocator(self) -> Dict[Tuple[str, str], Dict[str, str]]:
        """
        Return the in-memory D3N device allocation map.

        This is intentionally stored on the mgr module instance (self.mgr) as
        ephemeral state to keep per-(service, host) device selections stable across
        repeated scheduling cycles within the lifetime of the mgr process.

        It is not persisted to disk/mon-store; it will be rebuilt after mgr restart
        and also pruned via _d3n_gc_and_prune_alloc() based on currently running
        daemons.
        """
        alloc_all = getattr(self.mgr, "_d3n_device_alloc", None)
        if alloc_all is None:
            alloc_all = {}
            setattr(self.mgr, "_d3n_device_alloc", alloc_all)

        assert isinstance(alloc_all, dict)
        return alloc_all

    def _d3n_choose_device_for_daemon(
            self,
            service_name: str,
            host: str,
            devs: list[str],
            daemon_id: str,
            daemon_details: list[DaemonDescription],
    ) -> str:
        alloc_all = self._d3n_get_allocator()
        key = (service_name, host)
        alloc = alloc_all.setdefault(key, {})
        assert isinstance(alloc, dict)
        reason = "unknown"

        self._d3n_gc_and_prune_alloc(alloc, devs, daemon_details, daemon_id, key)

        logger.info(
            f"[D3N][alloc] pre-choose key={key} daemon={daemon_id} "
            f"daemon_details={len(daemon_details)} devs={devs} alloc={alloc}"
        )

        if daemon_id in alloc:
            logger.info(f"[D3N][alloc] reuse existing mapping daemon={daemon_id} dev={alloc[daemon_id]}")
            return alloc[daemon_id]

        used = set(alloc.values())
        free = [d for d in devs if d not in used]
        if free:
            chosen = free[0]  # 1:1 mapping whenever possible
        else:
            chosen = devs[len(alloc) % len(devs)]  # share the device when unavoidable
            reason = "round-robin/share"
        alloc[daemon_id] = chosen
        logger.info(f"[D3N][alloc] chosen={chosen} reason={reason} alloc_now={alloc}")
        return chosen

    def warn_if_sharing_unavoidable(
        self,
        service_name: str,
        host: str,
        devs: list[str],
    ) -> None:
        alloc_all = getattr(self.mgr, "_d3n_device_alloc", {})
        alloc = alloc_all.get((service_name, host), {}) if isinstance(alloc_all, dict) else {}
        if isinstance(alloc, dict) and len(alloc) > len(devs):
            logger.warning(
                f'D3N cache sub-optimal on host "{host}" for service "{service_name}": '
                f'{len(alloc)} RGW daemons but only {len(devs)} devices; devices will be shared.'
            )

    def plan_device_for_daemon(
        self,
        service_name: str,
        host: str,
        devs: list[str],
        daemon_id: str,
        daemon_details: list[DaemonDescription],
    ) -> str:
        self._d3n_fail_if_devs_used_by_other_rgw_service(host, devs, service_name)
        return self._d3n_choose_device_for_daemon(service_name, host, devs, daemon_id, daemon_details)
