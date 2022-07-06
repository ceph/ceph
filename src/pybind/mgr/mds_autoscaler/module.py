"""
Automatically scale MDSs based on status of the file-system using the FSMap
"""

import logging
from typing import Any, Optional
from mgr_module import MgrModule, NotifyType
from orchestrator._interface import MDSSpec, ServiceSpec
import orchestrator
import copy

log = logging.getLogger(__name__)


class MDSAutoscaler(orchestrator.OrchestratorClientMixin, MgrModule):
    """
    MDS autoscaler.
    """
    NOTIFY_TYPES = [NotifyType.fs_map]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        MgrModule.__init__(self, *args, **kwargs)
        self.set_mgr(self)

    def get_service(self, fs_name: str) -> Optional[orchestrator.ServiceDescription]:
        service = f"mds.{fs_name}"
        completion = self.describe_service(service_type='mds',
                                           service_name=service,
                                           refresh=True)
        orchestrator.raise_if_exception(completion)
        if completion.result:
            return completion.result[0]
        return None

    def update_daemon_count(self, spec: ServiceSpec, fs_name: str, abscount: int) -> MDSSpec:
        ps = copy.deepcopy(spec.placement)
        ps.count = abscount
        newspec = MDSSpec(service_type=spec.service_type,
                          service_id=spec.service_id,
                          placement=ps)
        return newspec

    def get_required_standby_count(self, fs_map: dict, fs_name: str) -> int:
        assert fs_map is not None
        for fs in fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == fs_name:
                return fs['mdsmap']['standby_count_wanted']
        assert False

    def get_required_max_mds(self, fs_map: dict, fs_name: str) -> int:
        assert fs_map is not None
        for fs in fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == fs_name:
                return fs['mdsmap']['max_mds']
        assert False

    def verify_and_manage_mds_instance(self, fs_map: dict, fs_name: str) -> None:
        assert fs_map is not None

        try:
            svc = self.get_service(fs_name)
            if not svc:
                self.log.info(f"fs {fs_name}: no service defined; skipping")
                return
            if not svc.spec.placement.count:
                self.log.info(f"fs {fs_name}: service does not specify a count; skipping")
                return

            standbys_required = self.get_required_standby_count(fs_map, fs_name)
            max_mds = self.get_required_max_mds(fs_map, fs_name)
            want = max_mds + standbys_required

            self.log.info(f"fs {fs_name}: "
                          f"max_mds={max_mds} "
                          f"standbys_required={standbys_required}, "
                          f"count={svc.spec.placement.count}")

            if want == svc.spec.placement.count:
                return

            self.log.info(f"fs {fs_name}: adjusting daemon count from {svc.spec.placement.count} to {want}")
            newspec = self.update_daemon_count(svc.spec, fs_name, want)
            completion = self.apply_mds(newspec)
            orchestrator.raise_if_exception(completion)
        except orchestrator.OrchestratorError as e:
            self.log.exception(f"fs {fs_name}: exception while updating service: {e}")
            pass

    def notify(self, notify_type: NotifyType, notify_id: str) -> None:
        if notify_type != NotifyType.fs_map:
            return
        fs_map = self.get('fs_map')
        if not fs_map:
            return

        # we don't know for which fs config has been changed
        for fs in fs_map['filesystems']:
            fs_name = fs['mdsmap']['fs_name']
            self.verify_and_manage_mds_instance(fs_map, fs_name)
