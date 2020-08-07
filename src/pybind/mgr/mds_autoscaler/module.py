"""
Automatically scale MDSs based on status of the file-system using the FSMap
"""

import logging
from typing import Optional, List, Set
from mgr_module import MgrModule
from ceph.deployment.service_spec import ServiceSpec
import orchestrator
import copy

log = logging.getLogger(__name__)


class MDSAutoscaler(orchestrator.OrchestratorClientMixin, MgrModule):
    """
    MDS autoscaler.
    """
    def __init__(self, *args, **kwargs):
        MgrModule.__init__(self, *args, **kwargs)
        self.set_mgr(self)

    def get_service(self, fs_name: str) -> List[orchestrator.ServiceDescription]:
        service = f"mds.{fs_name}"
        completion = self.describe_service(service_type='mds',
                                           service_name=service,
                                           refresh=True)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return completion.result

    def get_daemons(self, fs_name: str) -> List[orchestrator.DaemonDescription]:
        service = f"mds.{fs_name}"
        completion = self.list_daemons(service_name=service)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return completion.result

    def update_daemon_count(self, fs_name: str, abscount: int) -> ServiceSpec:
        svclist = self.get_service(fs_name)

        assert svclist is not None
        assert len(svclist) > 0
        
        svc = svclist[0]

        assert svc.spec.placement.count != abscount

        ps = copy.deepcopy(svc.spec.placement)
        ps.count = abscount
        newspec = ServiceSpec(service_type=svc.spec.service_type,
                              service_id=svc.spec.service_id,
                              placement=ps)
        return newspec

    def get_required_standby_count(self, fs_map: dict, fs_name: str) -> int:
        assert fs_map is not None
        for fs in fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == fs_name:
                return fs['mdsmap']['standby_count_wanted']
        assert False

    def get_current_standby_count(self, fs_map: dict, fs_name: str, daemons: List[orchestrator.DaemonDescription]) -> int:
        # standbys are not grouped by filesystems in fs_map
        # available = standby_replay + standby_active
        assert fs_map is not None
        total = 0
        daemon_names = {
            d.name() for d in daemons
        }
        for sb in fs_map['standbys']:
            full_name = f"mds.{sb['name']}"
            if full_name in daemon_names:
                total += 1
        return total

    def get_active_names(self, fs_map: dict, fs_name: str) -> Set[str]:
        active_names = set()
        for fs in fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == fs_name:
                for active in fs['mdsmap']['up']:
                    gid = fs['mdsmap']['up'][active]
                    gid_key = f"gid_{gid}"
                    active_names.add(f"mds.{fs['mdsmap']['info'][gid_key]['name']}")
        return active_names

    def get_current_active_count(self, fs_map: dict, fs_name: str, daemons: List[orchestrator.DaemonDescription]) -> int:
        assert fs_map is not None
        total = 0
        daemon_names = {
            d.name() for d in daemons
        }
        active_names = self.get_active_names(fs_map, fs_name)
        return len(daemon_names.intersection(active_names))

    def get_required_max_mds(self, fs_map: dict, fs_name: str) -> int:
        assert fs_map is not None
        for fs in fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == fs_name:
                return fs['mdsmap']['max_mds']
        assert False

    def verify_and_manage_mds_instance(self, fs_map: dict, fs_name: str):
        assert fs_map is not None

        try:
            daemons = self.get_daemons(fs_name)
            standbys_required = self.get_required_standby_count(fs_map, fs_name)
            standbys_current = self.get_current_standby_count(fs_map, fs_name, daemons)
            active = self.get_current_active_count(fs_map, fs_name, daemons)
            max_mds_required = self.get_required_max_mds(fs_map, fs_name)

            self.log.info(f"fs_name:{fs_name} "
                          f"standbys_required:{standbys_required}, "
                          f"standbys_current:{standbys_current}, "
                          f"active:{active}, "
                          f"max_mds_required:{max_mds_required}")

            total_current = standbys_current + active
            total_required = max_mds_required + standbys_required
            self.log.info(f"fs:{fs_name} total_required:{total_required}, total_current:{total_current}")

            if total_required < total_current:
                self.log.info(f"fs:{fs_name}, killing {total_current - total_required} standby mds ...")
            elif total_required > total_current:
                self.log.info(f"fs:{fs_name}, spawning {total_required - total_current} standby mds ...")
            else:
                self.log.info(f"fs:{fs_name} no change to mds count")
                return

            newspec = self.update_daemon_count(fs_name, total_required)

            self.log.info(f"fs:{fs_name}, new placement count:{newspec.placement.count}")

            completion = self.apply_mds(newspec)
            self._orchestrator_wait([completion])
            orchestrator.raise_if_exception(completion)
        except orchestrator.OrchestratorError as e:
            self.log.exception(f"fs:{fs_name} exception while verifying mds status: {e}")
            pass

    def notify(self, notify_type, notify_id):
        if notify_type != 'fs_map':
            return
        fs_map = self.get('fs_map')
        if not fs_map:
            return
        # we don't know for which fs config has been changed
        for fs in fs_map['filesystems']:
            fs_name = fs['mdsmap']['fs_name']
            self.log.info(f"processing fs:{fs_name}")
            self.verify_and_manage_mds_instance(fs_map, fs_name)
