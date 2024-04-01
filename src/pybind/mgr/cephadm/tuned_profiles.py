import logging
from typing import Dict, List, TYPE_CHECKING
from ceph.utils import datetime_now
from .schedule import HostAssignment
from ceph.deployment.service_spec import ServiceSpec, TunedProfileSpec
from . import ssh

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)

SYSCTL_DIR = '/etc/sysctl.d'

SYSCTL_SYSTEM_CMD = ssh.RemoteCommand(ssh.Executables.SYSCTL, ['--system'])


class TunedProfileUtils():
    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr

    def _profile_to_str(self, p: TunedProfileSpec) -> str:
        p_str = f'# created by cephadm\n# tuned profile "{p.profile_name}"\n\n'
        for k, v in p.settings.items():
            p_str += f'{k} = {v}\n'
        return p_str

    def _write_all_tuned_profiles(self) -> None:
        host_profile_mapping: Dict[str, List[Dict[str, str]]] = {}
        for host in self.mgr.cache.get_hosts():
            host_profile_mapping[host] = []

        for profile in self.mgr.tuned_profiles.list_profiles():
            p_str = self._profile_to_str(profile)
            ha = HostAssignment(
                spec=ServiceSpec(
                    'crash', placement=profile.placement),
                hosts=self.mgr.cache.get_schedulable_hosts(),
                unreachable_hosts=self.mgr.cache.get_unreachable_hosts(),
                draining_hosts=self.mgr.cache.get_draining_hosts(),
                daemons=[],
                networks=self.mgr.cache.networks,
            )
            all_slots, _, _ = ha.place()
            for host in {s.hostname for s in all_slots}:
                host_profile_mapping[host].append({profile.profile_name: p_str})

        for host, profiles in host_profile_mapping.items():
            self._remove_stray_tuned_profiles(host, profiles)
            self._write_tuned_profiles(host, profiles)

    def _remove_stray_tuned_profiles(self, host: str, profiles: List[Dict[str, str]]) -> None:
        """
        this function looks at the contents of /etc/sysctl.d/ for profiles we have written
        that should now be removed. It assumes any file with "-cephadm-tuned-profile.conf" in
        it is written by us any without that are not. Only files written by us are considered
        candidates for removal. The "profiles" parameter is a list of dictionaries that map
        profile names to the file contents to actually be written to the
        /etc/sysctl.d/<profile-name>-cephadm-tuned-profile.conf. For example
        [
            {
                'profile1': 'setting1: value1\nsetting2: value2'
            },
            {
                'profile2': 'setting3: value3'
            }
        ]
        what we want to end up doing is going through the keys of the dicts and appending
        -cephadm-tuned-profile.conf to the profile names to build our list of profile files that
        SHOULD be on the host. Then if we see any file names that don't match this, but
        DO include "-cephadm-tuned-profile.conf" (implying they're from us), remove them.
        """
        if self.mgr.cache.is_host_unreachable(host):
            return
        cmd = ssh.RemoteCommand(ssh.Executables.LS, [SYSCTL_DIR])
        found_files = self.mgr.ssh.check_execute_command(host, cmd, log_command=self.mgr.log_refresh_metadata).split('\n')
        found_files = [s.strip() for s in found_files]
        profile_names: List[str] = sum([[*p] for p in profiles], [])  # extract all profiles names
        profile_names = list(set(profile_names))  # remove duplicates
        expected_files = [p + '-cephadm-tuned-profile.conf' for p in profile_names]
        updated = False
        for file in found_files:
            if '-cephadm-tuned-profile.conf' not in file:
                continue
            if file not in expected_files:
                logger.info(f'Removing stray tuned profile file {file}')
                cmd = ssh.RemoteCommand(ssh.Executables.RM, ['-f', f'{SYSCTL_DIR}/{file}'])
                self.mgr.ssh.check_execute_command(host, cmd)
                updated = True
        if updated:
            self.mgr.ssh.check_execute_command(host, SYSCTL_SYSTEM_CMD)

    def _write_tuned_profiles(self, host: str, profiles: List[Dict[str, str]]) -> None:
        if self.mgr.cache.is_host_unreachable(host):
            return
        updated = False
        for p in profiles:
            for profile_name, content in p.items():
                if self.mgr.cache.host_needs_tuned_profile_update(host, profile_name):
                    logger.info(f'Writing tuned profile {profile_name} to host {host}')
                    profile_filename: str = f'{SYSCTL_DIR}/{profile_name}-cephadm-tuned-profile.conf'
                    self.mgr.ssh.write_remote_file(host, profile_filename, content.encode('utf-8'))
                    updated = True
        if updated:
            self.mgr.ssh.check_execute_command(host, SYSCTL_SYSTEM_CMD)
        self.mgr.cache.last_tuned_profile_update[host] = datetime_now()
