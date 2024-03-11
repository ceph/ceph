import json
import logging
import time
import uuid
from typing import TYPE_CHECKING, Optional, Dict, List, Tuple, Any, cast

import orchestrator
from cephadm.registry import Registry
from cephadm.serve import CephadmServe
from cephadm.services.cephadmservice import CephadmDaemonDeploySpec
from cephadm.utils import ceph_release_to_major, name_to_config_section, CEPH_UPGRADE_ORDER, \
    CEPH_TYPES, NON_CEPH_IMAGE_TYPES, GATEWAY_TYPES
from cephadm.ssh import HostConnectionError
from orchestrator import OrchestratorError, DaemonDescription, DaemonDescriptionStatus, daemon_type_to_service

if TYPE_CHECKING:
    from .module import CephadmOrchestrator


logger = logging.getLogger(__name__)

# from ceph_fs.h
CEPH_MDSMAP_ALLOW_STANDBY_REPLAY = (1 << 5)
CEPH_MDSMAP_NOT_JOINABLE = (1 << 0)


def normalize_image_digest(digest: str, default_registry: str) -> str:
    """
    Normal case:
    >>> normalize_image_digest('ceph/ceph', 'docker.io')
    'docker.io/ceph/ceph'

    No change:
    >>> normalize_image_digest('quay.ceph.io/ceph/ceph', 'docker.io')
    'quay.ceph.io/ceph/ceph'

    >>> normalize_image_digest('docker.io/ubuntu', 'docker.io')
    'docker.io/ubuntu'

    >>> normalize_image_digest('localhost/ceph', 'docker.io')
    'localhost/ceph'
    """
    known_shortnames = [
        'ceph/ceph',
        'ceph/daemon',
        'ceph/daemon-base',
    ]
    for image in known_shortnames:
        if digest.startswith(image):
            return f'{default_registry}/{digest}'
    return digest


class UpgradeState:
    def __init__(self,
                 target_name: str,
                 progress_id: str,
                 target_id: Optional[str] = None,
                 target_digests: Optional[List[str]] = None,
                 target_version: Optional[str] = None,
                 error: Optional[str] = None,
                 paused: Optional[bool] = None,
                 fail_fs: bool = False,
                 fs_original_max_mds: Optional[Dict[str, int]] = None,
                 fs_original_allow_standby_replay: Optional[Dict[str, bool]] = None,
                 daemon_types: Optional[List[str]] = None,
                 hosts: Optional[List[str]] = None,
                 services: Optional[List[str]] = None,
                 total_count: Optional[int] = None,
                 remaining_count: Optional[int] = None,
                 ):
        self._target_name: str = target_name  # Use CephadmUpgrade.target_image instead.
        self.progress_id: str = progress_id
        self.target_id: Optional[str] = target_id
        self.target_digests: Optional[List[str]] = target_digests
        self.target_version: Optional[str] = target_version
        self.error: Optional[str] = error
        self.paused: bool = paused or False
        self.fs_original_max_mds: Optional[Dict[str, int]] = fs_original_max_mds
        self.fs_original_allow_standby_replay: Optional[Dict[str,
                                                             bool]] = fs_original_allow_standby_replay
        self.fail_fs = fail_fs
        self.daemon_types = daemon_types
        self.hosts = hosts
        self.services = services
        self.total_count = total_count
        self.remaining_count = remaining_count

    def to_json(self) -> dict:
        return {
            'target_name': self._target_name,
            'progress_id': self.progress_id,
            'target_id': self.target_id,
            'target_digests': self.target_digests,
            'target_version': self.target_version,
            'fail_fs': self.fail_fs,
            'fs_original_max_mds': self.fs_original_max_mds,
            'fs_original_allow_standby_replay': self.fs_original_allow_standby_replay,
            'error': self.error,
            'paused': self.paused,
            'daemon_types': self.daemon_types,
            'hosts': self.hosts,
            'services': self.services,
            'total_count': self.total_count,
            'remaining_count': self.remaining_count,
        }

    @classmethod
    def from_json(cls, data: dict) -> Optional['UpgradeState']:
        valid_params = UpgradeState.__init__.__code__.co_varnames
        if data:
            c = {k: v for k, v in data.items() if k in valid_params}
            if 'repo_digest' in c:
                c['target_digests'] = [c.pop('repo_digest')]
            return cls(**c)
        else:
            return None


class CephadmUpgrade:
    UPGRADE_ERRORS = [
        'UPGRADE_NO_STANDBY_MGR',
        'UPGRADE_FAILED_PULL',
        'UPGRADE_REDEPLOY_DAEMON',
        'UPGRADE_BAD_TARGET_VERSION',
        'UPGRADE_EXCEPTION',
        'UPGRADE_OFFLINE_HOST'
    ]

    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr = mgr

        t = self.mgr.get_store('upgrade_state')
        if t:
            self.upgrade_state: Optional[UpgradeState] = UpgradeState.from_json(json.loads(t))
        else:
            self.upgrade_state = None
        self.upgrade_info_str: str = ''

    @property
    def target_image(self) -> str:
        assert self.upgrade_state
        if not self.mgr.use_repo_digest:
            return self.upgrade_state._target_name
        if not self.upgrade_state.target_digests:
            return self.upgrade_state._target_name

        # FIXME: we assume the first digest is the best one to use
        return self.upgrade_state.target_digests[0]

    def upgrade_status(self) -> orchestrator.UpgradeStatusSpec:
        r = orchestrator.UpgradeStatusSpec()
        if self.upgrade_state:
            r.target_image = self.target_image
            r.in_progress = True
            r.progress, r.services_complete = self._get_upgrade_info()
            r.is_paused = self.upgrade_state.paused

            if self.upgrade_state.daemon_types is not None:
                which_str = f'Upgrading daemons of type(s) {",".join(self.upgrade_state.daemon_types)}'
                if self.upgrade_state.hosts is not None:
                    which_str += f' on host(s) {",".join(self.upgrade_state.hosts)}'
            elif self.upgrade_state.services is not None:
                which_str = f'Upgrading daemons in service(s) {",".join(self.upgrade_state.services)}'
                if self.upgrade_state.hosts is not None:
                    which_str += f' on host(s) {",".join(self.upgrade_state.hosts)}'
            elif self.upgrade_state.hosts is not None:
                which_str = f'Upgrading all daemons on host(s) {",".join(self.upgrade_state.hosts)}'
            else:
                which_str = 'Upgrading all daemon types on all hosts'
            if self.upgrade_state.total_count is not None and self.upgrade_state.remaining_count is not None:
                which_str += f'. Upgrade limited to {self.upgrade_state.total_count} daemons ({self.upgrade_state.remaining_count} remaining).'
            r.which = which_str

            # accessing self.upgrade_info_str will throw an exception if it
            # has not been set in _do_upgrade yet
            try:
                r.message = self.upgrade_info_str
            except AttributeError:
                pass
            if self.upgrade_state.error:
                r.message = 'Error: ' + self.upgrade_state.error
            elif self.upgrade_state.paused:
                r.message = 'Upgrade paused'
        return r

    def _get_upgrade_info(self) -> Tuple[str, List[str]]:
        if not self.upgrade_state or not self.upgrade_state.target_digests:
            return '', []

        daemons = self._get_filtered_daemons()

        if any(not d.container_image_digests for d in daemons if d.daemon_type == 'mgr'):
            return '', []

        completed_daemons = [(d.daemon_type, any(d in self.upgrade_state.target_digests for d in (
            d.container_image_digests or []))) for d in daemons if d.daemon_type]

        done = len([True for completion in completed_daemons if completion[1]])

        completed_types = list(set([completion[0] for completion in completed_daemons if all(
            c[1] for c in completed_daemons if c[0] == completion[0])]))

        return '%s/%s daemons upgraded' % (done, len(daemons)), completed_types

    def _get_filtered_daemons(self) -> List[DaemonDescription]:
        # Return the set of daemons set to be upgraded with out current
        # filtering parameters (or all daemons in upgrade order if no filtering
        # parameter are set).
        assert self.upgrade_state is not None
        if self.upgrade_state.daemon_types is not None:
            daemons = [d for d in self.mgr.cache.get_daemons(
            ) if d.daemon_type in self.upgrade_state.daemon_types]
        elif self.upgrade_state.services is not None:
            daemons = []
            for service in self.upgrade_state.services:
                daemons += self.mgr.cache.get_daemons_by_service(service)
        else:
            daemons = [d for d in self.mgr.cache.get_daemons(
            ) if d.daemon_type in CEPH_UPGRADE_ORDER]
        if self.upgrade_state.hosts is not None:
            daemons = [d for d in daemons if d.hostname in self.upgrade_state.hosts]
        return daemons

    def _get_current_version(self) -> Tuple[int, int, str]:
        current_version = self.mgr.version.split('ceph version ')[1]
        (current_major, current_minor, _) = current_version.split('-')[0].split('.', 2)
        return (int(current_major), int(current_minor), current_version)

    def _check_target_version(self, version: str) -> Optional[str]:
        try:
            v = version.split('.', 2)
            (major, minor) = (int(v[0]), int(v[1]))
            assert minor >= 0
            # patch might be a number or {number}-g{sha1}
        except ValueError:
            return 'version must be in the form X.Y.Z (e.g., 15.2.3)'
        if major < 15 or (major == 15 and minor < 2):
            return 'cephadm only supports octopus (15.2.0) or later'

        # to far a jump?
        (current_major, current_minor, current_version) = self._get_current_version()
        if current_major < major - 2:
            return f'ceph can only upgrade 1 or 2 major versions at a time; {current_version} -> {version} is too big a jump'
        if current_major > major:
            return f'ceph cannot downgrade major versions (from {current_version} to {version})'
        if current_major == major:
            if current_minor > minor:
                return f'ceph cannot downgrade to a {"rc" if minor == 1 else "dev"} release'

        # check mon min
        monmap = self.mgr.get("mon_map")
        mon_min = monmap.get("min_mon_release", 0)
        if mon_min < major - 2:
            return f'min_mon_release ({mon_min}) < target {major} - 2; first complete an upgrade to an earlier release'

        # check osd min
        osdmap = self.mgr.get("osd_map")
        osd_min_name = osdmap.get("require_osd_release", "argonaut")
        osd_min = ceph_release_to_major(osd_min_name)
        if osd_min < major - 2:
            return f'require_osd_release ({osd_min_name} or {osd_min}) < target {major} - 2; first complete an upgrade to an earlier release'

        return None

    def upgrade_ls(self, image: Optional[str], tags: bool, show_all_versions: Optional[bool]) -> Dict:
        if not image:
            image = self.mgr.container_image_base
        reg_name, bare_image = image.split('/', 1)
        if ':' in bare_image:
            # for our purposes, we don't want to use the tag here
            bare_image = bare_image.split(':')[0]
        reg = Registry(reg_name)
        (current_major, current_minor, _) = self._get_current_version()
        versions = []
        r: Dict[Any, Any] = {
            "image": image,
            "registry": reg_name,
            "bare_image": bare_image,
        }

        try:
            ls = reg.get_tags(bare_image)
        except ValueError as e:
            raise OrchestratorError(f'{e}')
        if not tags:
            for t in ls:
                if t[0] != 'v':
                    continue
                v = t[1:].split('.')
                if len(v) != 3:
                    continue
                if '-' in v[2]:
                    continue
                v_major = int(v[0])
                v_minor = int(v[1])
                candidate_version = (v_major > current_major
                                     or (v_major == current_major and v_minor >= current_minor))
                if show_all_versions or candidate_version:
                    versions.append('.'.join(v))
            r["versions"] = sorted(
                versions,
                key=lambda k: list(map(int, k.split('.'))),
                reverse=True
            )
        else:
            r["tags"] = sorted(ls)
        return r

    def upgrade_start(self, image: str, version: str, daemon_types: Optional[List[str]] = None,
                      hosts: Optional[List[str]] = None, services: Optional[List[str]] = None, limit: Optional[int] = None) -> str:
        fail_fs_value = cast(bool, self.mgr.get_module_option_ex(
            'orchestrator', 'fail_fs', False))
        if self.mgr.mode != 'root':
            raise OrchestratorError('upgrade is not supported in %s mode' % (
                self.mgr.mode))
        if version:
            version_error = self._check_target_version(version)
            if version_error:
                raise OrchestratorError(version_error)
            target_name = self.mgr.container_image_base + ':v' + version
        elif image:
            target_name = normalize_image_digest(image, self.mgr.default_registry)
        else:
            raise OrchestratorError('must specify either image or version')

        if daemon_types is not None or services is not None or hosts is not None:
            self._validate_upgrade_filters(target_name, daemon_types, hosts, services)

        if self.upgrade_state:
            if self.upgrade_state._target_name != target_name:
                raise OrchestratorError(
                    'Upgrade to %s (not %s) already in progress' %
                    (self.upgrade_state._target_name, target_name))
            if self.upgrade_state.paused:
                self.upgrade_state.paused = False
                self._save_upgrade_state()
                return 'Resumed upgrade to %s' % self.target_image
            return 'Upgrade to %s in progress' % self.target_image

        running_mgr_count = len([daemon for daemon in self.mgr.cache.get_daemons_by_type(
            'mgr') if daemon.status == DaemonDescriptionStatus.running])

        if running_mgr_count < 2:
            raise OrchestratorError('Need at least 2 running mgr daemons for upgrade')

        self.mgr.log.info('Upgrade: Started with target %s' % target_name)
        self.upgrade_state = UpgradeState(
            target_name=target_name,
            progress_id=str(uuid.uuid4()),
            fail_fs=fail_fs_value,
            daemon_types=daemon_types,
            hosts=hosts,
            services=services,
            total_count=limit,
            remaining_count=limit,
        )
        self._update_upgrade_progress(0.0)
        self._save_upgrade_state()
        self._clear_upgrade_health_checks()
        self.mgr.event.set()
        return 'Initiating upgrade to %s' % (target_name)

    def _validate_upgrade_filters(self, target_name: str, daemon_types: Optional[List[str]] = None, hosts: Optional[List[str]] = None, services: Optional[List[str]] = None) -> None:
        def _latest_type(dtypes: List[str]) -> str:
            # [::-1] gives the list in reverse
            for daemon_type in CEPH_UPGRADE_ORDER[::-1]:
                if daemon_type in dtypes:
                    return daemon_type
            return ''

        def _get_earlier_daemons(dtypes: List[str], candidates: List[DaemonDescription]) -> List[DaemonDescription]:
            # this function takes a list of daemon types and first finds the daemon
            # type from that list that is latest in our upgrade order. Then, from
            # that latest type, it filters the list of candidate daemons received
            # for daemons with types earlier in the upgrade order than the latest
            # type found earlier. That filtered list of daemons is returned. The
            # purpose of this function is to help in finding daemons that must have
            # already been upgraded for the given filtering parameters (--daemon-types,
            # --services, --hosts) to be valid.
            latest = _latest_type(dtypes)
            if not latest:
                return []
            earlier_types = '|'.join(CEPH_UPGRADE_ORDER).split(latest)[0].split('|')[:-1]
            earlier_types = [t for t in earlier_types if t not in dtypes]
            return [d for d in candidates if d.daemon_type in earlier_types]

        if self.upgrade_state:
            raise OrchestratorError(
                'Cannot set values for --daemon-types, --services or --hosts when upgrade already in progress.')
        try:
            with self.mgr.async_timeout_handler('cephadm inspect-image'):
                target_id, target_version, target_digests = self.mgr.wait_async(
                    CephadmServe(self.mgr)._get_container_image_info(target_name))
        except OrchestratorError as e:
            raise OrchestratorError(f'Failed to pull {target_name}: {str(e)}')
        # what we need to do here is build a list of daemons that must already be upgraded
        # in order for the user's selection of daemons to upgrade to be valid. for example,
        # if they say --daemon-types 'osd,mds' but mons have not been upgraded, we block.
        daemons = [d for d in self.mgr.cache.get_daemons(
        ) if d.daemon_type not in NON_CEPH_IMAGE_TYPES]
        err_msg_base = 'Cannot start upgrade. '
        # "dtypes" will later be filled in with the types of daemons that will be upgraded with the given parameters
        dtypes = []
        if daemon_types is not None:
            dtypes = daemon_types
            if hosts is not None:
                dtypes = [_latest_type(dtypes)]
                other_host_daemons = [
                    d for d in daemons if d.hostname is not None and d.hostname not in hosts]
                daemons = _get_earlier_daemons(dtypes, other_host_daemons)
            else:
                daemons = _get_earlier_daemons(dtypes, daemons)
            err_msg_base += 'Daemons with types earlier in upgrade order than given types need upgrading.\n'
        elif services is not None:
            # for our purposes here we can effectively convert our list of services into the
            # set of daemon types the services contain. This works because we don't allow --services
            # and --daemon-types at the same time and we only allow services of the same type
            sspecs = [
                self.mgr.spec_store[s].spec for s in services if self.mgr.spec_store[s].spec is not None]
            stypes = list(set([s.service_type for s in sspecs]))
            if len(stypes) != 1:
                raise OrchestratorError('Doing upgrade by service only support services of one type at '
                                        f'a time. Found service types: {stypes}')
            for stype in stypes:
                dtypes += orchestrator.service_to_daemon_types(stype)
            dtypes = list(set(dtypes))
            if hosts is not None:
                other_host_daemons = [
                    d for d in daemons if d.hostname is not None and d.hostname not in hosts]
                daemons = _get_earlier_daemons(dtypes, other_host_daemons)
            else:
                daemons = _get_earlier_daemons(dtypes, daemons)
            err_msg_base += 'Daemons with types earlier in upgrade order than daemons from given services need upgrading.\n'
        elif hosts is not None:
            # hosts must be handled a bit differently. For this, we really need to find all the daemon types
            # that reside on hosts in the list of hosts we will upgrade. Then take the type from
            # that list that is latest in the upgrade order and check if any daemons on hosts not in the
            # provided list of hosts have a daemon with a type earlier in the upgrade order that is not upgraded.
            dtypes = list(
                set([d.daemon_type for d in daemons if d.daemon_type is not None and d.hostname in hosts]))
            other_hosts_daemons = [
                d for d in daemons if d.hostname is not None and d.hostname not in hosts]
            daemons = _get_earlier_daemons([_latest_type(dtypes)], other_hosts_daemons)
            err_msg_base += 'Daemons with types earlier in upgrade order than daemons on given host need upgrading.\n'
        need_upgrade_self, n1, n2, _ = self._detect_need_upgrade(daemons, target_digests, target_name)
        if need_upgrade_self and ('mgr' not in dtypes or (daemon_types is None and services is None)):
            # also report active mgr as needing to be upgraded. It is not included in the resulting list
            # by default as it is treated special and handled via the need_upgrade_self bool
            n1.insert(0, (self.mgr.mgr_service.get_active_daemon(
                self.mgr.cache.get_daemons_by_type('mgr')), True))
        if n1 or n2:
            raise OrchestratorError(f'{err_msg_base}Please first upgrade '
                                    f'{", ".join(list(set([d[0].name() for d in n1] + [d[0].name() for d in n2])))}\n'
                                    f'NOTE: Enforced upgrade order is: {" -> ".join(CEPH_TYPES + GATEWAY_TYPES)}')

    def upgrade_pause(self) -> str:
        if not self.upgrade_state:
            raise OrchestratorError('No upgrade in progress')
        if self.upgrade_state.paused:
            return 'Upgrade to %s already paused' % self.target_image
        self.upgrade_state.paused = True
        self.mgr.log.info('Upgrade: Paused upgrade to %s' % self.target_image)
        self._save_upgrade_state()
        return 'Paused upgrade to %s' % self.target_image

    def upgrade_resume(self) -> str:
        if not self.upgrade_state:
            raise OrchestratorError('No upgrade in progress')
        if not self.upgrade_state.paused:
            return 'Upgrade to %s not paused' % self.target_image
        self.upgrade_state.paused = False
        self.upgrade_state.error = ''
        self.mgr.log.info('Upgrade: Resumed upgrade to %s' % self.target_image)
        self._save_upgrade_state()
        self.mgr.event.set()
        for alert_id in self.UPGRADE_ERRORS:
            self.mgr.remove_health_warning(alert_id)
        return 'Resumed upgrade to %s' % self.target_image

    def upgrade_stop(self) -> str:
        if not self.upgrade_state:
            return 'No upgrade in progress'
        if self.upgrade_state.progress_id:
            self.mgr.remote('progress', 'complete',
                            self.upgrade_state.progress_id)
        target_image = self.target_image
        self.mgr.log.info('Upgrade: Stopped')
        self.upgrade_state = None
        self._save_upgrade_state()
        self._clear_upgrade_health_checks()
        self.mgr.event.set()
        return 'Stopped upgrade to %s' % target_image

    def continue_upgrade(self) -> bool:
        """
        Returns false, if nothing was done.
        :return:
        """
        if self.upgrade_state and not self.upgrade_state.paused:
            try:
                self._do_upgrade()
            except HostConnectionError as e:
                self._fail_upgrade('UPGRADE_OFFLINE_HOST', {
                    'severity': 'error',
                    'summary': f'Upgrade: Failed to connect to host {e.hostname} at addr ({e.addr})',
                    'count': 1,
                    'detail': [f'SSH connection failed to {e.hostname} at addr ({e.addr}): {str(e)}'],
                })
                return False
            except Exception as e:
                self._fail_upgrade('UPGRADE_EXCEPTION', {
                    'severity': 'error',
                    'summary': 'Upgrade: failed due to an unexpected exception',
                    'count': 1,
                    'detail': [f'Unexpected exception occurred during upgrade process: {str(e)}'],
                })
                return False
            return True
        return False

    def _wait_for_ok_to_stop(
            self, s: DaemonDescription,
            known: Optional[List[str]] = None,  # NOTE: output argument!
    ) -> bool:
        # only wait a little bit; the service might go away for something
        assert s.daemon_type is not None
        assert s.daemon_id is not None
        tries = 4
        while tries > 0:
            if not self.upgrade_state or self.upgrade_state.paused:
                return False

            # setting force flag to retain old functionality.
            # note that known is an output argument for ok_to_stop()
            r = self.mgr.cephadm_services[daemon_type_to_service(s.daemon_type)].ok_to_stop([
                s.daemon_id], known=known, force=True)

            if not r.retval:
                logger.info(f'Upgrade: {r.stdout}')
                return True
            logger.info(f'Upgrade: {r.stderr}')

            time.sleep(15)
            tries -= 1
        return False

    def _clear_upgrade_health_checks(self) -> None:
        for k in self.UPGRADE_ERRORS:
            if k in self.mgr.health_checks:
                del self.mgr.health_checks[k]
        self.mgr.set_health_checks(self.mgr.health_checks)

    def _fail_upgrade(self, alert_id: str, alert: dict) -> None:
        assert alert_id in self.UPGRADE_ERRORS
        if not self.upgrade_state:
            # this could happen if the user canceled the upgrade while we
            # were doing something
            return

        logger.error('Upgrade: Paused due to %s: %s' % (alert_id,
                                                        alert['summary']))
        self.upgrade_state.error = alert_id + ': ' + alert['summary']
        self.upgrade_state.paused = True
        self._save_upgrade_state()
        self.mgr.health_checks[alert_id] = alert
        self.mgr.set_health_checks(self.mgr.health_checks)

    def _update_upgrade_progress(self, progress: float) -> None:
        if not self.upgrade_state:
            assert False, 'No upgrade in progress'

        if not self.upgrade_state.progress_id:
            self.upgrade_state.progress_id = str(uuid.uuid4())
            self._save_upgrade_state()
        self.mgr.remote('progress', 'update', self.upgrade_state.progress_id,
                        ev_msg='Upgrade to %s' % (
                            self.upgrade_state.target_version or self.target_image
                        ),
                        ev_progress=progress,
                        add_to_ceph_s=True)

    def _save_upgrade_state(self) -> None:
        if not self.upgrade_state:
            self.mgr.set_store('upgrade_state', None)
            return
        self.mgr.set_store('upgrade_state', json.dumps(self.upgrade_state.to_json()))

    def get_distinct_container_image_settings(self) -> Dict[str, str]:
        # get all distinct container_image settings
        image_settings = {}
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'config dump',
            'format': 'json',
        })
        config = json.loads(out)
        for opt in config:
            if opt['name'] == 'container_image':
                image_settings[opt['section']] = opt['value']
        return image_settings

    def _prepare_for_mds_upgrade(
        self,
        target_major: str,
        need_upgrade: List[DaemonDescription]
    ) -> bool:
        # scale down all filesystems to 1 MDS
        assert self.upgrade_state
        if not self.upgrade_state.fs_original_max_mds:
            self.upgrade_state.fs_original_max_mds = {}
        if not self.upgrade_state.fs_original_allow_standby_replay:
            self.upgrade_state.fs_original_allow_standby_replay = {}
        fsmap = self.mgr.get("fs_map")
        continue_upgrade = True
        for fs in fsmap.get('filesystems', []):
            fscid = fs["id"]
            mdsmap = fs["mdsmap"]
            fs_name = mdsmap["fs_name"]

            # disable allow_standby_replay?
            if mdsmap['flags'] & CEPH_MDSMAP_ALLOW_STANDBY_REPLAY:
                self.mgr.log.info('Upgrade: Disabling standby-replay for filesystem %s' % (
                    fs_name
                ))
                if fscid not in self.upgrade_state.fs_original_allow_standby_replay:
                    self.upgrade_state.fs_original_allow_standby_replay[fscid] = True
                    self._save_upgrade_state()
                ret, out, err = self.mgr.check_mon_command({
                    'prefix': 'fs set',
                    'fs_name': fs_name,
                    'var': 'allow_standby_replay',
                    'val': '0',
                })
                continue_upgrade = False
                continue

            # scale down this filesystem?
            if mdsmap["max_mds"] > 1:
                if self.upgrade_state.fail_fs:
                    if not (mdsmap['flags'] & CEPH_MDSMAP_NOT_JOINABLE) and \
                            len(mdsmap['up']) > 0:
                        self.mgr.log.info(f'Upgrade: failing fs {fs_name} for '
                                          f'rapid multi-rank mds upgrade')
                        ret, out, err = self.mgr.check_mon_command({
                            'prefix': 'fs fail',
                            'fs_name': fs_name
                        })
                        if ret != 0:
                            continue_upgrade = False
                    continue
                else:
                    self.mgr.log.info('Upgrade: Scaling down filesystem %s' % (
                        fs_name
                    ))
                    if fscid not in self.upgrade_state.fs_original_max_mds:
                        self.upgrade_state.fs_original_max_mds[fscid] = \
                            mdsmap['max_mds']
                        self._save_upgrade_state()
                    ret, out, err = self.mgr.check_mon_command({
                        'prefix': 'fs set',
                        'fs_name': fs_name,
                        'var': 'max_mds',
                        'val': '1',
                    })
                    continue_upgrade = False
                    continue

            if not self.upgrade_state.fail_fs:
                if not (mdsmap['in'] == [0] and len(mdsmap['up']) <= 1):
                    self.mgr.log.info(
                        'Upgrade: Waiting for fs %s to scale down to reach 1 MDS' % (
                            fs_name))
                    time.sleep(10)
                    continue_upgrade = False
                    continue

            if len(mdsmap['up']) == 0:
                self.mgr.log.warning(
                    "Upgrade: No mds is up; continuing upgrade procedure to poke things in the right direction")
                # This can happen because the current version MDS have
                # incompatible compatsets; the mons will not do any promotions.
                # We must upgrade to continue.
            elif len(mdsmap['up']) > 0:
                mdss = list(mdsmap['info'].values())
                assert len(mdss) == 1
                lone_mds = mdss[0]
                if lone_mds['state'] != 'up:active':
                    self.mgr.log.info('Upgrade: Waiting for mds.%s to be up:active (currently %s)' % (
                        lone_mds['name'],
                        lone_mds['state'],
                    ))
                    time.sleep(10)
                    continue_upgrade = False
                    continue
            else:
                assert False

        return continue_upgrade

    def _enough_mons_for_ok_to_stop(self) -> bool:
        # type () -> bool
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'quorum_status',
        })
        try:
            j = json.loads(out)
        except Exception:
            raise OrchestratorError('failed to parse quorum status')

        mons = [m['name'] for m in j['monmap']['mons']]
        return len(mons) > 2

    def _enough_mds_for_ok_to_stop(self, mds_daemon: DaemonDescription) -> bool:
        # type (DaemonDescription) -> bool

        # find fs this mds daemon belongs to
        fsmap = self.mgr.get("fs_map")
        for fs in fsmap.get('filesystems', []):
            mdsmap = fs["mdsmap"]
            fs_name = mdsmap["fs_name"]

            assert mds_daemon.daemon_id
            if fs_name != mds_daemon.service_name().split('.', 1)[1]:
                # wrong fs for this mds daemon
                continue

            # get number of mds daemons for this fs
            mds_count = len(
                [daemon for daemon in self.mgr.cache.get_daemons_by_service(mds_daemon.service_name())])

            # standby mds daemons for this fs?
            if mdsmap["max_mds"] < mds_count:
                return True
            return False

        return True  # if mds has no fs it should pass ok-to-stop

    def _detect_need_upgrade(self, daemons: List[DaemonDescription], target_digests: Optional[List[str]] = None, target_name: Optional[str] = None) -> Tuple[bool, List[Tuple[DaemonDescription, bool]], List[Tuple[DaemonDescription, bool]], int]:
        # this function takes a list of daemons and container digests. The purpose
        # is to go through each daemon and check if the current container digests
        # for that daemon match the target digests. The purpose being that we determine
        # if a daemon is upgraded to a certain container image or not based on what
        # container digests it has. By checking the current digests against the
        # targets we can determine which daemons still need to be upgraded
        need_upgrade_self = False
        need_upgrade: List[Tuple[DaemonDescription, bool]] = []
        need_upgrade_deployer: List[Tuple[DaemonDescription, bool]] = []
        done = 0
        if target_digests is None:
            target_digests = []
        if target_name is None:
            target_name = ''
        for d in daemons:
            assert d.daemon_type is not None
            assert d.daemon_id is not None
            assert d.hostname is not None
            if self.mgr.use_agent and not self.mgr.cache.host_metadata_up_to_date(d.hostname):
                continue
            correct_image = False
            # check if the container digest for the digest we're upgrading to matches
            # the container digest for the daemon if "use_repo_digest" setting is true
            # or that the image name matches the daemon's image name if "use_repo_digest"
            # is false. The idea is to generally check if the daemon is already using
            # the image we're upgrading to or not. Additionally, since monitoring stack
            # daemons are included in the upgrade process but don't use the ceph images
            # we are assuming any monitoring stack daemon is on the "correct" image already
            if (
                (self.mgr.use_repo_digest and d.matches_digests(target_digests))
                or (not self.mgr.use_repo_digest and d.matches_image_name(target_name))
                or (d.daemon_type in NON_CEPH_IMAGE_TYPES)
            ):
                logger.debug('daemon %s.%s on correct image' % (
                    d.daemon_type, d.daemon_id))
                correct_image = True
                # do deployed_by check using digest no matter what. We don't care
                # what repo the image used to deploy the daemon was as long
                # as the image content is correct
                if any(d in target_digests for d in (d.deployed_by or [])):
                    logger.debug('daemon %s.%s deployed by correct version' % (
                        d.daemon_type, d.daemon_id))
                    done += 1
                    continue

            if self.mgr.daemon_is_self(d.daemon_type, d.daemon_id):
                logger.info('Upgrade: Need to upgrade myself (mgr.%s)' %
                            self.mgr.get_mgr_id())
                need_upgrade_self = True
                continue

            if correct_image:
                logger.debug('daemon %s.%s not deployed by correct version' % (
                    d.daemon_type, d.daemon_id))
                need_upgrade_deployer.append((d, True))
            else:
                logger.debug('daemon %s.%s not correct (%s, %s, %s)' % (
                    d.daemon_type, d.daemon_id,
                    d.container_image_name, d.container_image_digests, d.version))
                need_upgrade.append((d, False))

        return (need_upgrade_self, need_upgrade, need_upgrade_deployer, done)

    def _to_upgrade(self, need_upgrade: List[Tuple[DaemonDescription, bool]], target_image: str) -> Tuple[bool, List[Tuple[DaemonDescription, bool]]]:
        to_upgrade: List[Tuple[DaemonDescription, bool]] = []
        known_ok_to_stop: List[str] = []
        for d_entry in need_upgrade:
            d = d_entry[0]
            assert d.daemon_type is not None
            assert d.daemon_id is not None
            assert d.hostname is not None

            if not d.container_image_id:
                if d.container_image_name == target_image:
                    logger.debug(
                        'daemon %s has unknown container_image_id but has correct image name' % (d.name()))
                    continue

            if known_ok_to_stop:
                if d.name() in known_ok_to_stop:
                    logger.info(f'Upgrade: {d.name()} is also safe to restart')
                    to_upgrade.append(d_entry)
                continue

            if d.daemon_type == 'osd':
                # NOTE: known_ok_to_stop is an output argument for
                # _wait_for_ok_to_stop
                if not self._wait_for_ok_to_stop(d, known_ok_to_stop):
                    return False, to_upgrade

            if d.daemon_type == 'mon' and self._enough_mons_for_ok_to_stop():
                if not self._wait_for_ok_to_stop(d, known_ok_to_stop):
                    return False, to_upgrade

            if d.daemon_type == 'mds' and self._enough_mds_for_ok_to_stop(d):
                # when fail_fs is set to true, all MDS daemons will be moved to
                # up:standby state, so Cephadm won't be able to upgrade due to
                # this check and and will warn with "It is NOT safe to stop
                # mds.<daemon_name> at this time: one or more filesystems is
                # currently degraded", therefore we bypass this check for that
                # case.
                assert self.upgrade_state is not None
                if not self.upgrade_state.fail_fs \
                        and not self._wait_for_ok_to_stop(d, known_ok_to_stop):
                    return False, to_upgrade

            to_upgrade.append(d_entry)

            # if we don't have a list of others to consider, stop now
            if d.daemon_type in ['osd', 'mds', 'mon'] and not known_ok_to_stop:
                break
        return True, to_upgrade

    def _upgrade_daemons(self, to_upgrade: List[Tuple[DaemonDescription, bool]], target_image: str, target_digests: Optional[List[str]] = None) -> None:
        assert self.upgrade_state is not None
        num = 1
        if target_digests is None:
            target_digests = []
        for d_entry in to_upgrade:
            if self.upgrade_state.remaining_count is not None and self.upgrade_state.remaining_count <= 0 and not d_entry[1]:
                self.mgr.log.info(
                    f'Hit upgrade limit of {self.upgrade_state.total_count}. Stopping upgrade')
                return
            d = d_entry[0]
            assert d.daemon_type is not None
            assert d.daemon_id is not None
            assert d.hostname is not None

            # make sure host has latest container image
            with self.mgr.async_timeout_handler(d.hostname, 'cephadm inspect-image'):
                out, errs, code = self.mgr.wait_async(CephadmServe(self.mgr)._run_cephadm(
                    d.hostname, '', 'inspect-image', [],
                    image=target_image, no_fsid=True, error_ok=True))
            if code or not any(d in target_digests for d in json.loads(''.join(out)).get('repo_digests', [])):
                logger.info('Upgrade: Pulling %s on %s' % (target_image,
                                                           d.hostname))
                self.upgrade_info_str = 'Pulling %s image on host %s' % (
                    target_image, d.hostname)
                with self.mgr.async_timeout_handler(d.hostname, 'cephadm pull'):
                    out, errs, code = self.mgr.wait_async(CephadmServe(self.mgr)._run_cephadm(
                        d.hostname, '', 'pull', [],
                        image=target_image, no_fsid=True, error_ok=True))
                if code:
                    self._fail_upgrade('UPGRADE_FAILED_PULL', {
                        'severity': 'warning',
                        'summary': 'Upgrade: failed to pull target image',
                        'count': 1,
                        'detail': [
                            'failed to pull %s on host %s' % (target_image,
                                                              d.hostname)],
                    })
                    return
                r = json.loads(''.join(out))
                if not any(d in target_digests for d in r.get('repo_digests', [])):
                    logger.info('Upgrade: image %s pull on %s got new digests %s (not %s), restarting' % (
                        target_image, d.hostname, r['repo_digests'], target_digests))
                    self.upgrade_info_str = 'Image %s pull on %s got new digests %s (not %s), restarting' % (
                        target_image, d.hostname, r['repo_digests'], target_digests)
                    self.upgrade_state.target_digests = r['repo_digests']
                    self._save_upgrade_state()
                    return

                self.upgrade_info_str = 'Currently upgrading %s daemons' % (d.daemon_type)

            if len(to_upgrade) > 1:
                logger.info('Upgrade: Updating %s.%s (%d/%d)' % (d.daemon_type, d.daemon_id, num, min(len(to_upgrade),
                            self.upgrade_state.remaining_count if self.upgrade_state.remaining_count is not None else 9999999)))
            else:
                logger.info('Upgrade: Updating %s.%s' %
                            (d.daemon_type, d.daemon_id))
            action = 'Upgrading' if not d_entry[1] else 'Redeploying'
            try:
                daemon_spec = CephadmDaemonDeploySpec.from_daemon_description(d)
                self.mgr._daemon_action(
                    daemon_spec,
                    'redeploy',
                    image=target_image if not d_entry[1] else None
                )
                self.mgr.cache.metadata_up_to_date[d.hostname] = False
            except Exception as e:
                self._fail_upgrade('UPGRADE_REDEPLOY_DAEMON', {
                    'severity': 'warning',
                    'summary': f'{action} daemon {d.name()} on host {d.hostname} failed.',
                    'count': 1,
                    'detail': [
                        f'Upgrade daemon: {d.name()}: {e}'
                    ],
                })
                return
            num += 1
            if self.upgrade_state.remaining_count is not None and not d_entry[1]:
                self.upgrade_state.remaining_count -= 1
                self._save_upgrade_state()

    def _handle_need_upgrade_self(self, need_upgrade_self: bool, upgrading_mgrs: bool) -> None:
        if need_upgrade_self:
            try:
                self.mgr.mgr_service.fail_over()
            except OrchestratorError as e:
                self._fail_upgrade('UPGRADE_NO_STANDBY_MGR', {
                    'severity': 'warning',
                    'summary': f'Upgrade: {e}',
                    'count': 1,
                    'detail': [
                        'The upgrade process needs to upgrade the mgr, '
                        'but it needs at least one standby to proceed.',
                    ],
                })
                return

            return  # unreachable code, as fail_over never returns
        elif upgrading_mgrs:
            if 'UPGRADE_NO_STANDBY_MGR' in self.mgr.health_checks:
                del self.mgr.health_checks['UPGRADE_NO_STANDBY_MGR']
                self.mgr.set_health_checks(self.mgr.health_checks)

    def _set_container_images(self, daemon_type: str, target_image: str, image_settings: Dict[str, str]) -> None:
        # push down configs
        daemon_type_section = name_to_config_section(daemon_type)
        if image_settings.get(daemon_type_section) != target_image:
            logger.info('Upgrade: Setting container_image for all %s' %
                        daemon_type)
            self.mgr.set_container_image(daemon_type_section, target_image)
        to_clean = []
        for section in image_settings.keys():
            if section.startswith(name_to_config_section(daemon_type) + '.'):
                to_clean.append(section)
        if to_clean:
            logger.debug('Upgrade: Cleaning up container_image for %s' %
                         to_clean)
            for section in to_clean:
                ret, image, err = self.mgr.check_mon_command({
                    'prefix': 'config rm',
                    'name': 'container_image',
                    'who': section,
                })

    def _complete_osd_upgrade(self, target_major: str, target_major_name: str) -> None:
        osdmap = self.mgr.get("osd_map")
        osd_min_name = osdmap.get("require_osd_release", "argonaut")
        osd_min = ceph_release_to_major(osd_min_name)
        if osd_min < int(target_major):
            logger.info(
                f'Upgrade: Setting require_osd_release to {target_major} {target_major_name}')
            ret, _, err = self.mgr.check_mon_command({
                'prefix': 'osd require-osd-release',
                'release': target_major_name,
            })

    def _complete_mds_upgrade(self) -> None:
        assert self.upgrade_state is not None
        if self.upgrade_state.fail_fs:
            for fs in self.mgr.get("fs_map")['filesystems']:
                fs_name = fs['mdsmap']['fs_name']
                self.mgr.log.info('Upgrade: Setting filesystem '
                                  f'{fs_name} Joinable')
                try:
                    ret, _, err = self.mgr.check_mon_command({
                        'prefix': 'fs set',
                        'fs_name': fs_name,
                        'var': 'joinable',
                        'val': 'true',
                    })
                except Exception as e:
                    logger.error("Failed to set fs joinable "
                                 f"true due to {e}")
                    raise OrchestratorError("Failed to set"
                                            "fs joinable true"
                                            f"due to {e}")
        elif self.upgrade_state.fs_original_max_mds:
            for fs in self.mgr.get("fs_map")['filesystems']:
                fscid = fs["id"]
                fs_name = fs['mdsmap']['fs_name']
                new_max = self.upgrade_state.fs_original_max_mds.get(fscid, 1)
                if new_max > 1:
                    self.mgr.log.info('Upgrade: Scaling up filesystem %s max_mds to %d' % (
                        fs_name, new_max
                    ))
                    ret, _, err = self.mgr.check_mon_command({
                        'prefix': 'fs set',
                        'fs_name': fs_name,
                        'var': 'max_mds',
                        'val': str(new_max),
                    })

            self.upgrade_state.fs_original_max_mds = {}
            self._save_upgrade_state()
        if self.upgrade_state.fs_original_allow_standby_replay:
            for fs in self.mgr.get("fs_map")['filesystems']:
                fscid = fs["id"]
                fs_name = fs['mdsmap']['fs_name']
                asr = self.upgrade_state.fs_original_allow_standby_replay.get(fscid, False)
                if asr:
                    self.mgr.log.info('Upgrade: Enabling allow_standby_replay on filesystem %s' % (
                        fs_name
                    ))
                    ret, _, err = self.mgr.check_mon_command({
                        'prefix': 'fs set',
                        'fs_name': fs_name,
                        'var': 'allow_standby_replay',
                        'val': '1'
                    })

            self.upgrade_state.fs_original_allow_standby_replay = {}
            self._save_upgrade_state()

    def _mark_upgrade_complete(self) -> None:
        if not self.upgrade_state:
            logger.debug('_mark_upgrade_complete upgrade already marked complete, exiting')
            return
        logger.info('Upgrade: Complete!')
        if self.upgrade_state.progress_id:
            self.mgr.remote('progress', 'complete',
                            self.upgrade_state.progress_id)
        self.upgrade_state = None
        self._save_upgrade_state()

    def _do_upgrade(self):
        # type: () -> None
        if not self.upgrade_state:
            logger.debug('_do_upgrade no state, exiting')
            return

        if self.mgr.offline_hosts:
            # offline host(s), on top of potential connection errors when trying to upgrade a daemon
            # or pull an image, can cause issues where daemons are never ok to stop. Since evaluating
            # whether or not that risk is present for any given offline hosts is a difficult problem,
            # it's best to just fail upgrade cleanly so user can address the offline host(s)

            # the HostConnectionError expects a hostname and addr, so let's just take
            # one at random. It doesn't really matter which host we say we couldn't reach here.
            hostname: str = list(self.mgr.offline_hosts)[0]
            addr: str = self.mgr.inventory.get_addr(hostname)
            raise HostConnectionError(f'Host(s) were marked offline: {self.mgr.offline_hosts}', hostname, addr)

        target_image = self.target_image
        target_id = self.upgrade_state.target_id
        target_digests = self.upgrade_state.target_digests
        target_version = self.upgrade_state.target_version

        first = False
        if not target_id or not target_version or not target_digests:
            # need to learn the container hash
            logger.info('Upgrade: First pull of %s' % target_image)
            self.upgrade_info_str = 'Doing first pull of %s image' % (target_image)
            try:
                with self.mgr.async_timeout_handler(f'cephadm inspect-image (image {target_image})'):
                    target_id, target_version, target_digests = self.mgr.wait_async(
                        CephadmServe(self.mgr)._get_container_image_info(target_image))
            except OrchestratorError as e:
                self._fail_upgrade('UPGRADE_FAILED_PULL', {
                    'severity': 'warning',
                    'summary': 'Upgrade: failed to pull target image',
                    'count': 1,
                    'detail': [str(e)],
                })
                return
            if not target_version:
                self._fail_upgrade('UPGRADE_FAILED_PULL', {
                    'severity': 'warning',
                    'summary': 'Upgrade: failed to pull target image',
                    'count': 1,
                    'detail': ['unable to extract ceph version from container'],
                })
                return
            self.upgrade_state.target_id = target_id
            # extract the version portion of 'ceph version {version} ({sha1})'
            self.upgrade_state.target_version = target_version.split(' ')[2]
            self.upgrade_state.target_digests = target_digests
            self._save_upgrade_state()
            target_image = self.target_image
            first = True

        if target_digests is None:
            target_digests = []
        if target_version.startswith('ceph version '):
            # tolerate/fix upgrade state from older version
            self.upgrade_state.target_version = target_version.split(' ')[2]
            target_version = self.upgrade_state.target_version
        (target_major, _) = target_version.split('.', 1)
        target_major_name = self.mgr.lookup_release_name(int(target_major))

        if first:
            logger.info('Upgrade: Target is version %s (%s)' % (
                target_version, target_major_name))
            logger.info('Upgrade: Target container is %s, digests %s' % (
                target_image, target_digests))

        version_error = self._check_target_version(target_version)
        if version_error:
            self._fail_upgrade('UPGRADE_BAD_TARGET_VERSION', {
                'severity': 'error',
                'summary': f'Upgrade: cannot upgrade/downgrade to {target_version}',
                'count': 1,
                'detail': [version_error],
            })
            return

        image_settings = self.get_distinct_container_image_settings()

        if self.upgrade_state.daemon_types is not None:
            logger.debug(
                f'Filtering daemons to upgrade by daemon types: {self.upgrade_state.daemon_types}')
            daemons = [d for d in self.mgr.cache.get_daemons(
            ) if d.daemon_type in self.upgrade_state.daemon_types]
        elif self.upgrade_state.services is not None:
            logger.debug(
                f'Filtering daemons to upgrade by services: {self.upgrade_state.daemon_types}')
            daemons = []
            for service in self.upgrade_state.services:
                daemons += self.mgr.cache.get_daemons_by_service(service)
        else:
            daemons = [d for d in self.mgr.cache.get_daemons(
            ) if d.daemon_type in CEPH_UPGRADE_ORDER]
        if self.upgrade_state.hosts is not None:
            logger.debug(f'Filtering daemons to upgrade by hosts: {self.upgrade_state.hosts}')
            daemons = [d for d in daemons if d.hostname in self.upgrade_state.hosts]
        upgraded_daemon_count: int = 0
        for daemon_type in CEPH_UPGRADE_ORDER:
            if self.upgrade_state.remaining_count is not None and self.upgrade_state.remaining_count <= 0:
                # we hit our limit and should end the upgrade
                # except for cases where we only need to redeploy, but not actually upgrade
                # the image (which we don't count towards our limit). This case only occurs with mgr
                # and monitoring stack daemons. Additionally, this case is only valid if
                # the active mgr is already upgraded.
                if any(d in target_digests for d in self.mgr.get_active_mgr_digests()):
                    if daemon_type not in NON_CEPH_IMAGE_TYPES and daemon_type != 'mgr':
                        continue
                else:
                    self._mark_upgrade_complete()
                    return
            logger.debug('Upgrade: Checking %s daemons' % daemon_type)
            daemons_of_type = [d for d in daemons if d.daemon_type == daemon_type]

            need_upgrade_self, need_upgrade, need_upgrade_deployer, done = self._detect_need_upgrade(
                daemons_of_type, target_digests, target_image)
            upgraded_daemon_count += done
            self._update_upgrade_progress(upgraded_daemon_count / len(daemons))

            # make sure mgr and non-ceph-image daemons are properly redeployed in staggered upgrade scenarios
            if daemon_type == 'mgr' or daemon_type in NON_CEPH_IMAGE_TYPES:
                if any(d in target_digests for d in self.mgr.get_active_mgr_digests()):
                    need_upgrade_names = [d[0].name() for d in need_upgrade] + \
                        [d[0].name() for d in need_upgrade_deployer]
                    dds = [d for d in self.mgr.cache.get_daemons_by_type(
                        daemon_type) if d.name() not in need_upgrade_names]
                    need_upgrade_active, n1, n2, __ = self._detect_need_upgrade(dds, target_digests, target_image)
                    if not n1:
                        if not need_upgrade_self and need_upgrade_active:
                            need_upgrade_self = True
                        need_upgrade_deployer += n2
                else:
                    # no point in trying to redeploy with new version if active mgr is not on the new version
                    need_upgrade_deployer = []

            if any(d in target_digests for d in self.mgr.get_active_mgr_digests()):
                # only after the mgr itself is upgraded can we expect daemons to have
                # deployed_by == target_digests
                need_upgrade += need_upgrade_deployer

            # prepare filesystems for daemon upgrades?
            if (
                daemon_type == 'mds'
                and need_upgrade
                and not self._prepare_for_mds_upgrade(target_major, [d_entry[0] for d_entry in need_upgrade])
            ):
                return

            if need_upgrade:
                self.upgrade_info_str = 'Currently upgrading %s daemons' % (daemon_type)

            _continue, to_upgrade = self._to_upgrade(need_upgrade, target_image)
            if not _continue:
                return
            self._upgrade_daemons(to_upgrade, target_image, target_digests)
            if to_upgrade:
                return

            self._handle_need_upgrade_self(need_upgrade_self, daemon_type == 'mgr')

            # following bits of _do_upgrade are for completing upgrade for given
            # types. If we haven't actually finished upgrading all the daemons
            # of this type, we should exit the loop here
            _, n1, n2, _ = self._detect_need_upgrade(
                self.mgr.cache.get_daemons_by_type(daemon_type), target_digests, target_image)
            if n1 or n2:
                continue

            # complete mon upgrade?
            if daemon_type == 'mon':
                if not self.mgr.get("have_local_config_map"):
                    logger.info('Upgrade: Restarting mgr now that mons are running pacific')
                    need_upgrade_self = True

            self._handle_need_upgrade_self(need_upgrade_self, daemon_type == 'mgr')

            # make sure 'ceph versions' agrees
            ret, out_ver, err = self.mgr.check_mon_command({
                'prefix': 'versions',
            })
            j = json.loads(out_ver)
            for version, count in j.get(daemon_type, {}).items():
                short_version = version.split(' ')[2]
                if short_version != target_version:
                    logger.warning(
                        'Upgrade: %d %s daemon(s) are %s != target %s' %
                        (count, daemon_type, short_version, target_version))

            self._set_container_images(daemon_type, target_image, image_settings)

            # complete osd upgrade?
            if daemon_type == 'osd':
                self._complete_osd_upgrade(target_major, target_major_name)

            # complete mds upgrade?
            if daemon_type == 'mds':
                self._complete_mds_upgrade()

            # Make sure all metadata is up to date before saying we are done upgrading this daemon type
            if self.mgr.use_agent and not self.mgr.cache.all_host_metadata_up_to_date():
                self.mgr.agent_helpers._request_ack_all_not_up_to_date()
                return

            logger.debug('Upgrade: Upgraded %s daemon(s).' % daemon_type)

        # clean up
        logger.info('Upgrade: Finalizing container_image settings')
        self.mgr.set_container_image('global', target_image)

        for daemon_type in CEPH_UPGRADE_ORDER:
            ret, image, err = self.mgr.check_mon_command({
                'prefix': 'config rm',
                'name': 'container_image',
                'who': name_to_config_section(daemon_type),
            })

        self._mark_upgrade_complete()
        return
