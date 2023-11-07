import json
import logging
from asyncio import gather
from threading import Lock
from typing import List, Dict, Any, Set, Tuple, cast, Optional, TYPE_CHECKING

from ceph.deployment import translate
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.drive_selection import DriveSelection
from ceph.deployment.inventory import Device
from ceph.utils import datetime_to_str, str_to_datetime

from datetime import datetime
import orchestrator
from cephadm.serve import CephadmServe
from ceph.utils import datetime_now
from orchestrator import OrchestratorError, DaemonDescription
from mgr_module import MonCommandFailed

from cephadm.services.cephadmservice import CephadmDaemonDeploySpec, CephService

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


class OSDService(CephService):
    TYPE = 'osd'

    def create_from_spec(self, drive_group: DriveGroupSpec) -> str:
        logger.debug(f"Processing DriveGroup {drive_group}")
        osd_id_claims = OsdIdClaims(self.mgr)
        if osd_id_claims.get():
            logger.info(
                f"Found osd claims for drivegroup {drive_group.service_id} -> {osd_id_claims.get()}")

        async def create_from_spec_one(host: str, drive_selection: DriveSelection) -> Optional[str]:
            # skip this host if there has been no change in inventory
            if not self.mgr.cache.osdspec_needs_apply(host, drive_group):
                self.mgr.log.debug("skipping apply of %s on %s (no change)" % (
                    host, drive_group))
                return None
            # skip this host if we cannot schedule here
            if self.mgr.inventory.has_label(host, '_no_schedule'):
                return None

            osd_id_claims_for_host = osd_id_claims.filtered_by_host(host)

            cmds: List[str] = self.driveselection_to_ceph_volume(drive_selection,
                                                                 osd_id_claims_for_host)
            if not cmds:
                logger.debug("No data_devices, skipping DriveGroup: {}".format(
                    drive_group.service_id))
                return None

            logger.debug('Applying service osd.%s on host %s...' % (
                drive_group.service_id, host
            ))
            start_ts = datetime_now()
            env_vars: List[str] = [f"CEPH_VOLUME_OSDSPEC_AFFINITY={drive_group.service_id}"]
            ret_msg = await self.create_single_host(
                drive_group, host, cmds,
                replace_osd_ids=osd_id_claims_for_host, env_vars=env_vars
            )
            self.mgr.cache.update_osdspec_last_applied(
                host, drive_group.service_name(), start_ts
            )
            self.mgr.cache.save_host(host)
            return ret_msg

        async def all_hosts() -> List[Optional[str]]:
            futures = [create_from_spec_one(h, ds)
                       for h, ds in self.prepare_drivegroup(drive_group)]
            return await gather(*futures)

        with self.mgr.async_timeout_handler('cephadm deploy (osd daemon)'):
            ret = self.mgr.wait_async(all_hosts())
        return ", ".join(filter(None, ret))

    async def create_single_host(self,
                                 drive_group: DriveGroupSpec,
                                 host: str, cmds: List[str], replace_osd_ids: List[str],
                                 env_vars: Optional[List[str]] = None) -> str:
        for cmd in cmds:
            out, err, code = await self._run_ceph_volume_command(host, cmd, env_vars=env_vars)
            if code == 1 and ', it is already prepared' in '\n'.join(err):
                # HACK: when we create against an existing LV, ceph-volume
                # returns an error and the above message.  To make this
                # command idempotent, tolerate this "error" and continue.
                logger.debug('the device was already prepared; continuing')
                code = 0
            if code:
                raise RuntimeError(
                    'cephadm exited with an error code: %d, stderr:%s' % (
                        code, '\n'.join(err)))
        return await self.deploy_osd_daemons_for_existing_osds(host, drive_group.service_name(),
                                                               replace_osd_ids)

    async def deploy_osd_daemons_for_existing_osds(self, host: str, service_name: str,
                                                   replace_osd_ids: Optional[List[str]] = None) -> str:

        if replace_osd_ids is None:
            replace_osd_ids = OsdIdClaims(self.mgr).filtered_by_host(host)
            assert replace_osd_ids is not None

        # check result: lvm
        osds_elems: dict = await CephadmServe(self.mgr)._run_cephadm_json(
            host, 'osd', 'ceph-volume',
            [
                '--',
                'lvm', 'list',
                '--format', 'json',
            ])
        before_osd_uuid_map = self.mgr.get_osd_uuid_map(only_up=True)
        fsid = self.mgr._cluster_fsid
        osd_uuid_map = self.mgr.get_osd_uuid_map()
        created = []
        for osd_id, osds in osds_elems.items():
            for osd in osds:
                if osd['type'] == 'db':
                    continue
                if osd['tags']['ceph.cluster_fsid'] != fsid:
                    logger.debug('mismatched fsid, skipping %s' % osd)
                    continue
                if osd_id in before_osd_uuid_map and osd_id not in replace_osd_ids:
                    # if it exists but is part of the replacement operation, don't skip
                    continue
                if self.mgr.cache.has_daemon(f'osd.{osd_id}', host):
                    # cephadm daemon instance already exists
                    logger.debug(f'osd id {osd_id} daemon already exists')
                    continue
                if osd_id not in osd_uuid_map:
                    logger.debug('osd id {} does not exist in cluster'.format(osd_id))
                    continue
                if osd_uuid_map.get(osd_id) != osd['tags']['ceph.osd_fsid']:
                    logger.debug('mismatched osd uuid (cluster has %s, osd '
                                 'has %s)' % (
                                     osd_uuid_map.get(osd_id),
                                     osd['tags']['ceph.osd_fsid']))
                    continue

                created.append(osd_id)
                daemon_spec: CephadmDaemonDeploySpec = CephadmDaemonDeploySpec(
                    service_name=service_name,
                    daemon_id=str(osd_id),
                    host=host,
                    daemon_type='osd',
                )
                daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
                await CephadmServe(self.mgr)._create_daemon(
                    daemon_spec,
                    osd_uuid_map=osd_uuid_map)

        # check result: raw
        raw_elems: dict = await CephadmServe(self.mgr)._run_cephadm_json(
            host, 'osd', 'ceph-volume',
            [
                '--',
                'raw', 'list',
                '--format', 'json',
            ])
        for osd_uuid, osd in raw_elems.items():
            if osd.get('ceph_fsid') != fsid:
                continue
            osd_id = str(osd.get('osd_id', '-1'))
            if osd_id in before_osd_uuid_map and osd_id not in replace_osd_ids:
                # if it exists but is part of the replacement operation, don't skip
                continue
            if self.mgr.cache.has_daemon(f'osd.{osd_id}', host):
                # cephadm daemon instance already exists
                logger.debug(f'osd id {osd_id} daemon already exists')
                continue
            if osd_id not in osd_uuid_map:
                logger.debug('osd id {} does not exist in cluster'.format(osd_id))
                continue
            if osd_uuid_map.get(osd_id) != osd_uuid:
                logger.debug('mismatched osd uuid (cluster has %s, osd '
                             'has %s)' % (osd_uuid_map.get(osd_id), osd_uuid))
                continue
            if osd_id in created:
                continue

            created.append(osd_id)
            daemon_spec = CephadmDaemonDeploySpec(
                service_name=service_name,
                daemon_id=osd_id,
                host=host,
                daemon_type='osd',
            )
            daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
            await CephadmServe(self.mgr)._create_daemon(
                daemon_spec,
                osd_uuid_map=osd_uuid_map)

        if created:
            self.mgr.cache.invalidate_host_devices(host)
            self.mgr.cache.invalidate_autotune(host)
            return "Created osd(s) %s on host '%s'" % (','.join(created), host)
        else:
            return "Created no osd(s) on host %s; already created?" % host

    def prepare_drivegroup(self, drive_group: DriveGroupSpec) -> List[Tuple[str, DriveSelection]]:
        # 1) use fn_filter to determine matching_hosts
        matching_hosts = drive_group.placement.filter_matching_hostspecs(
            self.mgr.cache.get_schedulable_hosts())
        # 2) Map the inventory to the InventoryHost object
        host_ds_map = []

        # set osd_id_claims

        def _find_inv_for_host(hostname: str, inventory_dict: dict) -> List[Device]:
            # This is stupid and needs to be loaded with the host
            for _host, _inventory in inventory_dict.items():
                if _host == hostname:
                    return _inventory
            raise OrchestratorError("No inventory found for host: {}".format(hostname))

        # 3) iterate over matching_host and call DriveSelection
        logger.debug(f"Checking matching hosts -> {matching_hosts}")
        for host in matching_hosts:
            inventory_for_host = _find_inv_for_host(host, self.mgr.cache.devices)
            logger.debug(f"Found inventory for host {inventory_for_host}")

            # List of Daemons on that host
            dd_for_spec = self.mgr.cache.get_daemons_by_service(drive_group.service_name())
            dd_for_spec_and_host = [dd for dd in dd_for_spec if dd.hostname == host]

            drive_selection = DriveSelection(drive_group, inventory_for_host,
                                             existing_daemons=len(dd_for_spec_and_host))
            logger.debug(f"Found drive selection {drive_selection}")
            if drive_group.method and drive_group.method == 'raw':
                # ceph-volume can currently only handle a 1:1 mapping
                # of data/db/wal devices for raw mode osds. If db/wal devices
                # are defined and the number does not match the number of data
                # devices, we need to bail out
                if drive_selection.data_devices() and drive_selection.db_devices():
                    if len(drive_selection.data_devices()) != len(drive_selection.db_devices()):
                        raise OrchestratorError('Raw mode only supports a 1:1 ratio of data to db devices. Found '
                                                f'{len(drive_selection.data_devices())} potential data device(s) and '
                                                f'{len(drive_selection.db_devices())} potential db device(s) on host {host}')
                if drive_selection.data_devices() and drive_selection.wal_devices():
                    if len(drive_selection.data_devices()) != len(drive_selection.wal_devices()):
                        raise OrchestratorError('Raw mode only supports a 1:1 ratio of data to wal devices. Found '
                                                f'{len(drive_selection.data_devices())} potential data device(s) and '
                                                f'{len(drive_selection.wal_devices())} potential wal device(s) on host {host}')
            host_ds_map.append((host, drive_selection))
        return host_ds_map

    @staticmethod
    def driveselection_to_ceph_volume(drive_selection: DriveSelection,
                                      osd_id_claims: Optional[List[str]] = None,
                                      preview: bool = False) -> List[str]:
        logger.debug(f"Translating DriveGroup <{drive_selection.spec}> to ceph-volume command")
        cmds: List[str] = translate.to_ceph_volume(drive_selection,
                                                   osd_id_claims, preview=preview).run()
        logger.debug(f"Resulting ceph-volume cmds: {cmds}")
        return cmds

    def get_previews(self, host: str) -> List[Dict[str, Any]]:
        # Find OSDSpecs that match host.
        osdspecs = self.resolve_osdspecs_for_host(host)
        return self.generate_previews(osdspecs, host)

    def generate_previews(self, osdspecs: List[DriveGroupSpec], for_host: str) -> List[Dict[str, Any]]:
        """

        The return should look like this:

        [
          {'data': {<metadata>},
           'osdspec': <name of osdspec>,
           'host': <name of host>,
           'notes': <notes>
           },

           {'data': ...,
            'osdspec': ..,
            'host': ...,
            'notes': ...
           }
        ]

        Note: One host can have multiple previews based on its assigned OSDSpecs.
        """
        self.mgr.log.debug(f"Generating OSDSpec previews for {osdspecs}")
        ret_all: List[Dict[str, Any]] = []
        if not osdspecs:
            return ret_all
        for osdspec in osdspecs:

            # populate osd_id_claims
            osd_id_claims = OsdIdClaims(self.mgr)

            # prepare driveselection
            for host, ds in self.prepare_drivegroup(osdspec):
                if host != for_host:
                    continue

                # driveselection for host
                cmds: List[str] = self.driveselection_to_ceph_volume(ds,
                                                                     osd_id_claims.filtered_by_host(
                                                                         host),
                                                                     preview=True)
                if not cmds:
                    logger.debug("No data_devices, skipping DriveGroup: {}".format(
                        osdspec.service_name()))
                    continue

                # get preview data from ceph-volume
                for cmd in cmds:
                    with self.mgr.async_timeout_handler(host, f'cephadm ceph-volume -- {cmd}'):
                        out, err, code = self.mgr.wait_async(self._run_ceph_volume_command(host, cmd))
                    if out:
                        try:
                            concat_out: Dict[str, Any] = json.loads(' '.join(out))
                        except ValueError:
                            logger.exception('Cannot decode JSON: \'%s\'' % ' '.join(out))
                            concat_out = {}
                        notes = []
                        if osdspec.data_devices is not None and osdspec.data_devices.limit and len(concat_out) < osdspec.data_devices.limit:
                            found = len(concat_out)
                            limit = osdspec.data_devices.limit
                            notes.append(
                                f'NOTE: Did not find enough disks matching filter on host {host} to reach data device limit (Found: {found} | Limit: {limit})')
                        ret_all.append({'data': concat_out,
                                        'osdspec': osdspec.service_id,
                                        'host': host,
                                        'notes': notes})
        return ret_all

    def resolve_hosts_for_osdspecs(self,
                                   specs: Optional[List[DriveGroupSpec]] = None
                                   ) -> List[str]:
        osdspecs = []
        if specs:
            osdspecs = [cast(DriveGroupSpec, spec) for spec in specs]
        if not osdspecs:
            self.mgr.log.debug("No OSDSpecs found")
            return []
        return sum([spec.placement.filter_matching_hostspecs(self.mgr.cache.get_schedulable_hosts()) for spec in osdspecs], [])

    def resolve_osdspecs_for_host(self, host: str,
                                  specs: Optional[List[DriveGroupSpec]] = None) -> List[DriveGroupSpec]:
        matching_specs = []
        self.mgr.log.debug(f"Finding OSDSpecs for host: <{host}>")
        if not specs:
            specs = [cast(DriveGroupSpec, spec) for (sn, spec) in self.mgr.spec_store.spec_preview.items()
                     if spec.service_type == 'osd']
        for spec in specs:
            if host in spec.placement.filter_matching_hostspecs(self.mgr.cache.get_schedulable_hosts()):
                self.mgr.log.debug(f"Found OSDSpecs for host: <{host}> -> <{spec}>")
                matching_specs.append(spec)
        return matching_specs

    async def _run_ceph_volume_command(self, host: str,
                                       cmd: str, env_vars: Optional[List[str]] = None
                                       ) -> Tuple[List[str], List[str], int]:
        self.mgr.inventory.assert_host(host)

        # get bootstrap key
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get',
            'entity': 'client.bootstrap-osd',
        })

        j = json.dumps({
            'config': self.mgr.get_minimal_ceph_conf(),
            'keyring': keyring,
        })

        split_cmd = cmd.split(' ')
        _cmd = ['--config-json', '-', '--']
        _cmd.extend(split_cmd)
        out, err, code = await CephadmServe(self.mgr)._run_cephadm(
            host, 'osd', 'ceph-volume',
            _cmd,
            env_vars=env_vars,
            stdin=j,
            error_ok=True)
        return out, err, code

    def post_remove(self, daemon: DaemonDescription, is_failed_deploy: bool) -> None:
        # Do not remove the osd.N keyring, if we failed to deploy the OSD, because
        # we cannot recover from it. The OSD keys are created by ceph-volume and not by
        # us.
        if not is_failed_deploy:
            super().post_remove(daemon, is_failed_deploy=is_failed_deploy)


class OsdIdClaims(object):
    """
    Retrieve and provide osd ids that can be reused in the cluster
    """

    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr: "CephadmOrchestrator" = mgr
        self.osd_host_map: Dict[str, List[str]] = dict()
        self.refresh()

    def refresh(self) -> None:
        try:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'osd tree',
                'states': ['destroyed'],
                'format': 'json'
            })
        except MonCommandFailed as e:
            logger.exception('osd tree failed')
            raise OrchestratorError(str(e))
        try:
            tree = json.loads(out)
        except ValueError:
            logger.exception(f'Cannot decode JSON: \'{out}\'')
            return

        nodes = tree.get('nodes', {})
        for node in nodes:
            if node.get('type') == 'host':
                self.osd_host_map.update(
                    {node.get('name'): [str(_id) for _id in node.get('children', list())]}
                )
        if self.osd_host_map:
            self.mgr.log.info(f"Found osd claims -> {self.osd_host_map}")

    def get(self) -> Dict[str, List[str]]:
        return self.osd_host_map

    def filtered_by_host(self, host: str) -> List[str]:
        """
        Return the list of osd ids that can be reused  in a host

        OSD id claims in CRUSH map are linked to the bare name of
        the hostname. In case of FQDN hostnames the host is searched by the
        bare name
        """
        return self.osd_host_map.get(host.split(".")[0], [])


class RemoveUtil(object):
    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr: "CephadmOrchestrator" = mgr

    def get_osds_in_cluster(self) -> List[str]:
        osd_map = self.mgr.get_osdmap()
        return [str(x.get('osd')) for x in osd_map.dump().get('osds', [])]

    def osd_df(self) -> dict:
        base_cmd = 'osd df'
        ret, out, err = self.mgr.mon_command({
            'prefix': base_cmd,
            'format': 'json'
        })
        try:
            return json.loads(out)
        except ValueError:
            logger.exception(f'Cannot decode JSON: \'{out}\'')
            return {}

    def get_pg_count(self, osd_id: int, osd_df: Optional[dict] = None) -> int:
        if not osd_df:
            osd_df = self.osd_df()
        osd_nodes = osd_df.get('nodes', [])
        for osd_node in osd_nodes:
            if osd_node.get('id') == int(osd_id):
                return osd_node.get('pgs', -1)
        return -1

    def find_osd_stop_threshold(self, osds: List["OSD"]) -> Optional[List["OSD"]]:
        """
        Cut osd_id list in half until it's ok-to-stop

        :param osds: list of osd_ids
        :return: list of ods_ids that can be stopped at once
        """
        if not osds:
            return []
        while not self.ok_to_stop(osds):
            if len(osds) <= 1:
                # can't even stop one OSD, aborting
                self.mgr.log.debug(
                    "Can't even stop one OSD. Cluster is probably busy. Retrying later..")
                return []

            # This potentially prolongs the global wait time.
            self.mgr.event.wait(1)
            # splitting osd_ids in half until ok_to_stop yields success
            # maybe popping ids off one by one is better here..depends on the cluster size I guess..
            # There's a lot of room for micro adjustments here
            osds = osds[len(osds) // 2:]
        return osds

        # todo start draining
        #  return all([osd.start_draining() for osd in osds])

    def ok_to_stop(self, osds: List["OSD"]) -> bool:
        cmd_args = {
            'prefix': "osd ok-to-stop",
            'ids': [str(osd.osd_id) for osd in osds]
        }
        return self._run_mon_cmd(cmd_args, error_ok=True)

    def set_osd_flag(self, osds: List["OSD"], flag: str) -> bool:
        base_cmd = f"osd {flag}"
        self.mgr.log.debug(f"running cmd: {base_cmd} on ids {osds}")
        ret, out, err = self.mgr.mon_command({
            'prefix': base_cmd,
            'ids': [str(osd.osd_id) for osd in osds]
        })
        if ret != 0:
            self.mgr.log.error(f"Could not set {flag} flag for {osds}. <{err}>")
            return False
        self.mgr.log.info(f"{','.join([str(o) for o in osds])} now {flag}")
        return True

    def get_weight(self, osd: "OSD") -> Optional[float]:
        ret, out, err = self.mgr.mon_command({
            'prefix': 'osd crush tree',
            'format': 'json',
        })
        if ret != 0:
            self.mgr.log.error(f"Could not dump crush weights. <{err}>")
            return None
        j = json.loads(out)
        for n in j.get("nodes", []):
            if n.get("name") == f"osd.{osd.osd_id}":
                self.mgr.log.info(f"{osd} crush weight is {n.get('crush_weight')}")
                return n.get("crush_weight")
        return None

    def reweight_osd(self, osd: "OSD", weight: float) -> bool:
        self.mgr.log.debug(f"running cmd: osd crush reweight on {osd}")
        ret, out, err = self.mgr.mon_command({
            'prefix': "osd crush reweight",
            'name': f"osd.{osd.osd_id}",
            'weight': weight,
        })
        if ret != 0:
            self.mgr.log.error(f"Could not reweight {osd} to {weight}. <{err}>")
            return False
        self.mgr.log.info(f"{osd} weight is now {weight}")
        return True

    def zap_osd(self, osd: "OSD") -> str:
        "Zaps all devices that are associated with an OSD"
        if osd.hostname is not None:
            cmd = ['--', 'lvm', 'zap', '--osd-id', str(osd.osd_id)]
            if not osd.no_destroy:
                cmd.append('--destroy')
            with self.mgr.async_timeout_handler(osd.hostname, f'cephadm ceph-volume {" ".join(cmd)}'):
                out, err, code = self.mgr.wait_async(CephadmServe(self.mgr)._run_cephadm(
                    osd.hostname, 'osd', 'ceph-volume',
                    cmd,
                    error_ok=True))
            self.mgr.cache.invalidate_host_devices(osd.hostname)
            if code:
                raise OrchestratorError('Zap failed: %s' % '\n'.join(out + err))
            return '\n'.join(out + err)
        raise OrchestratorError(f"Failed to zap OSD {osd.osd_id} because host was unknown")

    def safe_to_destroy(self, osd_ids: List[int]) -> bool:
        """ Queries the safe-to-destroy flag for OSDs """
        cmd_args = {'prefix': 'osd safe-to-destroy',
                    'ids': [str(x) for x in osd_ids]}
        return self._run_mon_cmd(cmd_args, error_ok=True)

    def destroy_osd(self, osd_id: int) -> bool:
        """ Destroys an OSD (forcefully) """
        cmd_args = {'prefix': 'osd destroy-actual',
                    'id': int(osd_id),
                    'yes_i_really_mean_it': True}
        return self._run_mon_cmd(cmd_args)

    def purge_osd(self, osd_id: int) -> bool:
        """ Purges an OSD from the cluster (forcefully) """
        cmd_args = {
            'prefix': 'osd purge-actual',
            'id': int(osd_id),
            'yes_i_really_mean_it': True
        }
        return self._run_mon_cmd(cmd_args)

    def _run_mon_cmd(self, cmd_args: dict, error_ok: bool = False) -> bool:
        """
        Generic command to run mon_command and evaluate/log the results
        """
        ret, out, err = self.mgr.mon_command(cmd_args)
        if ret != 0:
            self.mgr.log.debug(f"ran {cmd_args} with mon_command")
            if not error_ok:
                self.mgr.log.error(
                    f"cmd: {cmd_args.get('prefix')} failed with: {err}. (errno:{ret})")
            return False
        self.mgr.log.debug(f"cmd: {cmd_args.get('prefix')} returns: {out}")
        return True


class NotFoundError(Exception):
    pass


class OSD:

    def __init__(self,
                 osd_id: int,
                 remove_util: RemoveUtil,
                 drain_started_at: Optional[datetime] = None,
                 process_started_at: Optional[datetime] = None,
                 drain_stopped_at: Optional[datetime] = None,
                 drain_done_at: Optional[datetime] = None,
                 draining: bool = False,
                 started: bool = False,
                 stopped: bool = False,
                 replace: bool = False,
                 force: bool = False,
                 hostname: Optional[str] = None,
                 zap: bool = False,
                 no_destroy: bool = False):
        # the ID of the OSD
        self.osd_id = osd_id

        # when did process (not the actual draining) start
        self.process_started_at = process_started_at

        # when did the drain start
        self.drain_started_at = drain_started_at

        # when did the drain stop
        self.drain_stopped_at = drain_stopped_at

        # when did the drain finish
        self.drain_done_at = drain_done_at

        # did the draining start
        self.draining = draining

        # was the operation started
        self.started = started

        # was the operation stopped
        self.stopped = stopped

        # If this is a replace or remove operation
        self.replace = replace
        # If we wait for the osd to be drained
        self.force = force
        # The name of the node
        self.hostname = hostname

        # mgr obj to make mgr/mon calls
        self.rm_util: RemoveUtil = remove_util

        self.original_weight: Optional[float] = None

        # Whether devices associated with the OSD should be zapped (DATA ERASED)
        self.zap = zap
        # Whether all associated LV devices should be destroyed.
        self.no_destroy = no_destroy

    def start(self) -> None:
        if self.started:
            logger.debug(f"Already started draining {self}")
            return None
        self.started = True
        self.stopped = False
        self.original_weight = self.rm_util.get_weight(self)

    def start_draining(self) -> bool:
        if self.stopped:
            logger.debug(f"Won't start draining {self}. OSD draining is stopped.")
            return False
        if self.replace:
            self.rm_util.set_osd_flag([self], 'out')
        else:
            self.rm_util.reweight_osd(self, 0.0)
        self.drain_started_at = datetime.utcnow()
        self.draining = True
        logger.debug(f"Started draining {self}.")
        return True

    def stop_draining(self) -> bool:
        if self.replace:
            self.rm_util.set_osd_flag([self], 'in')
        else:
            if self.original_weight:
                self.rm_util.reweight_osd(self, self.original_weight)
        self.drain_stopped_at = datetime.utcnow()
        self.draining = False
        logger.debug(f"Stopped draining {self}.")
        return True

    def stop(self) -> None:
        if self.stopped:
            logger.debug(f"Already stopped draining {self}")
            return None
        self.started = False
        self.stopped = True
        self.stop_draining()

    @property
    def is_draining(self) -> bool:
        """
        Consider an OSD draining when it is
        actively draining but not yet empty
        """
        return self.draining and not self.is_empty

    @property
    def is_ok_to_stop(self) -> bool:
        return self.rm_util.ok_to_stop([self])

    @property
    def is_empty(self) -> bool:
        if self.get_pg_count() == 0:
            if not self.drain_done_at:
                self.drain_done_at = datetime.utcnow()
                self.draining = False
            return True
        return False

    def safe_to_destroy(self) -> bool:
        return self.rm_util.safe_to_destroy([self.osd_id])

    def down(self) -> bool:
        return self.rm_util.set_osd_flag([self], 'down')

    def destroy(self) -> bool:
        return self.rm_util.destroy_osd(self.osd_id)

    def do_zap(self) -> str:
        return self.rm_util.zap_osd(self)

    def purge(self) -> bool:
        return self.rm_util.purge_osd(self.osd_id)

    def get_pg_count(self) -> int:
        return self.rm_util.get_pg_count(self.osd_id)

    @property
    def exists(self) -> bool:
        return str(self.osd_id) in self.rm_util.get_osds_in_cluster()

    def drain_status_human(self) -> str:
        default_status = 'not started'
        status = 'started' if self.started and not self.draining else default_status
        status = 'draining' if self.draining else status
        status = 'done, waiting for purge' if self.drain_done_at and not self.draining else status
        return status

    def pg_count_str(self) -> str:
        return 'n/a' if self.get_pg_count() < 0 else str(self.get_pg_count())

    def to_json(self) -> dict:
        out: Dict[str, Any] = dict()
        out['osd_id'] = self.osd_id
        out['started'] = self.started
        out['draining'] = self.draining
        out['stopped'] = self.stopped
        out['replace'] = self.replace
        out['force'] = self.force
        out['zap'] = self.zap
        out['hostname'] = self.hostname  # type: ignore
        out['original_weight'] = self.original_weight

        for k in ['drain_started_at', 'drain_stopped_at', 'drain_done_at', 'process_started_at']:
            if getattr(self, k):
                out[k] = datetime_to_str(getattr(self, k))
            else:
                out[k] = getattr(self, k)
        return out

    @classmethod
    def from_json(cls, inp: Optional[Dict[str, Any]], rm_util: RemoveUtil) -> Optional["OSD"]:
        if not inp:
            return None
        for date_field in ['drain_started_at', 'drain_stopped_at', 'drain_done_at', 'process_started_at']:
            if inp.get(date_field):
                inp.update({date_field: str_to_datetime(inp.get(date_field, ''))})
        inp.update({'remove_util': rm_util})
        if 'nodename' in inp:
            hostname = inp.pop('nodename')
            inp['hostname'] = hostname
        return cls(**inp)

    def __hash__(self) -> int:
        return hash(self.osd_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OSD):
            return NotImplemented
        return self.osd_id == other.osd_id

    def __repr__(self) -> str:
        return f"osd.{self.osd_id}{' (draining)' if self.draining else ''}"


class OSDRemovalQueue(object):

    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr: "CephadmOrchestrator" = mgr
        self.osds: Set[OSD] = set()
        self.rm_util = RemoveUtil(mgr)

        # locks multithreaded access to self.osds. Please avoid locking
        # network calls, like mon commands.
        self.lock = Lock()

    def process_removal_queue(self) -> None:
        """
        Performs actions in the _serve() loop to remove an OSD
        when criteria is met.

        we can't hold self.lock, as we're calling _remove_daemon in the loop
        """

        # make sure that we don't run on OSDs that are not in the cluster anymore.
        self.cleanup()

        # find osds that are ok-to-stop and not yet draining
        ready_to_drain_osds = self._ready_to_drain_osds()
        if ready_to_drain_osds:
            # start draining those
            _ = [osd.start_draining() for osd in ready_to_drain_osds]

        all_osds = self.all_osds()

        logger.debug(
            f"{self.queue_size()} OSDs are scheduled "
            f"for removal: {all_osds}")

        # Check all osds for their state and take action (remove, purge etc)
        new_queue: Set[OSD] = set()
        for osd in all_osds:  # type: OSD
            if not osd.force:
                # skip criteria
                if not osd.is_empty:
                    logger.debug(f"{osd} is not empty yet. Waiting a bit more")
                    new_queue.add(osd)
                    continue

            if not osd.safe_to_destroy():
                logger.debug(
                    f"{osd} is not safe-to-destroy yet. Waiting a bit more")
                new_queue.add(osd)
                continue

            # abort criteria
            if not osd.down():
                # also remove it from the remove_osd list and set a health_check warning?
                raise orchestrator.OrchestratorError(
                    f"Could not mark {osd} down")

            # stop and remove daemon
            assert osd.hostname is not None

            if self.mgr.cache.has_daemon(f'osd.{osd.osd_id}'):
                CephadmServe(self.mgr)._remove_daemon(f'osd.{osd.osd_id}', osd.hostname)
                logger.info(f"Successfully removed {osd} on {osd.hostname}")
            else:
                logger.info(f"Daemon {osd} on {osd.hostname} was already removed")

            if osd.replace:
                # mark destroyed in osdmap
                if not osd.destroy():
                    raise orchestrator.OrchestratorError(
                        f"Could not destroy {osd}")
                logger.info(
                    f"Successfully destroyed old {osd} on {osd.hostname}; ready for replacement")
            else:
                # purge from osdmap
                if not osd.purge():
                    raise orchestrator.OrchestratorError(f"Could not purge {osd}")
                logger.info(f"Successfully purged {osd} on {osd.hostname}")

            if osd.zap:
                # throws an exception if the zap fails
                logger.info(f"Zapping devices for {osd} on {osd.hostname}")
                osd.do_zap()
                logger.info(f"Successfully zapped devices for {osd} on {osd.hostname}")

            logger.debug(f"Removing {osd} from the queue.")

        # self could change while this is processing (osds get added from the CLI)
        # The new set is: 'an intersection of all osds that are still not empty/removed (new_queue) and
        # osds that were added while this method was executed'
        with self.lock:
            self.osds.intersection_update(new_queue)
            self._save_to_store()

    def cleanup(self) -> None:
        # OSDs can always be cleaned up manually. This ensures that we run on existing OSDs
        with self.lock:
            for osd in self._not_in_cluster():
                self.osds.remove(osd)

    def _ready_to_drain_osds(self) -> List["OSD"]:
        """
        Returns OSDs that are ok to stop and not yet draining. Only returns as many OSDs as can
        be accomodated by the 'max_osd_draining_count' config value, considering the number of OSDs
        that are already draining.
        """
        draining_limit = max(1, self.mgr.max_osd_draining_count)
        num_already_draining = len(self.draining_osds())
        num_to_start_draining = max(0, draining_limit - num_already_draining)
        stoppable_osds = self.rm_util.find_osd_stop_threshold(self.idling_osds())
        return [] if stoppable_osds is None else stoppable_osds[:num_to_start_draining]

    def _save_to_store(self) -> None:
        osd_queue = [osd.to_json() for osd in self.osds]
        logger.debug(f"Saving {osd_queue} to store")
        self.mgr.set_store('osd_remove_queue', json.dumps(osd_queue))

    def load_from_store(self) -> None:
        with self.lock:
            for k, v in self.mgr.get_store_prefix('osd_remove_queue').items():
                for osd in json.loads(v):
                    logger.debug(f"Loading osd ->{osd} from store")
                    osd_obj = OSD.from_json(osd, rm_util=self.rm_util)
                    if osd_obj is not None:
                        self.osds.add(osd_obj)

    def as_osd_ids(self) -> List[int]:
        with self.lock:
            return [osd.osd_id for osd in self.osds]

    def queue_size(self) -> int:
        with self.lock:
            return len(self.osds)

    def draining_osds(self) -> List["OSD"]:
        with self.lock:
            return [osd for osd in self.osds if osd.is_draining]

    def idling_osds(self) -> List["OSD"]:
        with self.lock:
            return [osd for osd in self.osds if not osd.is_draining and not osd.is_empty]

    def empty_osds(self) -> List["OSD"]:
        with self.lock:
            return [osd for osd in self.osds if osd.is_empty]

    def all_osds(self) -> List["OSD"]:
        with self.lock:
            return [osd for osd in self.osds]

    def _not_in_cluster(self) -> List["OSD"]:
        return [osd for osd in self.osds if not osd.exists]

    def enqueue(self, osd: "OSD") -> None:
        if not osd.exists:
            raise NotFoundError()
        with self.lock:
            self.osds.add(osd)
        osd.start()

    def rm_by_osd_id(self, osd_id: int) -> None:
        osd: Optional["OSD"] = None
        for o in self.osds:
            if o.osd_id == osd_id:
                osd = o
        if not osd:
            logger.debug(f"Could not find osd with id {osd_id} in queue.")
            raise KeyError(f'No osd with id {osd_id} in removal queue')
        self.rm(osd)

    def rm(self, osd: "OSD") -> None:
        if not osd.exists:
            raise NotFoundError()
        osd.stop()
        with self.lock:
            try:
                logger.debug(f'Removing {osd} from the queue.')
                self.osds.remove(osd)
            except KeyError:
                logger.debug(f"Could not find {osd} in queue.")
                raise KeyError

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, OSDRemovalQueue):
            return False
        with self.lock:
            return self.osds == other.osds
