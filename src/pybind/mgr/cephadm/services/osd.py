import datetime
import json
import logging
from typing import List, Dict, Any, Set, Union, Tuple, cast, Optional

from ceph.deployment import translate
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.drive_selection import DriveSelection

import orchestrator
from orchestrator import OrchestratorError
from mgr_module import MonCommandFailed

from cephadm.services.cephadmservice import CephadmService


logger = logging.getLogger(__name__)


class OSDService(CephadmService):
    def create_from_spec(self, drive_group: DriveGroupSpec) -> str:
        logger.debug(f"Processing DriveGroup {drive_group}")
        ret = []
        drive_group.osd_id_claims = self.find_destroyed_osds()
        logger.info(f"Found osd claims for drivegroup {drive_group.service_id} -> {drive_group.osd_id_claims}")
        for host, drive_selection in self.prepare_drivegroup(drive_group):
            logger.info('Applying %s on host %s...' % (drive_group.service_id, host))
            cmd = self.driveselection_to_ceph_volume(drive_group, drive_selection,
                                                     drive_group.osd_id_claims.get(host, []))
            if not cmd:
                logger.debug("No data_devices, skipping DriveGroup: {}".format(drive_group.service_id))
                continue
            ret_msg = self.create(host, cmd,
                                              replace_osd_ids=drive_group.osd_id_claims.get(host, []))
            ret.append(ret_msg)
        return ", ".join(ret)
        
    def create(self, host: str, cmd: str, replace_osd_ids=None) -> str:
        out, err, code = self._run_ceph_volume_command(host, cmd)

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

        # check result
        out, err, code = self.mgr._run_cephadm(
            host, 'osd', 'ceph-volume',
            [
                '--',
                'lvm', 'list',
                '--format', 'json',
            ])
        before_osd_uuid_map = self.mgr.get_osd_uuid_map(only_up=True)
        osds_elems = json.loads('\n'.join(out))
        fsid = self.mgr._cluster_fsid
        osd_uuid_map = self.mgr.get_osd_uuid_map()
        created = []
        for osd_id, osds in osds_elems.items():
            for osd in osds:
                if osd['tags']['ceph.cluster_fsid'] != fsid:
                    logger.debug('mismatched fsid, skipping %s' % osd)
                    continue
                if osd_id in before_osd_uuid_map and osd_id not in replace_osd_ids:
                    # if it exists but is part of the replacement operation, don't skip
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
                self.mgr._create_daemon(
                    'osd', osd_id, host,
                    osd_uuid_map=osd_uuid_map)

        if created:
            self.mgr.cache.invalidate_host_devices(host)
            return "Created osd(s) %s on host '%s'" % (','.join(created), host)
        else:
            return "Created no osd(s) on host %s; already created?" % host

    def prepare_drivegroup(self, drive_group: DriveGroupSpec) -> List[
        Tuple[str, DriveSelection]]:
        # 1) use fn_filter to determine matching_hosts
        matching_hosts = drive_group.placement.pattern_matches_hosts(
            [x for x in self.mgr.cache.get_hosts()])
        # 2) Map the inventory to the InventoryHost object
        host_ds_map = []

        # set osd_id_claims

        def _find_inv_for_host(hostname: str, inventory_dict: dict):
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
            drive_selection = DriveSelection(drive_group, inventory_for_host)
            logger.debug(f"Found drive selection {drive_selection}")
            host_ds_map.append((host, drive_selection))
        return host_ds_map

    def driveselection_to_ceph_volume(self, drive_group: DriveGroupSpec,
                                      drive_selection: DriveSelection,
                                      osd_id_claims: Optional[List[str]] = None,
                                      preview: bool = False) -> Optional[str]:
        logger.debug(f"Translating DriveGroup <{drive_group}> to ceph-volume command")
        cmd: Optional[str] = translate.to_ceph_volume(drive_group, drive_selection,
                                                      osd_id_claims, preview=preview).run()
        logger.debug(f"Resulting ceph-volume cmd: {cmd}")
        return cmd

    def preview_drivegroups(self, drive_group_name: Optional[str] = None,
                            dg_specs: Optional[List[DriveGroupSpec]] = None) -> List[
        Dict[str, Dict[Any, Any]]]:
        # find drivegroups
        if drive_group_name:
            drive_groups = cast(List[DriveGroupSpec],
                                self.mgr.spec_store.find(service_name=drive_group_name))
        elif dg_specs:
            drive_groups = dg_specs
        else:
            drive_groups = []
        ret_all = []
        for drive_group in drive_groups:
            drive_group.osd_id_claims = self.find_destroyed_osds()
            logger.info(
                f"Found osd claims for drivegroup {drive_group.service_id} -> {drive_group.osd_id_claims}")
            # prepare driveselection
            for host, ds in self.prepare_drivegroup(drive_group):
                cmd = self.driveselection_to_ceph_volume(drive_group, ds,
                                                         drive_group.osd_id_claims.get(host,
                                                                                       []),
                                                         preview=True)
                if not cmd:
                    logger.debug("No data_devices, skipping DriveGroup: {}".format(
                        drive_group.service_name()))
                    continue
                out, err, code = self._run_ceph_volume_command(host, cmd)
                if out:
                    concat_out = json.loads(" ".join(out))
                    ret_all.append({'data': concat_out, 'drivegroup': drive_group.service_id,
                                    'host': host})
        return ret_all

    def _run_ceph_volume_command(self, host: str, cmd: str) -> Tuple[List[str], List[str], int]:
        self.mgr.inventory.assert_host(host)

        # get bootstrap key
        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get',
            'entity': 'client.bootstrap-osd',
        })

        # generate config
        ret, config, err = self.mgr.check_mon_command({
            "prefix": "config generate-minimal-conf",
        })

        j = json.dumps({
            'config': config,
            'keyring': keyring,
        })

        split_cmd = cmd.split(' ')
        _cmd = ['--config-json', '-', '--']
        _cmd.extend(split_cmd)
        out, err, code = self.mgr._run_cephadm(
            host, 'osd', 'ceph-volume',
            _cmd,
            stdin=j,
            error_ok=True)
        return out, err, code

    def get_osdspec_affinity(self, osd_id: str) -> str:
        return self.mgr.get('osd_metadata').get(osd_id, {}).get('osdspec_affinity', '')

    def find_destroyed_osds(self) -> Dict[str, List[str]]:
        osd_host_map: Dict[str, List[str]] = dict()
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
        except json.decoder.JSONDecodeError:
            logger.exception(f"Could not decode json -> {out}")
            return osd_host_map

        nodes = tree.get('nodes', {})
        for node in nodes:
            if node.get('type') == 'host':
                osd_host_map.update(
                    {node.get('name'): [str(_id) for _id in node.get('children', list())]}
                )
        return osd_host_map


class OSDRemoval(object):
    def __init__(self,
                 osd_id: str,
                 replace: bool,
                 force: bool,
                 nodename: str,
                 fullname: str,
                 start_at: datetime.datetime,
                 pg_count: int):
        self.osd_id = osd_id
        self.replace = replace
        self.force = force
        self.nodename = nodename
        self.fullname = fullname
        self.started_at = start_at
        self.pg_count = pg_count

    # needed due to changing 'started_at' attr
    def __eq__(self, other):
        return self.osd_id == other.osd_id

    def __hash__(self):
        return hash(self.osd_id)

    def __repr__(self):
        return ('<OSDRemoval>(osd_id={}, replace={}, force={}, nodename={}'
                ', fullname={}, started_at={}, pg_count={})').format(
            self.osd_id, self.replace, self.force, self.nodename,
            self.fullname, self.started_at, self.pg_count)

    @property
    def pg_count_str(self) -> str:
        return 'n/a' if self.pg_count < 0 else str(self.pg_count)


class RemoveUtil(object):
    def __init__(self, mgr):
        self.mgr = mgr
        self.to_remove_osds: Set[OSDRemoval] = set()
        self.osd_removal_report: Dict[OSDRemoval, Union[int,str]] = dict()

    @property
    def report(self) -> Set[OSDRemoval]:
        return self.to_remove_osds.copy()

    def queue_osds_for_removal(self, osds: Set[OSDRemoval]):
        self.to_remove_osds.update(osds)

    def _remove_osds_bg(self) -> None:
        """
        Performs actions in the _serve() loop to remove an OSD
        when criteria is met.
        """
        logger.debug(
            f"{len(self.to_remove_osds)} OSDs are scheduled for removal: {list(self.to_remove_osds)}")
        self._update_osd_removal_status()
        remove_osds: set = self.to_remove_osds.copy()
        for osd in remove_osds:
            if not osd.force:
                self.drain_osd(osd.osd_id)
                # skip criteria
                if not self.is_empty(osd.osd_id):
                    logger.info(f"OSD <{osd.osd_id}> is not empty yet. Waiting a bit more")
                    continue

            if not self.ok_to_destroy([osd.osd_id]):
                logger.info(
                    f"OSD <{osd.osd_id}> is not safe-to-destroy yet. Waiting a bit more")
                continue

            # abort criteria
            if not self.down_osd([osd.osd_id]):
                # also remove it from the remove_osd list and set a health_check warning?
                raise orchestrator.OrchestratorError(
                    f"Could not set OSD <{osd.osd_id}> to 'down'")

            if osd.replace:
                if not self.destroy_osd(osd.osd_id):
                    # also remove it from the remove_osd list and set a health_check warning?
                    raise orchestrator.OrchestratorError(
                        f"Could not destroy OSD <{osd.osd_id}>")
            else:
                if not self.purge_osd(osd.osd_id):
                    # also remove it from the remove_osd list and set a health_check warning?
                    raise orchestrator.OrchestratorError(f"Could not purge OSD <{osd.osd_id}>")

            self.mgr._remove_daemon(osd.fullname, osd.nodename)
            logger.info(f"Successfully removed OSD <{osd.osd_id}> on {osd.nodename}")
            logger.debug(f"Removing {osd.osd_id} from the queue.")
            self.to_remove_osds.remove(osd)

    def _update_osd_removal_status(self):
        """
        Generate a OSD report that can be printed to the CLI
        """
        logger.debug("Update OSD removal status")
        for osd in self.to_remove_osds:
            osd.pg_count = self.get_pg_count(str(osd.osd_id))
        logger.debug(f"OSD removal status: {self.to_remove_osds}")

    def drain_osd(self, osd_id: str) -> bool:
        """
        Uses `osd_support` module to schedule a drain operation of an OSD
        """
        cmd_args = {
            'prefix': 'osd drain',
            'osd_ids': [int(osd_id)]
        }
        return self._run_mon_cmd(cmd_args)

    def get_pg_count(self, osd_id: str) -> int:
        """ Queries for PG count of an OSD """
        self.mgr.log.debug("Querying for drain status")
        ret, out, err = self.mgr.mon_command({
            'prefix': 'osd drain status',
        })
        if ret != 0:
            self.mgr.log.error(f"Calling osd drain status failed with {err}")
            raise OrchestratorError("Could not query `osd drain status`")
        out = json.loads(out)
        for o in out:
            if str(o.get('osd_id', '')) == str(osd_id):
                return int(o.get('pgs', -1))
        return -1

    def is_empty(self, osd_id: str) -> bool:
        """ Checks if an OSD is empty """
        return self.get_pg_count(osd_id) == 0

    def ok_to_destroy(self, osd_ids: List[int]) -> bool:
        """ Queries the safe-to-destroy flag for OSDs """
        cmd_args = {'prefix': 'osd safe-to-destroy',
                    'ids': osd_ids}
        return self._run_mon_cmd(cmd_args)

    def destroy_osd(self, osd_id: int) -> bool:
        """ Destroys an OSD (forcefully) """
        cmd_args = {'prefix': 'osd destroy-actual',
                    'id': int(osd_id),
                    'yes_i_really_mean_it': True}
        return self._run_mon_cmd(cmd_args)

    def down_osd(self, osd_ids: List[int]) -> bool:
        """ Sets `out` flag to OSDs """
        cmd_args = {
            'prefix': 'osd down',
            'ids': osd_ids,
        }
        return self._run_mon_cmd(cmd_args)

    def purge_osd(self, osd_id: int) -> bool:
        """ Purges an OSD from the cluster (forcefully) """
        cmd_args = {
            'prefix': 'osd purge-actual',
            'id': int(osd_id),
            'yes_i_really_mean_it': True
        }
        return self._run_mon_cmd(cmd_args)

    def out_osd(self, osd_ids: List[int]) -> bool:
        """ Sets `down` flag to OSDs """
        cmd_args = {
            'prefix': 'osd out',
            'ids': osd_ids,
        }
        return self._run_mon_cmd(cmd_args)

    def _run_mon_cmd(self, cmd_args: dict) -> bool:
        """
        Generic command to run mon_command and evaluate/log the results
        """
        ret, out, err = self.mgr.mon_command(cmd_args)
        if ret != 0:
            self.mgr.log.debug(f"ran {cmd_args} with mon_command")
            self.mgr.log.error(f"cmd: {cmd_args.get('prefix')} failed with: {err}. (errno:{ret})")
            return False
        self.mgr.log.debug(f"cmd: {cmd_args.get('prefix')} returns: {out}")
        return True
