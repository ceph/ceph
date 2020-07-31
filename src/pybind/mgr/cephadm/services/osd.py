import json
import logging
from typing import List, Dict, Any, Set, Union, Tuple, cast, Optional

from ceph.deployment import translate
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.drive_selection import DriveSelection

from datetime import datetime
import orchestrator
from cephadm.utils import forall_hosts
from orchestrator import OrchestratorError
from mgr_module import MonCommandFailed

from cephadm.services.cephadmservice import CephadmService, CephadmDaemonSpec

logger = logging.getLogger(__name__)
DATEFMT = '%Y-%m-%dT%H:%M:%S.%f'


class OSDService(CephadmService):
    TYPE = 'osd'

    def create_from_spec(self, drive_group: DriveGroupSpec) -> str:
        logger.debug(f"Processing DriveGroup {drive_group}")
        osd_id_claims = self.find_destroyed_osds()
        logger.info(f"Found osd claims for drivegroup {drive_group.service_id} -> {osd_id_claims}")

        @forall_hosts
        def create_from_spec_one(host: str, drive_selection: DriveSelection) -> Optional[str]:
            logger.info('Applying %s on host %s...' % (drive_group.service_id, host))
            cmd = self.driveselection_to_ceph_volume(drive_selection,
                                                     osd_id_claims.get(host, []))
            if not cmd:
                logger.debug("No data_devices, skipping DriveGroup: {}".format(drive_group.service_id))
                return None
            env_vars: List[str] = [f"CEPH_VOLUME_OSDSPEC_AFFINITY={drive_group.service_id}"]
            ret_msg = self.create_single_host(
                host, cmd, replace_osd_ids=osd_id_claims.get(host, []), env_vars=env_vars
            )
            return ret_msg

        ret = create_from_spec_one(self.prepare_drivegroup(drive_group))
        return ", ".join(filter(None, ret))

    def create_single_host(self, host: str, cmd: str, replace_osd_ids=None, env_vars: Optional[List[str]] = None) -> str:
        out, err, code = self._run_ceph_volume_command(host, cmd, env_vars=env_vars)

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
                daemon_spec: CephadmDaemonSpec = CephadmDaemonSpec(
                    daemon_id=osd_id,
                    host=host,
                    daemon_type='osd',
                )
                self.mgr._create_daemon(
                    daemon_spec,
                    osd_uuid_map=osd_uuid_map)

        if created:
            self.mgr.cache.invalidate_host_devices(host)
            return "Created osd(s) %s on host '%s'" % (','.join(created), host)
        else:
            return "Created no osd(s) on host %s; already created?" % host

    def prepare_drivegroup(self, drive_group: DriveGroupSpec) -> List[Tuple[str, DriveSelection]]:
        # 1) use fn_filter to determine matching_hosts
        matching_hosts = drive_group.placement.filter_matching_hosts(self.mgr._get_hosts)
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

    def driveselection_to_ceph_volume(self,
                                      drive_selection: DriveSelection,
                                      osd_id_claims: Optional[List[str]] = None,
                                      preview: bool = False) -> Optional[str]:
        logger.debug(f"Translating DriveGroup <{drive_selection.spec}> to ceph-volume command")
        cmd: Optional[str] = translate.to_ceph_volume(drive_selection,
                                                      osd_id_claims, preview=preview).run()
        logger.debug(f"Resulting ceph-volume cmd: {cmd}")
        return cmd

    def get_previews(self, host) -> List[Dict[str, Any]]:
        # Find OSDSpecs that match host.
        osdspecs = self.resolve_osdspecs_for_host(host)
        return self.generate_previews(osdspecs, host)

    def generate_previews(self, osdspecs: List[DriveGroupSpec], for_host: str) -> List[Dict[str, Any]]:
        """

        The return should look like this:

        [
          {'data': {<metadata>},
           'osdspec': <name of osdspec>,
           'host': <name of host>
           },

           {'data': ...,
            'osdspec': ..,
            'host': ..
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
            osd_id_claims = self.find_destroyed_osds()

            # prepare driveselection
            for host, ds in self.prepare_drivegroup(osdspec):
                if host != for_host:
                    continue

                # driveselection for host
                cmd = self.driveselection_to_ceph_volume(ds,
                                                         osd_id_claims.get(host, []),
                                                         preview=True)
                if not cmd:
                    logger.debug("No data_devices, skipping DriveGroup: {}".format(
                        osdspec.service_name()))
                    continue

                # get preview data from ceph-volume
                out, err, code = self._run_ceph_volume_command(host, cmd)
                if out:
                    concat_out: Dict[str, Any] = json.loads(" ".join(out))
                    ret_all.append({'data': concat_out,
                                    'osdspec': osdspec.service_id,
                                    'host': host})
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
        return sum([spec.placement.filter_matching_hosts(self.mgr._get_hosts) for spec in osdspecs], [])

    def resolve_osdspecs_for_host(self, host: str, specs: Optional[List[DriveGroupSpec]] = None):
        matching_specs = []
        self.mgr.log.debug(f"Finding OSDSpecs for host: <{host}>")
        if not specs:
            specs = [cast(DriveGroupSpec, spec) for (sn, spec) in self.mgr.spec_store.spec_preview.items()
                     if spec.service_type == 'osd']
        for spec in specs:
            if host in spec.placement.filter_matching_hosts(self.mgr._get_hosts):
                self.mgr.log.debug(f"Found OSDSpecs for host: <{host}> -> <{spec}>")
                matching_specs.append(spec)
        return matching_specs

    def _run_ceph_volume_command(self, host: str,
                                 cmd: str, env_vars: Optional[List[str]] = None
                                 ) -> Tuple[List[str], List[str], int]:
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
            env_vars=env_vars,
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
        self.mgr.log.info(
            f"Found osd claims -> {osd_host_map}")
        return osd_host_map


class RemoveUtil(object):
    def __init__(self, mgr):
        self.mgr = mgr

    def process_removal_queue(self) -> None:
        """
        Performs actions in the _serve() loop to remove an OSD
        when criteria is met.
        """

        # make sure that we don't run on OSDs that are not in the cluster anymore.
        self.cleanup()

        logger.debug(
            f"{self.mgr.to_remove_osds.queue_size()} OSDs are scheduled "
            f"for removal: {self.mgr.to_remove_osds.all_osds()}")

        # find osds that are ok-to-stop and not yet draining
        ok_to_stop_osds = self.find_osd_stop_threshold(self.mgr.to_remove_osds.idling_osds())
        if ok_to_stop_osds:
            # start draining those
            _ = [osd.start_draining() for osd in ok_to_stop_osds]

        # Check all osds for their state and take action (remove, purge etc)
        to_remove_osds = self.mgr.to_remove_osds.all_osds()
        new_queue = set()
        for osd in to_remove_osds:
            if not osd.force:
                # skip criteria
                if not osd.is_empty:
                    logger.info(f"OSD <{osd.osd_id}> is not empty yet. Waiting a bit more")
                    new_queue.add(osd)
                    continue

            if not osd.safe_to_destroy():
                logger.info(
                    f"OSD <{osd.osd_id}> is not safe-to-destroy yet. Waiting a bit more")
                new_queue.add(osd)
                continue

            # abort criteria
            if not osd.down():
                # also remove it from the remove_osd list and set a health_check warning?
                raise orchestrator.OrchestratorError(
                    f"Could not set OSD <{osd.osd_id}> to 'down'")

            if osd.replace:
                if not osd.destroy():
                    raise orchestrator.OrchestratorError(
                        f"Could not destroy OSD <{osd.osd_id}>")
            else:
                if not osd.purge():
                    raise orchestrator.OrchestratorError(f"Could not purge OSD <{osd.osd_id}>")

            if not osd.exists:
                continue
            self.mgr._remove_daemon(osd.fullname, osd.nodename)
            logger.info(f"Successfully removed OSD <{osd.osd_id}> on {osd.nodename}")
            logger.debug(f"Removing {osd.osd_id} from the queue.")

        # self.mgr.to_remove_osds could change while this is processing (osds get added from the CLI)
        # The new set is: 'an intersection of all osds that are still not empty/removed (new_queue) and
        # osds that were added while this method was executed'
        self.mgr.to_remove_osds.intersection_update(new_queue)
        self.save_to_store()

    def cleanup(self):
        # OSDs can always be cleaned up manually. This ensures that we run on existing OSDs
        not_in_cluster_osds = self.mgr.to_remove_osds.not_in_cluster()
        [self.mgr.to_remove_osds.remove(osd) for osd in not_in_cluster_osds]

    def get_osds_in_cluster(self) -> List[str]:
        osd_map = self.mgr.get_osdmap()
        return [str(x.get('osd')) for x in osd_map.dump().get('osds', [])]

    def osd_df(self) -> dict:
        base_cmd = 'osd df'
        ret, out, err = self.mgr.mon_command({
            'prefix': base_cmd,
            'format': 'json'
        })
        return json.loads(out)

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
                self.mgr.log.info("Can't even stop one OSD. Cluster is probably busy. Retrying later..")
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
        return self._run_mon_cmd(cmd_args)

    def set_osd_flag(self, osds: List["OSD"], flag: str) -> bool:
        base_cmd = f"osd {flag}"
        self.mgr.log.debug(f"running cmd: {base_cmd} on ids {osds}")
        ret, out, err = self.mgr.mon_command({
            'prefix': base_cmd,
            'ids': [str(osd.osd_id) for osd in osds]
        })
        if ret != 0:
            self.mgr.log.error(f"Could not set <{flag}> flag for osds: {osds}. <{err}>")
            return False
        self.mgr.log.info(f"OSDs <{osds}> are now <{flag}>")
        return True

    def safe_to_destroy(self, osd_ids: List[int]) -> bool:
        """ Queries the safe-to-destroy flag for OSDs """
        cmd_args = {'prefix': 'osd safe-to-destroy',
                    'ids': [str(x) for x in osd_ids]}
        return self._run_mon_cmd(cmd_args)

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

    def save_to_store(self):
        osd_queue = [osd.to_json() for osd in self.mgr.to_remove_osds.all_osds()]
        logger.debug(f"Saving {osd_queue} to store")
        self.mgr.set_store('osd_remove_queue', json.dumps(osd_queue))

    def load_from_store(self):
        for k, v in self.mgr.get_store_prefix('osd_remove_queue').items():
            for osd in json.loads(v):
                logger.debug(f"Loading osd ->{osd} from store")
                osd_obj = OSD.from_json(json.loads(osd), ctx=self)
                self.mgr.to_remove_osds.add(osd_obj)


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
                 fullname: Optional[str] = None,
                 ):
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
        self.nodename = hostname
        # The full name of the osd
        self.fullname = fullname

        # mgr obj to make mgr/mon calls
        self.rm_util = remove_util

    def start(self) -> None:
        if self.started:
            logger.debug(f"Already started draining {self}")
            return None
        self.started = True
        self.stopped = False

    def start_draining(self) -> bool:
        if self.stopped:
            logger.debug(f"Won't start draining {self}. OSD draining is stopped.")
            return False
        self.rm_util.set_osd_flag([self], 'out')
        self.drain_started_at = datetime.utcnow()
        self.draining = True
        logger.debug(f"Started draining {self}.")
        return True

    def stop_draining(self) -> bool:
        self.rm_util.set_osd_flag([self], 'in')
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

    def purge(self) -> bool:
        return self.rm_util.purge_osd(self.osd_id)

    def get_pg_count(self) -> int:
        return self.rm_util.get_pg_count(self.osd_id)

    @property
    def exists(self) -> bool:
        return str(self.osd_id) in self.rm_util.get_osds_in_cluster()

    def drain_status_human(self):
        default_status = 'not started'
        status = 'started' if self.started and not self.draining else default_status
        status = 'draining' if self.draining else status
        status = 'done, waiting for purge' if self.drain_done_at and not self.draining else status
        return status

    def pg_count_str(self):
        return 'n/a' if self.get_pg_count() < 0 else str(self.get_pg_count())

    def to_json(self) -> str:
        out = dict()
        out['osd_id'] = self.osd_id
        out['started'] = self.started
        out['draining'] = self.draining
        out['stopped'] = self.stopped
        out['replace'] = self.replace
        out['force'] = self.force
        out['nodename'] = self.nodename  # type: ignore

        for k in ['drain_started_at', 'drain_stopped_at', 'drain_done_at', 'process_started_at']:
            if getattr(self, k):
                out[k] = getattr(self, k).strftime(DATEFMT)
            else:
                out[k] = getattr(self, k)
        return json.dumps(out)

    @classmethod
    def from_json(cls, inp: Optional[Dict[str, Any]], ctx: Optional[RemoveUtil] = None) -> Optional["OSD"]:
        if not inp:
            return None
        for date_field in ['drain_started_at', 'drain_stopped_at', 'drain_done_at', 'process_started_at']:
            if inp.get(date_field):
                inp.update({date_field: datetime.strptime(inp.get(date_field, ''), DATEFMT)})
        inp.update({'remove_util': ctx})
        return cls(**inp)

    def __hash__(self):
        return hash(self.osd_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OSD):
            return NotImplemented
        return self.osd_id == other.osd_id

    def __repr__(self) -> str:
        return f"<OSD>(osd_id={self.osd_id}, is_draining={self.is_draining})"


class OSDQueue(Set):

    def __init__(self):
        super().__init__()

    def as_osd_ids(self) -> List[int]:
        return [osd.osd_id for osd in self]

    def queue_size(self) -> int:
        return len(self)

    def draining_osds(self) -> List["OSD"]:
        return [osd for osd in self if osd.is_draining]

    def idling_osds(self) -> List["OSD"]:
        return [osd for osd in self if not osd.is_draining and not osd.is_empty]

    def empty_osds(self) -> List["OSD"]:
        return [osd for osd in self if osd.is_empty]

    def all_osds(self) -> List["OSD"]:
        return [osd for osd in self]

    def not_in_cluster(self) -> List["OSD"]:
        return [osd for osd in self if not osd.exists]

    def enqueue(self, osd: "OSD") -> None:
        if not osd.exists:
            raise NotFoundError()
        self.add(osd)
        osd.start()

    def rm(self, osd: "OSD") -> None:
        if not osd.exists:
            raise NotFoundError()
        osd.stop()
        try:
            logger.debug(f'Removing {osd} from the queue.')
            self.remove(osd)
        except KeyError:
            logger.debug(f"Could not find {osd} in queue.")
            raise KeyError
