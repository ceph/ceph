import datetime
import json
import logging
import time

from typing import List, Dict, Any, Set, Union

import orchestrator
from orchestrator import OrchestratorError

logger = logging.getLogger(__name__)


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

            self.mgr._remove_daemon(osd.fullname, osd.nodename, True)
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
