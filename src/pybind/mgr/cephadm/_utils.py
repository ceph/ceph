import json
try:
    from typing import List
except ImportError:
    pass

from orchestrator import OrchestratorError


class RemoveUtil(object):
    def __init__(self, mgr):
        self.mgr = mgr

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
