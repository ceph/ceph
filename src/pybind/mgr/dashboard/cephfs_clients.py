
import json

from mgr_module import CommandResult
from .remote_view_cache import RemoteViewCache


class CephFSClients(RemoteViewCache):
    def __init__(self, module_inst, fscid):
        super(CephFSClients, self).__init__(module_inst)

        self.fscid = fscid

    def _get(self):
        mds_spec = "{0}:0".format(self.fscid)
        result = CommandResult("")
        self._module.send_command(result, "mds", mds_spec,
               json.dumps({
                   "prefix": "session ls",
                   }),
               "")
        r, outb, outs = result.wait()
        # TODO handle nonzero returns, e.g. when rank isn't active
        assert r == 0
        return json.loads(outb)
