
from mgr_module import MgrModule

import orchestrator

class OrchestratorCli(MgrModule):
    COMMANDS = [
            {
                'cmd': "orchestrator list devices "
                "name=node,type=CephString,req=false",
                "desc": "List devices on a node",
                "perm": "r"
                }
            ]

    def _select_orchestrator(self):
        # TODO: auto or config based mechanism
        # to work out which module is enabled for orchestration.
        # (complain if someone has tried to enable multiple?)
        return "rook"

    def _list_devices(self, cmd):
        node = cmd.get('node', None)

        args = {}
        if node:
            nf = orchestrator.NodeFilter()
            nf.nodes = [node]
            args = {"node_filter": nf}

        completion = self.remote(
                self._select_orchestrator(),
                "get_inventory",
                args)

        # OK, this is actually really weird to do over a remote
        # interface because the Orchestrator interface is trying
        # to tag the results onto the completion instance
        # we passed in.

        self.remote(self._select_orchestrator(),
                "wait",
                {"completions": [completion]})
        
        # Spit out a human readable version
        result = ""

        for inventory_node in completion.result:
            result += "{0}:\n".format(inventory_node.name)
            for d in inventory_node.devices:
                result += "{0} ({1}, {2}b)\n".format(
                        d.id, d.type, d.size)
            result += "\n"

        return 0, result, ""


    def handle_command(self, cmd):
        if cmd['prefix'] == "orchestrator list devices":
            self._list_devices(cmd)
        else:
            raise NotImplementedError()
