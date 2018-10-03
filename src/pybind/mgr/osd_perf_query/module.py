
"""
osd_perf_query module
"""

from mgr_module import MgrModule


class OSDPerfQuery(MgrModule):
    COMMANDS = [
        {
            "cmd": "osd perf query add "
                   "name=query,type=CephString,req=true",
            "desc": "add osd perf query",
            "perm": "w"
        },
        {
            "cmd": "osd perf query remove "
                   "name=query_id,type=CephInt,req=true",
            "desc": "remove osd perf query",
            "perm": "w"
        },
    ]

    def handle_command(self, inbuf, cmd):
        if cmd['prefix'] == "osd perf query add":
            query_id = self.add_osd_perf_query(cmd['query'])
            return 0, str(query_id), "added query " + cmd['query'] + " with id " + str(query_id)
        elif cmd['prefix'] == "osd perf query remove":
            self.remove_osd_perf_query(cmd['query_id'])
            return 0, "", "removed query with id " + str(cmd['query_id'])
        else:
            raise NotImplementedError(cmd['prefix'])

