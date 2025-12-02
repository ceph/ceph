"""
performance stats for ceph filesystem (for now...)
"""

import json
from typing import List, Dict
from xml.dom.minidom import parseString

from .cli import StatsCLICommand

from mgr_module import MgrModule, Option, NotifyType

from .fs.perf_stats import FSPerfStats

try:
    from dicttoxml import dicttoxml # type: ignore[import-not-found] 
except ImportError:
    dicttoxml = None

class Module(MgrModule):
    CLICommand = StatsCLICommand
    COMMANDS = [
        {
            "cmd": "fs perf stats "
                   "name=mds_rank,type=CephString,req=false "
                   "name=client_id,type=CephString,req=false "
                   "name=client_ip,type=CephString,req=false ",
            "desc": "retrieve ceph fs performance stats",
            "perm": "r"
        },
    ]
    MODULE_OPTIONS: List[Option] = []
    NOTIFY_TYPES = [NotifyType.command, NotifyType.fs_map]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.fs_perf_stats = FSPerfStats(self)

    def notify(self, notify_type: NotifyType, notify_id):
        if notify_type == NotifyType.command:
            self.fs_perf_stats.notify_cmd(notify_id)
        elif notify_type == NotifyType.fs_map:
            self.fs_perf_stats.notify_fsmap()

    def handle_command(self, inbuf, cmd):
        prefix = cmd['prefix']
        # only supported command is `fs perf stats` right now
        if prefix.startswith('fs perf stats'):
            result = self.fs_perf_stats.get_perf_data(cmd)
            if 'format' in cmd:
                if cmd['format'] == 'json-pretty':
                    return 0, json.dumps(result, indent=2), ""
                elif cmd['format'] == 'xml':
                    if dicttoxml is None:
                        raise ImportError("dicttoxml package required for xml")
                    result = json.loads(json.dumps(result, default=str))
                    return dicttoxml(result)
                elif cmd['format'] == 'xml-pretty':
                    if dicttoxml is None:
                        raise ImportError("dicttoxml package required for xml")
                    res_xml = parseString(dicttoxml(result))
                    return res_xml.toprettyxml()
            return 0, json.dumps(result), ""
        raise NotImplementedError(cmd['prefix'])
