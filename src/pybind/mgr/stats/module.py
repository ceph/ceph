"""
performance stats for ceph filesystem (for now...)
"""

import json
from typing import List, Dict

from mgr_module import MgrModule, Option, NotifyType

from .fs.perf_stats import FSPerfStats

class Module(MgrModule):
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
    NOTIFY_TYPES = [NotifyType.command]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.fs_perf_stats = FSPerfStats(self)

    def notify(self, notify_type: NotifyType, notify_id):
        if notify_type == NotifyType.command:
            self.fs_perf_stats.notify(notify_id)

    def handle_command(self, inbuf, cmd):
        prefix = cmd['prefix']
        # only supported command is `fs perf stats` right now
        if prefix.startswith('fs perf stats'):
            return self.fs_perf_stats.get_perf_data(cmd)
        raise NotImplementedError(cmd['prefix'])
