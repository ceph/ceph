"""
performance stats for ceph filesystem (for now...)
"""

import json
import threading
from typing import List, Dict

from mgr_module import MgrModule, Option

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

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.fs_perf_stats = FSPerfStats(self)
        try:
            self.prev_mds_name_rank0 = self.get_rank0_mds_name(self.get('fs_map'))
        except Exception:
            raise

    def notify(self, notify_type, notify_id):
        if notify_type == "command":
            self.fs_perf_stats.notify(notify_id)
        elif notify_type == "fs_map":
            try:
                mds_name_rank0 = self.get_rank0_mds_name(self.get('fs_map'))
                if (mds_name_rank0 != self.prev_mds_name_rank0):
                    threading.Timer(0, self.fs_perf_stats.re_register_queries).start()
                    self.prev_mds_name_rank0 = mds_name_rank0
            except RuntimeError as e:
                log.warn(e)

    def handle_command(self, inbuf, cmd):
        prefix = cmd['prefix']
        # only supported command is `fs perf stats` right now
        if prefix.startswith('fs perf stats'):
            return self.fs_perf_stats.get_perf_data(cmd)
        raise NotImplementedError(cmd['prefix'])

    def get_rank0_mds_name(self, fsmap):
        for fs in fsmap['filesystems']:
            mds_map = fs['mdsmap']
            if mds_map is not None:
                for mds_id, mds_status in mds_map['info'].items():
                    if mds_status['rank'] == 0:
                        return mds_status['name']
        raise RuntimeError("Failed to find a rank0 mds in fsmap")
