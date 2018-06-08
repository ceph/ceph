
"""
Device health monitoring
"""

import json
from mgr_module import MgrModule, CommandResult
import rados
from threading import Event
from datetime import datetime, timedelta, date, time

class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "device query-daemon-health-metrics "
                   "name=osd_id,type=CephOsdName,req=true",
            "desc": "Get device health metrics for a given daemon (OSD)",
            "perm": "r"
        },
    ]

    def handle_command(self, inbuf, cmd):
        self.log.error("handle_command")

        if cmd['prefix'] == 'device query-daemon-health-metrics':
            result = CommandResult('')
            self.send_command(result, 'osd', str(cmd['osd_id']), json.dumps({
                'prefix': 'smart',
                'format': 'json',
            }), '')
            r, outb, outs = result.wait()
            return (r, outb, outs)

        else:
            # mgr should respect our self.COMMANDS and not call us for
            # any prefix we don't advertise
            raise NotImplementedError(cmd['prefix'])
