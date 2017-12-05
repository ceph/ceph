
"""
Pulling smart data from OSD
"""

import json
from mgr_module import MgrModule, CommandResult


class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "osd smart get "
                   "name=osd_id,type=CephString,req=true",
            "desc": "Get smart data for osd.id",
            "perm": "r"
        },
    ]

    def handle_command(self, cmd):
        self.log.error("handle_command")

        if cmd['prefix'] == 'osd smart get':
            result = CommandResult('')
            self.send_command(result, 'osd', cmd['osd_id'], json.dumps({
                'prefix': 'smart',
                'format': 'json',
            }), '')
            r, outb, outs = result.wait()
            return (r, outb, outs)

        else:
            # mgr should respect our self.COMMANDS and not call us for
            # any prefix we don't advertise
            raise NotImplementedError(cmd['prefix'])
