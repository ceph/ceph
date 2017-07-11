
"""
Balance PG distribution across OSDs.
"""

import json
import time
from mgr_module import MgrModule, CommandResult
from threading import Event

# available modes: 'none', 'crush', 'crush-compat', 'upmap', 'osd_weight'
default_mode = 'upmap'
default_sleep_interval = 30   # seconds
default_max_misplaced = .03   # max ratio of pgs replaced at a time

class Module(MgrModule):
    run = True

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.event = Event()

    def handle_command(self, command):
        return (-errno.EINVAL, '',
                "Command not found '{0}'".format(command['prefix']))

    def shutdown(self):
        self.log.info('Stopping')
        self.run = False
        self.event.set()

    def serve(self):
        self.log.info('Starting')
        while self.run:
            mode = self.get_config('mode', default_mode)
            sleep_interval = float(self.get_config('sleep_interval',
                                                   default_sleep_interval))
            max_misplaced = float(self.get_config('max_misplaced',
                                                  default_max_misplaced))
            self.log.info('Mode %s, sleep interval %f, max misplaced %f' %
                          (mode, sleep_interval, max_misplaced))

            info = self.get('pg_status')
            if info.get('unknown_ratio', 0.0) + info.get('degraded_ratio', 0.0) + info.get('inactive_ratio', 0.0) > 0.0:
                self.log.info('Some PGs are unknown, degraded, or inactive; waiting')
            elif info.get('misplaced_ratio', 0.0) >= max_misplaced:
                self.log.info('Too many PGs (%f > max %f) are misplaced; waiting' % (info.get('misplaced_ratio', 0.0), max_misplaced))
            else:
                if mode == 'upmap':
                    self.do_upmap()
                elif mode == 'crush':
                    self.do_crush(compat=False)
                elif mode == 'crush-compat':
                    self.do_crush(compat=True)
                elif mode == 'osd_weight':
                    self.osd_weight()
                elif mode == 'none':
                    self.log.info('Idle')
                else:
                    self.log.info('Unrecognized mode %s' % mode)
            self.event.wait(sleep_interval)

    def do_upmap(self):
        self.log.info('do_upmap')
        max_iterations = self.get_config('upmap_max_iterations', 10)
        max_deviation = self.get_config('upmap_max_deviation', .01)
        osdmap = self.get_osdmap()
        inc = osdmap.new_incremental()
        osdmap.calc_pg_upmaps(inc, max_deviation, max_iterations, [])
        incdump = inc.dump()
        self.log.info('inc is %s' % incdump)

        for pgid in incdump.get('old_pg_upmap_items', []):
            self.log.info('ceph osd rm-pg-upmap-items %s', pgid)
            result = CommandResult('foo')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd rm-pg-upmap-items',
                'format': 'json',
                'pgid': pgid,
            }), 'foo')
            r, outb, outs = result.wait()
            if r != 0:
                self.log.error('Error removing pg-upmap on %s' % pgid)
                break;

        for item in incdump.get('new_pg_upmap_items', []):
            self.log.info('ceph osd pg-upmap-items %s mappings %s', item['pgid'],
                          item['mappings'])
            osdlist = []
            for m in item['mappings']:
                osdlist += [m['from'], m['to']]
            result = CommandResult('foo')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd pg-upmap-items',
                'format': 'json',
                'pgid': item['pgid'],
                'id': osdlist,
            }), 'foo')
            r, outb, outs = result.wait()
            if r != 0:
                self.log.error('Error setting pg-upmap on %s' % item['pgid'])
                break;


    def do_crush(self, compat):
        self.log.info('do_crush compat=%b' % compat)

    def do_osd_weight(self):
        self.log.info('do_osd_weight')
