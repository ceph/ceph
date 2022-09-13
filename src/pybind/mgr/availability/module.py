import time
import mgr_util
import threading
import json
from prettytable import PrettyTable
from typing import Any, Dict, List, Optional, Set, Tuple, TYPE_CHECKING, Union
from mgr_module import MgrModule, Option, CLIReadCommand

class PoolAvailability(object):
    """
    Keeps the initial and target pg_num values
    """
    def __init__(self, pool_name: str) -> None:
        now = time.time()
        self.pool_name  = pool_name
        self.started_at = now
        self.uptime = 0
        self.last_uptime = now
        self.downtime = 0
        self.last_downtime = now
        self.num_failures = 0
        self.is_avail = True

class Availability(MgrModule):
    """
    Cluster Availability.
    """
    MODULE_OPTIONS = [
        Option(
            'sleep_interval',
            default=2,
            type='secs',
            desc='how long the module is going to sleep',
            runtime=True
        ),
        Option(
            name='noavailability',
            type='bool',
            desc='global availability flag',
            long_desc=('Option to turn on/off the availability module for all pools'),
            default=False
        ),
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(Availability, self).__init__(*args, **kwargs)
        self._shutdown = threading.Event()
        self._pool_avail_map: Dict[int, PoolAvailability] = {}

    def config_notify(self) -> None:
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' mgr option %s = %s',
                           opt['name'], getattr(self, opt['name']))

    @CLIReadCommand('osd pool availability-status')
    def _command_availability_status(self, format: str = 'plain') -> Tuple[int, str, str]:
        """
        report on pool availability
        """
        #List[Dict[str, Any]]
        ps = []
        for key, value in self._pool_avail_map.items():
            mtbf = value.uptime / value.num_failures if value.num_failures > 0 else 0
            mttr = value.downtime / value.num_failures if value.num_failures > 0 else 0
            score = value.uptime / (value.uptime + value.downtime) * 100 if (value.downtime != None) and (value.uptime > 0)  else 0
            ps.append({
                'pool_name': value.pool_name,
                'uptime': value.uptime,
                'downtime': value.downtime,
                'num_failures': value.num_failures,
                'mtbf': mtbf,
                'mttr': mttr,
                'score': value.uptime / (value.uptime + value.downtime) * 100,
                'is_avail': value.is_avail
            })
        if format in ('json', 'json-pretty'):
            return 0, json.dumps(ps, indent=4, sort_keys=True), ''
        else:
            table = PrettyTable(['POOL', 'UPTIME', 'DOWNTIME', 'NUM_FAILURES', 'MTBF', 'MTTR', 'SCORE', 'AVAILABLE'], border=False)
            table.left_padding_width = 0
            table.right_padding_width = 2
            table.align['POOL'] = 'l'
            table.align['UPTIME'] = 'r'
            table.align['DOWNTIME'] = 'r'
            table.align['NUM_FAILURES'] = 'r'
            table.align['MTBF'] = 'r'
            table.align['MTTR'] = 'r'
            table.align['SCORE'] = 'r'
            table.align['AVAILABLE'] = 'r'
            for p in ps:
                table.add_row([
                    p['pool_name'],
                    str(p['uptime']),
                    str(p['downtime']),
                    str(p['num_failures']),
                    str(p['mtbf']),
                    str(p['mttr']),
                    str(p['score']),
                    str(p['is_avail']),
                ])
            return 0, table.get_string(), ''

    def serve(self) -> None:
        self.config_notify()
        while not self._shutdown.is_set():
            osdmap = self.get_osdmap()
            osdmap_pools = osdmap.get_pools()
            pool_unavailable_pgs = self.get("pool_unavailable_pgs")
            stuck_pg_stats = pool_unavailable_pgs["stuck_pg_stats"]
            per_pool_pgs_by_unavailable = pool_unavailable_pgs["per_pool_pgs_by_unavailable"]
            self.log.debug("pool_unavailable_pgs: {0}".format(
                json.dumps(pool_unavailable_pgs)
            ))

            for pool_id, p in osdmap_pools.items():
                if pool_id not in self._pool_avail_map:
                   self._pool_avail_map[pool_id] = PoolAvailability(p['pool_name'])
                   self.log.debug("Adding pool: {0}".format(p['pool_name']))

            for pool_id in list(self._pool_avail_map):
                pool_avail = self._pool_avail_map[pool_id]
                if pool_id not in osdmap_pools:
                   del self._pool_avail_map[pool_id]
                   self.log.debug("Deleting pool: {0}".format(pool_avail.pool_name))
                   continue

                unavailable_pg_count = 0
                stuck_pg_count = 0

                if str(pool_id) in per_pool_pgs_by_unavailable: 
                   unavailable_pg_count = per_pool_pgs_by_unavailable[str(pool_id)]["unavailable_pg_count"]
                if str(pool_id) in stuck_pg_stats:
                   stuck_pg_count = len(stuck_pg_stats[str(pool_id)]["pgs"])
                
                unavailable_condition = unavailable_pg_count > 0 or stuck_pg_count > 0
                now = time.time()
                if self._pool_avail_map[pool_id].is_avail:
                   if unavailable_condition:
                      # Up to Down
                      self.log.debug("Up to Down")
                      self._pool_avail_map[pool_id].is_avail = False
                      self._pool_avail_map[pool_id].num_failures += 1
                      self._pool_avail_map[pool_id].last_downtime = now
                      self._pool_avail_map[pool_id].uptime += int(now - self._pool_avail_map[pool_id].last_uptime)
                   else:
                      # Up to Up
                      self.log.debug("Up to Up")
                      self._pool_avail_map[pool_id].uptime += int(now - self._pool_avail_map[pool_id].last_uptime)
                      self._pool_avail_map[pool_id].last_uptime = now
                else:
                   if unavailable_condition:
                      # Down to Down
                      self.log.debug("Down to Down")
                      self._pool_avail_map[pool_id].downtime += int(now - self._pool_avail_map[pool_id].last_downtime)
                      self._pool_avail_map[pool_id].last_downtime = now
                   else:
                      # Down to Up
                      self.log.debug("Down to Up")
                      self._pool_avail_map[pool_id].is_avail = True
                      self._pool_avail_map[pool_id].last_uptime = now
                      self._pool_avail_map[pool_id].uptime += int(now - self._pool_avail_map[pool_id].last_downtime)
                 
                self.log.debug('pool: {0}, uptime: {1}, downtime: {2}, is_avail: {3}'.format(
                     self._pool_avail_map[pool_id].pool_name,
                     self._pool_avail_map[pool_id].uptime,
                     self._pool_avail_map[pool_id].downtime,
                     self._pool_avail_map[pool_id].is_avail
                ))
                self.update_availability(pool_id,
                                         self._pool_avail_map[pool_id].pool_name,
                                         self._pool_avail_map[pool_id].started_at,
                                         self._pool_avail_map[pool_id].uptime,
                                         self._pool_avail_map[pool_id].last_uptime,
                                         self._pool_avail_map[pool_id].downtime,
                                         self._pool_avail_map[pool_id].last_downtime,
                                         self._pool_avail_map[pool_id].num_failures,
                                         self._pool_avail_map[pool_id].is_avail
                                        )

            self._shutdown.wait(timeout=self.sleep_interval)

    def shutdown(self) -> None:
        self.log.info('Stopping availability')
        self._shutdown.set()