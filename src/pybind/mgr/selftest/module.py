
from mgr_module import MgrModule


class Module(MgrModule):
    COMMANDS = [{
        "cmd": "mgr self-test",
        "desc": "Run mgr python interface tests",
        "perm": "r"
    }]
    
    def handle_command(self, command):
        if command['prefix'] == 'mgr self-test':
            self._self_test()

            return 0, '', 'Self-test succeeded'
        else:
            return (-errno.EINVAL, '',
                    "Command not found '{0}'".format(command['prefix']))

    def _self_test(self):
        osdmap = self.get_osdmap()
        osdmap.get_epoch()
        osdmap.get_crush_version()
        osdmap.dump()


        inc = osdmap.new_incremental()
        osdmap.apply_incremental(inc)
        inc.get_epoch()
        inc.dump()

        crush = osdmap.get_crush()
        crush.dump()
        crush.get_item_name(-1)
        crush.get_item_weight(-1)
        crush.find_takes()
        crush.get_take_weight_osd_map(-1)

        #osdmap.get_pools_by_take()
        #osdmap.calc_pg_upmaps()
        #osdmap.map_pools_pgs_up()

        #inc.set_osd_reweights
        #inc.set_crush_compat_weight_set_weights

