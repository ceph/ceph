
from mgr_module import MgrModule, CommandResult
import threading
import random
import json
import errno


class Module(MgrModule):
    """
    This module is for testing the ceph-mgr python interface from within
    a running ceph-mgr daemon.

    It implements a sychronous self-test command for calling the functions
    in the MgrModule interface one by one, and a background "workload"
    command for causing the module to perform some thrashing-type
    activities in its serve() thread.
    """

    WORKLOAD_COMMAND_SPAM = "command_spam"
    SHUTDOWN = "shutdown"

    WORKLOADS = (WORKLOAD_COMMAND_SPAM, )

    COMMANDS = [
            {
                "cmd": "mgr self-test run",
                "desc": "Run mgr python interface tests",
                "perm": "r"
            },
            {
                "cmd": "mgr self-test background start name=workload,type=CephString",
                "desc": "Activate a background workload (one of {0})".format(
                    ", ".join(WORKLOADS)),
                "perm": "r"
            },
            {
                "cmd": "mgr self-test background stop",
                "desc": "Stop background workload if any is running",
                "perm": "r"
            },
            ]



    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self._event = threading.Event()
        self._workload = None

    def handle_command(self, command):
        if command['prefix'] == 'mgr self-test run':
            self._self_test()
            return 0, '', 'Self-test succeeded'

        elif command['prefix'] == 'mgr self-test background start':
            if command['workload'] not in self.WORKLOADS:
                return (-errno.EINVAL, '',
                        "Workload not found '{0}'".format(command['workload']))
            self._workload = command['workload']
            self._event.set()
            return 0, '', 'Running `{0}` in background'.format(self._workload)

        elif command['prefix'] == 'mgr self-test background stop':
            if self._workload:
                was_running = self._workload
                self._workload = None
                self._event.set()
                return 0, '', 'Stopping background workload `{0}`'.format(
                        was_running)
            else:
                return 0, '', 'No background workload was running'

        else:
            return (-errno.EINVAL, '',
                    "Command not found '{0}'".format(command['prefix']))

    def _self_test(self):
        self.log.info("Running self-test procedure...")

        self._self_test_osdmap()
        self._self_test_getters()
        self._self_test_config()
        self._self_test_misc()
        self._self_test_perf_counters()

    def _self_test_getters(self):
        self.version
        self.get_context()
        self.get_mgr_id()

        # In this function, we will assume that the system is in a steady
        # state, i.e. if a server/service appears in one call, it will
        # not have gone by the time we call another function referring to it

        objects = [
                "fs_map",
                "osdmap_crush_map_text",
                "osd_map",
                "config",
                "mon_map",
                "service_map",
                "osd_metadata",
                "pg_summary",
                "pg_status",
                "pg_dump",
                "df",
                "osd_stats",
                "health",
                "mon_status",
                "mgr_map"
                ]
        for obj in objects:
            self.get(obj)

        servers = self.list_servers()
        for server in servers:
            self.get_server(server['hostname'])

        osdmap = self.get('osd_map')
        for o in osdmap['osds']:
            osd_id = o['osd']
            self.get_metadata("osd", str(osd_id))

        self.get_daemon_status("osd", "0")
        #send_command

    def _self_test_config(self):
        # This is not a strong test (can't tell if values really
        # persisted), it's just for the python interface bit.

        self.set_config("testkey", "testvalue")
        assert self.get_config("testkey") == "testvalue"

        self.set_localized_config("testkey", "testvalue")
        assert self.get_localized_config("testkey") == "testvalue"

        self.set_config_json("testjsonkey", {"testblob": 2})
        assert self.get_config_json("testjsonkey") == {"testblob": 2}

        assert sorted(self.get_config_prefix("test").keys()) == sorted(
                ["testkey", "testjsonkey"])

    def _self_test_perf_counters(self):
        self.get_perf_schema("osd", "0")
        self.get_counter("osd", "0", "osd.op")
        #get_counter
        #get_all_perf_coutners

    def _self_test_misc(self):
        self.set_uri("http://this.is.a.test.com")
        self.set_health_checks({})

    def _self_test_osdmap(self):
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

        self.log.info("Finished self-test procedure.")

    def shutdown(self):
        self._workload = self.SHUTDOWN
        self._event.set()

    def _command_spam(self):
        self.log.info("Starting command_spam workload...")
        while not self._event.is_set():
            osdmap = self.get_osdmap()
            dump = osdmap.dump()
            count = len(dump['osds'])
            i = int(random.random() * count)
            w = random.random()

            result = CommandResult('')
            self.send_command(result, 'mon', '', json.dumps({
                'prefix': 'osd reweight',
                'id': i,
                'weight': w
                }), '')

            crush = osdmap.get_crush().dump()
            r, outb, outs = result.wait()

        self._event.clear()
        self.log.info("Ended command_spam workload...")

    def serve(self):
        while True:
            if self._workload == self.WORKLOAD_COMMAND_SPAM:
                self._command_spam()
            elif self._workload == self.SHUTDOWN:
                self.log.info("Shutting down...")
                break
            else:
                self.log.info("Waiting for workload request...")
                self._event.wait()
                self._event.clear()
