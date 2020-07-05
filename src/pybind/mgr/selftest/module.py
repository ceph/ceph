
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

    # These workloads are things that can be requested to run inside the
    # serve() function
    WORKLOAD_COMMAND_SPAM = "command_spam"
    WORKLOAD_THROW_EXCEPTION = "throw_exception"
    SHUTDOWN = "shutdown"

    WORKLOADS = (WORKLOAD_COMMAND_SPAM, WORKLOAD_THROW_EXCEPTION)

    # The test code in qa/ relies on these options existing -- they
    # are of course not really used for anything in the module
    MODULE_OPTIONS = [
        {'name': 'testkey'},
        {'name': 'testlkey'},
        {'name': 'testnewline'},
        {'name': 'roption1'},
        {'name': 'roption2', 'type': 'str', 'default': 'xyz'},
        {'name': 'rwoption1'},
        {'name': 'rwoption2', 'type': 'int'},
        {'name': 'rwoption3', 'type': 'float'},
        {'name': 'rwoption4', 'type': 'str'},
        {'name': 'rwoption5', 'type': 'bool'},
        {'name': 'rwoption6', 'type': 'bool', 'default': True}
    ]

    COMMANDS = [
            {
                "cmd": "mgr self-test run",
                "desc": "Run mgr python interface tests",
                "perm": "rw"
            },
            {
                "cmd": "mgr self-test background start name=workload,type=CephString",
                "desc": "Activate a background workload (one of {0})".format(
                    ", ".join(WORKLOADS)),
                "perm": "rw"
            },
            {
                "cmd": "mgr self-test background stop",
                "desc": "Stop background workload if any is running",
                "perm": "rw"
            },
            {
                "cmd": "mgr self-test config get name=key,type=CephString",
                "desc": "Peek at a configuration value",
                "perm": "rw"
            },
            {
                "cmd": "mgr self-test config get_localized name=key,type=CephString",
                "desc": "Peek at a configuration value (localized variant)",
                "perm": "rw"
            },
            {
                "cmd": "mgr self-test remote",
                "desc": "Test inter-module calls",
                "perm": "rw"
            },
            {
                "cmd": "mgr self-test module name=module,type=CephString",
                "desc": "Run another module's self_test() method",
                "perm": "rw"
            },
            {
                "cmd": "mgr self-test health set name=checks,type=CephString",
                "desc": "Set a health check from a JSON-formatted description.",
                "perm": "rw"
            },
            {
                "cmd": "mgr self-test health clear name=checks,type=CephString,n=N,req=False",
                "desc": "Clear health checks by name. If no names provided, clear all.",
                "perm": "rw"
            },
            {
                "cmd": "mgr self-test insights_set_now_offset name=hours,type=CephString",
                "desc": "Set the now time for the insights module.",
                "perm": "rw"
            },
            {
                "cmd": "mgr self-test cluster-log name=channel,type=CephString "
                       "name=priority,type=CephString "
                       "name=message,type=CephString",
                "desc": "Create an audit log record.",
                "perm": "rw"
            },
            ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self._event = threading.Event()
        self._workload = None
        self._health = {}

    def handle_command(self, inbuf, command):
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
        elif command['prefix'] == 'mgr self-test config get':
            return 0, str(self.get_module_option(command['key'])), ''
        elif command['prefix'] == 'mgr self-test config get_localized':
            return 0, str(self.get_localized_module_option(command['key'])), ''
        elif command['prefix'] == 'mgr self-test remote':
            self._test_remote_calls()
            return 0, '', 'Successfully called'
        elif command['prefix'] == 'mgr self-test module':
            try:
                r = self.remote(command['module'], "self_test")
            except RuntimeError as e:
                return -1, '', "Test failed: {0}".format(e)
            else:
                return 0, str(r), "Self-test OK"
        elif command['prefix'] == 'mgr self-test health set':
            return self._health_set(inbuf, command)
        elif command['prefix'] == 'mgr self-test health clear':
            return self._health_clear(inbuf, command)
        elif command['prefix'] == 'mgr self-test insights_set_now_offset':
            return self._insights_set_now_offset(inbuf, command)
        elif command['prefix'] == 'mgr self-test cluster-log':
            priority_map = {
                'info': self.CLUSTER_LOG_PRIO_INFO,
                'security': self.CLUSTER_LOG_PRIO_SEC,
                'warning': self.CLUSTER_LOG_PRIO_WARN,
                'error': self.CLUSTER_LOG_PRIO_ERROR
            }
            self.cluster_log(command['channel'],
                             priority_map[command['priority']],
                             command['message'])
            return 0, '', 'Successfully called'
        else:
            return (-errno.EINVAL, '',
                    "Command not found '{0}'".format(command['prefix']))

    def _health_set(self, inbuf, command):
        try:
            checks = json.loads(command["checks"])
        except Exception as e:
            return -1, "", "Failed to decode JSON input: {}".format(e)

        try:
            for check, info in checks.items():
                self._health[check] = {
                    "severity": str(info["severity"]),
                    "summary": str(info["summary"]),
                    "count": 123,
                    "detail": [str(m) for m in info["detail"]]
                }
        except Exception as e:
            return -1, "", "Invalid health check format: {}".format(e)

        self.set_health_checks(self._health)
        return 0, "", ""

    def _health_clear(self, inbuf, command):
        if "checks" in command:
            for check in command["checks"]:
                if check in self._health:
                    del self._health[check]
        else:
            self._health = dict()

        self.set_health_checks(self._health)
        return 0, "", ""

    def _insights_set_now_offset(self, inbuf, command):
        try:
            hours = int(command["hours"])
        except Exception as e:
            return -1, "", "Timestamp must be numeric: {}".format(e)

        self.remote("insights", "testing_set_now_time_offset", hours)
        return 0, "", ""

    def _self_test(self):
        self.log.info("Running self-test procedure...")

        self._self_test_osdmap()
        self._self_test_getters()
        self._self_test_config()
        self._self_test_store()
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
                "pg_ready",
                "df",
                "pg_stats",
                "pool_stats",
                "osd_stats",
                "osd_ping_times",
                "health",
                "mon_status",
                "mgr_map"
                ]
        for obj in objects:
            assert self.get(obj) is not None

        assert self.get("__OBJ_DNE__") is None

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

        self.set_module_option("testkey", "testvalue")
        assert self.get_module_option("testkey") == "testvalue"

        self.set_localized_module_option("testkey", "foo")
        assert self.get_localized_module_option("testkey") == "foo"

        # Must return the default value defined in MODULE_OPTIONS.
        value = self.get_localized_module_option("rwoption6")
        assert isinstance(value, bool)
        assert value is True

        # Use default value.
        assert self.get_module_option("roption1") is None
        assert self.get_module_option("roption1", "foobar") == "foobar"
        assert self.get_module_option("roption2") == "xyz"
        assert self.get_module_option("roption2", "foobar") == "xyz"

        # Option type is not defined => return as string.
        self.set_module_option("rwoption1", 8080)
        value = self.get_module_option("rwoption1")
        assert isinstance(value, str)
        assert value == "8080"

        # Option type is defined => return as integer.
        self.set_module_option("rwoption2", 10)
        value = self.get_module_option("rwoption2")
        assert isinstance(value, int)
        assert value == 10

        # Option type is defined => return as float.
        self.set_module_option("rwoption3", 1.5)
        value = self.get_module_option("rwoption3")
        assert isinstance(value, float)
        assert value == 1.5

        # Option type is defined => return as string.
        self.set_module_option("rwoption4", "foo")
        value = self.get_module_option("rwoption4")
        assert isinstance(value, str)
        assert value == "foo"

        # Option type is defined => return as bool.
        self.set_module_option("rwoption5", False)
        value = self.get_module_option("rwoption5")
        assert isinstance(value, bool)
        assert value is False

        # Specified module does not exist => return None.
        assert self.get_module_option_ex("foo", "bar") is None

        # Specified key does not exist => return None.
        assert self.get_module_option_ex("dashboard", "bar") is None

        self.set_module_option_ex("telemetry", "contact", "test@test.com")
        assert self.get_module_option_ex("telemetry", "contact") == "test@test.com"

        # No option default value, so use the specified one.
        assert self.get_module_option_ex("dashboard", "password") is None
        assert self.get_module_option_ex("dashboard", "password", "foobar") == "foobar"

        # Option type is not defined => return as string.
        self.set_module_option_ex("selftest", "rwoption1", 1234)
        value = self.get_module_option_ex("selftest", "rwoption1")
        assert isinstance(value, str)
        assert value == "1234"

        # Option type is defined => return as integer.
        self.set_module_option_ex("telemetry", "interval", 60)
        value = self.get_module_option_ex("telemetry", "interval")
        assert isinstance(value, int)
        assert value == 60

        # Option type is defined => return as bool.
        self.set_module_option_ex("telemetry", "leaderboard", True)
        value = self.get_module_option_ex("telemetry", "leaderboard")
        assert isinstance(value, bool)
        assert value is True

    def _self_test_store(self):
        existing_keys = set(self.get_store_prefix("test").keys())
        self.set_store("testkey", "testvalue")
        assert self.get_store("testkey") == "testvalue"

        assert sorted(self.get_store_prefix("test").keys()) == sorted(
                list({"testkey"} | existing_keys))


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

    def _test_remote_calls(self):
        # Test making valid call
        self.remote("influx", "handle_command", "", {"prefix": "influx self-test"})

        # Test calling module that exists but isn't enabled
        # (arbitrarily pick a non-always-on module to use)
        disabled_module = "telegraf"
        mgr_map = self.get("mgr_map")
        assert disabled_module not in mgr_map['modules']

        # (This works until the Z release in about 2027)
        latest_release = sorted(mgr_map['always_on_modules'].keys())[-1]
        assert disabled_module not in mgr_map['always_on_modules'][latest_release]

        try:
            self.remote(disabled_module, "handle_command", {"prefix": "influx self-test"})
        except ImportError:
            pass
        else:
            raise RuntimeError("ImportError not raised for disabled module")

        # Test calling module that doesn't exist
        try:
            self.remote("idontexist", "handle_command", {"prefix": "influx self-test"})
        except ImportError:
            pass
        else:
            raise RuntimeError("ImportError not raised for nonexistent module")

        # Test calling method that doesn't exist
        try:
            self.remote("influx", "idontexist", {"prefix": "influx self-test"})
        except NameError:
            pass
        else:
            raise RuntimeError("KeyError not raised")

    def remote_from_orchestrator_cli_self_test(self, what):
        import orchestrator
        if what == 'OrchestratorError':
            c = orchestrator.TrivialReadCompletion(result=None)
            c.fail(orchestrator.OrchestratorError('hello', 'world'))
            return c
        elif what == "ZeroDivisionError":
            c = orchestrator.TrivialReadCompletion(result=None)
            c.fail(ZeroDivisionError('hello', 'world'))
            return c
        assert False, repr(what)

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
            elif self._workload == self.WORKLOAD_THROW_EXCEPTION:
                raise RuntimeError("Synthetic exception in serve")
            else:
                self.log.info("Waiting for workload request...")
                self._event.wait()
                self._event.clear()
