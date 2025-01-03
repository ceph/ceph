
from mgr_module import MgrModule, CommandResult, HandleCommandResult, CLICommand, Option
import enum
import json
import random
import sys
import threading
from code import InteractiveInterpreter
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple


# These workloads are things that can be requested to run inside the
# serve() function
class Workload(enum.Enum):
    COMMAND_SPAM = 'command_spam'
    THROW_EXCEPTION = 'throw_exception'
    SHUTDOWN = 'shutdown'


class Module(MgrModule):
    """
    This module is for testing the ceph-mgr python interface from within
    a running ceph-mgr daemon.

    It implements a sychronous self-test command for calling the functions
    in the MgrModule interface one by one, and a background "workload"
    command for causing the module to perform some thrashing-type
    activities in its serve() thread.
    """

    # The test code in qa/ relies on these options existing -- they
    # are of course not really used for anything in the module
    MODULE_OPTIONS = [
        Option(name='testkey'),
        Option(name='testlkey'),
        Option(name='testnewline'),
        Option(name='roption1'),
        Option(name='roption2',
               type='str',
               default='xyz'),
        Option(name='rwoption1'),
        Option(name='rwoption2',
               type='int'),
        Option(name='rwoption3',
               type='float'),
        Option(name='rwoption4',
               type='str'),
        Option(name='rwoption5',
               type='bool'),
        Option(name='rwoption6',
               type='bool',
               default=True),
        Option(name='rwoption7',
               type='int',
               min=1,
               max=42),
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(Module, self).__init__(*args, **kwargs)
        self._event = threading.Event()
        self._workload: Optional[Workload] = None
        self._health: Dict[str, Dict[str, Any]] = {}
        self._repl = InteractiveInterpreter(dict(mgr=self))

    @CLICommand('mgr self-test python-version', perm='r')
    def python_version(self) -> Tuple[int, str, str]:
        '''
        Query the version of the embedded Python runtime
        '''
        major = sys.version_info.major
        minor = sys.version_info.minor
        micro = sys.version_info.micro
        return 0, f'{major}.{minor}.{micro}', ''

    @CLICommand('mgr self-test run')
    def run(self) -> Tuple[int, str, str]:
        '''
        Run mgr python interface tests
        '''
        self._self_test()
        return 0, '', 'Self-test succeeded'

    @CLICommand('mgr self-test background start')
    def backgroun_start(self, workload: Workload) -> Tuple[int, str, str]:
        '''
        Activate a background workload (one of command_spam, throw_exception)
        '''
        self._workload = workload
        self._event.set()
        return 0, '', 'Running `{0}` in background'.format(self._workload)

    @CLICommand('mgr self-test background stop')
    def background_stop(self) -> Tuple[int, str, str]:
        '''
        Stop background workload if any is running
        '''
        if self._workload:
            was_running = self._workload
            self._workload = None
            self._event.set()
            return 0, '', 'Stopping background workload `{0}`'.format(
                was_running)
        else:
            return 0, '', 'No background workload was running'

    @CLICommand('mgr self-test config get')
    def config_get(self, key: str) -> Tuple[int, str, str]:
        '''
        Peek at a configuration value
        '''
        return 0, str(self.get_module_option(key)), ''

    @CLICommand('mgr self-test config get_localized')
    def config_get_localized(self, key: str) -> Tuple[int, str, str]:
        '''
        Peek at a configuration value (localized variant)
        '''
        return 0, str(self.get_localized_module_option(key)), ''

    @CLICommand('mgr self-test remote')
    def test_remote(self) -> Tuple[int, str, str]:
        '''
        Test inter-module calls
        '''
        self._test_remote_calls()
        return 0, '', 'Successfully called'

    @CLICommand('mgr self-test module')
    def module(self, module: str) -> Tuple[int, str, str]:
        '''
        Run another module's self_test() method
        '''
        try:
            r = self.remote(module, "self_test")
        except RuntimeError as e:
            return -1, '', "Test failed: {0}".format(e)
        else:
            return 0, str(r), "Self-test OK"

    @CLICommand('mgr self-test cluster-log')
    def do_cluster_log(self,
                       channel: str,
                       priority: str,
                       message: str) -> Tuple[int, str, str]:
        '''
        Create an audit log record.
        '''
        priority_map = {
            'info': self.ClusterLogPrio.INFO,
            'security': self.ClusterLogPrio.SEC,
            'warning': self.ClusterLogPrio.WARN,
            'error': self.ClusterLogPrio.ERROR
        }
        self.cluster_log(channel,
                         priority_map[priority],
                         message)
        return 0, '', 'Successfully called'

    @CLICommand('mgr self-test health set')
    def health_set(self, checks: str) -> Tuple[int, str, str]:
        '''
        Set a health check from a JSON-formatted description.
        '''
        try:
            health_check = json.loads(checks)
        except Exception as e:
            return -1, "", "Failed to decode JSON input: {}".format(e)

        try:
            for check, info in health_check.items():
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

    @CLICommand('mgr self-test health clear')
    def health_clear(self, checks: Optional[List[str]] = None) -> Tuple[int, str, str]:
        '''
        Clear health checks by name. If no names provided, clear all.
        '''
        if checks is not None:
            for check in checks:
                if check in self._health:
                    del self._health[check]
        else:
            self._health = dict()

        self.set_health_checks(self._health)
        return 0, "", ""

    @CLICommand('mgr self-test insights_set_now_offset')
    def insights_set_now_offset(self, hours: int) -> Tuple[int, str, str]:
        '''
        Set the now time for the insights module.
        '''
        self.remote("insights", "testing_set_now_time_offset", hours)
        return 0, "", ""

    def _self_test(self) -> None:
        self.log.info("Running self-test procedure...")

        self._self_test_osdmap()
        self._self_test_getters()
        self._self_test_config()
        self._self_test_store()
        self._self_test_misc()
        self._self_test_perf_counters()

    def _self_test_getters(self) -> None:
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
            self.get_server(server['hostname'])  # type: ignore

        osdmap = self.get('osd_map')
        for o in osdmap['osds']:
            osd_id = o['osd']
            self.get_metadata("osd", str(osd_id))

        self.get_daemon_status("osd", "0")

    def _self_test_config(self) -> None:
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

        # Option value range is specified
        try:
            self.set_module_option("rwoption7", 43)
        except Exception as e:
            assert isinstance(e, ValueError)
        else:
            message = "should raise if value is not in specified range"
            assert False, message

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

    def _self_test_store(self) -> None:
        existing_keys = set(self.get_store_prefix("test").keys())
        self.set_store("testkey", "testvalue")
        assert self.get_store("testkey") == "testvalue"

        assert (set(self.get_store_prefix("test").keys())
                == {"testkey"} | existing_keys)

    def _self_test_perf_counters(self) -> None:
        self.get_perf_schema("osd", "0")
        self.get_counter("osd", "0", "osd.op")
        # get_counter
        # get_all_perf_coutners

    def _self_test_misc(self) -> None:
        self.set_uri("http://this.is.a.test.com")
        self.set_health_checks({})

    def _self_test_osdmap(self) -> None:
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

        # osdmap.get_pools_by_take()
        # osdmap.calc_pg_upmaps()
        # osdmap.map_pools_pgs_up()

        # inc.set_osd_reweights
        # inc.set_crush_compat_weight_set_weights

        self.log.info("Finished self-test procedure.")

    def _test_remote_calls(self) -> None:
        # Test making valid call
        self.remote("influx", "self_test")

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
            self.remote("idontexist", "self_test")
        except ImportError:
            pass
        else:
            raise RuntimeError("ImportError not raised for nonexistent module")

        # Test calling method that doesn't exist
        try:
            self.remote("influx", "idontexist")
        except NameError:
            pass
        else:
            raise RuntimeError("KeyError not raised")

    def remote_from_orchestrator_cli_self_test(self, what: str) -> Any:
        import orchestrator
        if what == 'OrchestratorError':
            return orchestrator.OrchResult(result=None, exception=orchestrator.OrchestratorError('hello, world'))
        elif what == "ZeroDivisionError":
            return orchestrator.OrchResult(result=None, exception=ZeroDivisionError('hello, world'))
        assert False, repr(what)

    def shutdown(self) -> None:
        self._workload = Workload.SHUTDOWN
        self._event.set()

    def _command_spam(self) -> None:
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
                'weight': w}), '')

            _ = osdmap.get_crush().dump()
            r, outb, outs = result.wait()

        self._event.clear()
        self.log.info("Ended command_spam workload...")

    @CLICommand('mgr self-test eval')
    def eval(self,
             s: Optional[str] = None,
             inbuf: Optional[str] = None) -> HandleCommandResult:
        '''
        eval given source
        '''
        source = s or inbuf
        if source is None:
            return HandleCommandResult(-1, '', 'source is not specified')

        err = StringIO()
        out = StringIO()
        with redirect_stderr(err), redirect_stdout(out):
            needs_more = self._repl.runsource(source)
            if needs_more:
                retval = 2
                stdout = ''
                stderr = ''
            else:
                retval = 0
                stdout = out.getvalue()
                stderr = err.getvalue()
            return HandleCommandResult(retval, stdout, stderr)

    def serve(self) -> None:
        while True:
            if self._workload == Workload.COMMAND_SPAM:
                self._command_spam()
            elif self._workload == Workload.SHUTDOWN:
                self.log.info("Shutting down...")
                break
            elif self._workload == Workload.THROW_EXCEPTION:
                raise RuntimeError("Synthetic exception in serve")
            else:
                self.log.info("Waiting for workload request...")
                self._event.wait()
                self._event.clear()
