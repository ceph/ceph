"""
System  Tests
"""
import logging
from cStringIO import StringIO
from time import sleep

from teuthology.orchestra import run
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)


_MGR = "mgr"
_OSD = "osd"
_MON = "mon"
_MDS = "mds"

_CEPH_HEALTH = {
    "error": "HEALTH_ERR",
    "warn": "HEALTH_WARN",
    "good": "HEALTH_OK"
}

_systemd_cmd = 'sudo systemctl {action} {daemon}@{id_}'


def __mark(dstate):
    """
    Logging marker
    Args:
        dstate: daemon state
    """
    return " [ {}:{} ]".format(dstate.type_.upper(), dstate.id_.upper())


def __wait(seconds=60, msg=None):
    """
    Method to initiate sleep with reason
    """
    log.info("Wait for {} seconds......".format(seconds))
    log.info("Reason to wait : {}".format(msg))
    sleep(seconds)
    log.info("wait completed......")


def daemon_service(dstate, action, retries=10):
    """
    perform systemctl command with action provided
    Args:
        dstate: Daemon state
        action: action to be performed
        retries: number of retries
    """
    mark = __mark(dstate)
    daemon = "{cluster}-{role}".format(cluster=dstate.cluster, role=dstate.type_)
    daemon_id = "{id}".format(id=dstate.id_)

    log.info("{} {} daemon".format(mark, action.upper()))
    while retries:
        retries -= 1
        try:
            getattr(dstate, action)()
            __wait(60, msg="systemctl command executed")
            res = dstate.remote.run(args=[run.Raw(dstate.show_cmd)], stdout=StringIO())
            res = res.stdout.read().lower()
            if "ActiveState=failed".lower() in res:
                assert False, res
            log.info("{} {} daemon - Successful ".format(mark, action))
            return
        except (AssertionError, CommandFailedError) as err:
            log.error("{} Command execution failed - {}".format(mark, err))
            log.warn("{} Trying to {}, Retries left: {}".format(mark, action,
                                                                retries))
            cmd = "sudo systemctl reset-failed"
            log.warn("{} Running '{}'".format(mark, cmd))
            dstate.remote.run(args=[run.Raw(cmd)])
            __wait(10, msg="Resetted failed daemons")
            cmd = "sudo systemctl daemon-reload"
            log.warn("{} Running '{}'".format(mark, cmd))
            dstate.remote.run(args=[run.Raw(cmd)])
            __wait(10, msg="Daemon reloaded")
            log.warn("{} Restarting daemon".format(mark))
            dstate.restart()
            __wait(30, msg="Daemon Restarted")
    else:
        assert False, "{} Unable to complete {} action".format(mark, action)


def get_daemon_info(daemon, ctx):
    """
    Get number daemons and objects
    Args:
        daemon: daemon name
        ctx: ceph context object
    Return:
        daemon_stat: daemon details
    """
    daemon = "ceph.%s" % daemon.lower() \
        if not daemon.lower().startswith("ceph.") else daemon

    log.info(" [ {} ] Get daemon information".format(daemon.upper()))

    daemon_stat = {
        "count": None,
        "daemons": None,
        "active": dict(),
        "active_count": None,
        "inactive": dict(),
        "inactive_count": None
    }

    try:
        assert daemon in ctx.daemons.daemons,\
            " {} Daemons Not Found".format(daemon)

        daemons = ctx.daemons.daemons[daemon]
        daemon_stat['daemons'] = daemons
        daemon_stat['count'] = len(daemons)
        for name, dstate in daemons.items():
            if dstate.running():
                daemon_stat["active"].update({name: dstate})
            else:
                daemon_stat["inactive"].update({name: dstate})
        daemon_stat['active_count'] = len(daemon_stat["active"])
        daemon_stat['inactive_count'] = len(daemon_stat["inactive"])
        log.info(" [ {} ] Ceph daemon Stats : {}".format(daemon.upper(), daemon_stat))
        return daemon_stat
    except AssertionError as err:
        log.warn(" {}".format(err))
        log.warn(" [ {} ] Daemons not available in cluster".format(daemon.upper()))
    return False


def check_ceph_cli_availability(ctx):
    """
    Function to check ceph cli availability,
    In case, ceph command line fails when
        1) less number of mons available
    Args:
        ctx: ceph context object
    """
    __warn_status = "Make sure required number(2) of MON(S) are running"
    mons = ctx.daemons.daemons['ceph.mon']
    res = [daemon.running() for node, daemon in mons.items() if daemon.running()]
    if len(res) >= 2:
        log.info(" CEPH CLI is available")
        return True
    log.warn(" {} CEPH CLI not available {}".format("*"*10, "*"*10))
    log.warn(" Cannot run CEPH CLI commands: {}".format(__warn_status))
    return False


def check_service_status(ctx, dstate, **args):
    """
    check service status and cluster health_status
    Args:
        ctx: ceph context obj
        dstate: daemon state obj
        args: arguments
                (ex., timeout: 120(default)
                 state: Health states list (ex., [HEALTH_ERR, HEALTH_WARN])
                 exit_status: exit status
                 check: true)
    """
    timeout = 120
    interval = 5
    mark = __mark(dstate)

    try:
        # Check daemon restart/start status
        if args.get('timeout'):
            timeout = args['timeout']

        iterations = timeout / interval
        exit_status = args.get('exit_status')
        action = args.get('action', "start")

        while iterations:
            log.info("{} Check daemon status".format(mark))
            sleep(interval)
            iterations -= 1
            try:
                if dstate.check_status() is not exit_status:
                    log.warn("{} is still not {}".format(mark, exit_status))
                    continue
            except CommandFailedError:
                daemon_service(dstate, action)
                continue
            break
        else:
            assert False

        # Check cluster health
        # Check ceph cli availability, skip if ceph cli not available
        cluster = ctx.managers.keys()[0]
        check_status = args.get('check_status', False)
        check_key = args.get('check_keys')
        health_state = args.get('state')

        try:
            if dstate.type_.lower() in [_MON]:
                assert check_ceph_cli_availability(ctx)
            elif dstate.type_.lower() in [_MGR, _MDS]:
                res = get_daemon_info(dstate.type_, ctx)
                if res['active_count'] > 0:
                    health_state.append(_CEPH_HEALTH['good'])
        except AssertionError:
            return True

        while timeout:
            sleep(interval)
            timeout -= interval

            try:
                cluster_status = ctx.managers[cluster].raw_cluster_status()
                health = cluster_status.get('health')
                status = health['status']
                checks = health['checks']

                if check_status:
                    log.info("{} Validate CEPH Health Status".format(mark))
                    assert status in health_state, \
                        " [ {} ] not found in health status as expected," \
                        " current status : {}".format(health_state, status)
                    log.info(" Cluster health status : {} as expected".format(status))

                    if check_key:
                        log.info("{} Validate CEPH Health Checks".format(mark))
                        check_key = [check_key] if not isinstance(check_key, list) else check_key
                        for chk in check_key:
                            assert chk.upper() in checks, \
                                " [ {} ] not found in health checks {}".format(chk, checks)
                            log.info(" [ {} ] found in cluster health checks as expected".format(chk))
                        log.info(" Cluster health checks as expected : {}".format(checks))
                return health
            except AssertionError as err:
                log.warn(err)
                log.warn("{} Retrying with {} seconds left".format(mark, timeout))
                continue
        else:
            assert False, " [ {} ] not found in health checks".format(health_state)
    except AssertionError:
        assert False


def wait_for_daemon_healthy(dstate, ctx, timeout=1200):
    """
    Wait for daemon health
        1. check daemon available with healthy status
        2. wait for PG's clean state
    Args:
        dstate: Daemon state
        ctx: ceph context object
        timeout: timeout in seconds
    """
    _type = dstate.type_
    daemon_count = len(ctx.daemons.iter_daemons_of_role(_MON))
    mark = __mark(dstate)
    cluster = ctx.managers.keys()[0]

    try:
        ctx.managers[cluster].wait_for_mgr_available(timeout=timeout)
        log.info("{} MGRs are available now".format(mark))
        ctx.managers[cluster].wait_for_mon_quorum_size(daemon_count,
                                                       timeout=timeout)
        log.info("{} MONs has correct Quorum size : {}".format(mark, daemon_count))
        ctx.managers[cluster].wait_for_all_osds_up(timeout=timeout)
        log.info("{} OSDs are all up".format(mark))
        ctx.managers[cluster].wait_for_clean(timeout=timeout)
        log.info("{} CEPH cluster has all Active+Clean PGs".format(mark))
        ctx.managers[cluster].wait_till_active(timeout=timeout)
        log.info("{} CEPH cluster is Active".format(mark))
    except Exception as err:
        log.error(err)
        assert False, err


def reboot_node(dstate, **args):
    """
    Reboot daemon node
    Args:
        dstate: daemon dstate
        args: reboot arguments(ex., timeout=300, interval=30)
    """
    timeout = 600
    interval = 30

    if args.get('timeout'):
        timeout = args['timeout']
    if args.get('interval'):
        interval = args['interval']

    try:
        # Reboot node
        log.info(" [ {} ] Reboot {} daemon node".format(dstate.id_.upper(),
                                                        dstate.type_.upper()))
        dstate.remote.run(args=["sudo", "shutdown", "-r", "now", run.Raw("&")])

        # Wait for ssh reconnection
        assert dstate.remote.reconnect(timeout=timeout, sleep_time=interval),\
            " [ {} ] Reboot failed".format(dstate.id_)
        log.info(" [ {} ] Reboot successful".format(dstate.id_))
        return True
    except (AssertionError, CommandFailedError) as err:
        assert False, err


def ceph_daemon_system_test(ctx, daemon):
    """
    Perform sequential actions on daemon.
        1) stop daemon, check IO and cluster status
        2) re/start daemon, check IO and cluster status
        3) reboot node, check IO and cluster
    Args:
        ctx: ceph context obj
        daemon: ceph daemon
    """
    daemon = "ceph.%s" % daemon.lower() \
        if not daemon.lower().startswith("ceph.") else daemon

    kwargs = {
        "timeout": 120,
        "exit_status": None,
        "state": None,
        "check_status": True,
        "verify_status": None,
        "check_keys": None,
        "action": None
    }

    try:
        # Get daemon nodes with SystemDState obj from ctx
        daemons = ctx.daemons.daemons.get(daemon)
        for name, dstate in daemons.items():
            mark = __mark(dstate)

            # Stop and verify the cluster status
            # Wait for 60 secs for clear status
            log.info("{}  System tests - STARTED ".format(mark))
            kwargs['exit_status'] = 0
            kwargs['state'] = [_CEPH_HEALTH['warn']]
            kwargs['action'] = "stop"
            daemon_service(dstate, kwargs['action'])
            check_service_status(ctx, dstate, **kwargs)
            __wait(60, msg="wait for daemon to be in expected state")
            log.info("{} STOP daemon system test - Done".format(mark))

            # Start and verify the cluster status
            # Wait for 60 secs for clear status
            kwargs['exit_status'] = None
            kwargs['state'] = [_CEPH_HEALTH['warn'], _CEPH_HEALTH['good']]
            kwargs['check_keys'] = None
            kwargs['action'] = "start"
            daemon_service(dstate, kwargs['action'])
            check_service_status(ctx, dstate, **kwargs)
            __wait(60, msg="wait for daemon to be in expected state")
            wait_for_daemon_healthy(dstate, ctx, timeout=3600)
            log.info("{} START daemon system test - Done".format(mark))

            # Restart daemon and verify cluster status
            # Wait for 60 secs for clear status
            kwargs['action'] = "restart"
            daemon_service(dstate, kwargs['action'])
            check_service_status(ctx, dstate, **kwargs)
            __wait(60, msg="wait for daemon to be in expected state")
            wait_for_daemon_healthy(dstate, ctx, timeout=3600)
            log.info("{} RESTART daemon system test - Done".format(mark))

            # Reboot daemon node and verify cluster status
            assert reboot_node(dstate, timeout=1200, interval=30)
            __wait(60, msg="wait for node to be in expected state")
            wait_for_daemon_healthy(dstate, ctx, timeout=3600)
            log.info("{} REBOOT daemon system test - Done".format(mark))
        log.info("[ {} ] Daemon system tests - PASSED".format(daemon.upper()))
        return True
    except KeyError as err:
        log.error("No {}(s) found".format(daemon))
        assert False, err
    except Exception as err:
        log.error("[ {} ] : System tests - FAILED ".format(daemon.upper()))
        assert False, err
    finally:
        log.info("[ {} ] : System tests - COMPLETED ".format(daemon.upper()))
