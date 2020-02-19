"""
System  tests
"""
import logging
from time import sleep
from constants import HEALTH

from teuthology.orchestra import run

log = logging.getLogger(__name__)


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
    try:
        # Check daemon restart/start status
        timeout = 120
        interval = 5

        if args.get('timeout'):
            timeout = args['timeout']

        iterations = timeout / interval
        exit_status = args.get('exit_status')

        while iterations:
            log.info("Check {} {} daemon status".format(dstate.role,
                                                        dstate.id_))
            if dstate.check_status() is not exit_status:
                log.warn("{} {} is still not {}".format(dstate.role,
                                                        dstate.id_, exit_status))
                sleep(interval)
                iterations -= 1
                continue
            break
        else:
            assert False

        # check cluster health
        cluster = ctx.managers.keys()[0]
        check_status = args.get('check_status', False)
        check_key = args.get('check_keys')
        health_state = args.get('state')

        while timeout:
            sleep(interval)
            timeout -= interval
            cluster_status = ctx.managers[cluster].raw_cluster_status()
            health = cluster_status.get('health')
            status = health['status']
            checks = health['checks']

            try:
                if check_status:
                    assert status in health_state, \
                        "[ {} ] not found in health status {}".format(health_state, status)
                    log.info(" Cluster health status : {} as expected".format(status))
                    if check_key:
                        check_key = [check_key] if not isinstance(check_key, list) else check_key

                        for chk in check_key:
                            assert chk.upper() in checks, \
                                "[ {} ] not found in health checks {}".format(chk, checks)
                            log.info("[ {} ] found in cluster health checks as expected".format(chk))
                        log.info(" Cluster health status : {}".format(checks))
                return health
            except AssertionError as err:
                log.warn(err)
                log.warn("Retrying with {} seconds left".format(timeout))
                continue
        else:
            assert False, "[ {} ] not found in health checks".format(health_state)
    except AssertionError:
        assert False


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
        # reboot node
        dstate.remote.run(args=["sudo", "shutdown", "-r", "now", run.Raw("&")])

        # wait for ssh reconnection
        assert dstate.remote.reconnect(timeout=timeout, sleep_time=interval),\
            " [ {} ] Reboot failed".format(dstate.id_)
        log.info(" [ {} ] Reboot successful".format(dstate.id_))
        return True
    except AssertionError as err:
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
        "check_keys": None
    }

    try:
        # Get daemon nodes with SystemDState obj from ctx
        daemons = ctx.daemons.daemons.get(daemon)
        for name, dstate in daemons.items():
            # stop and verify the cluster status
            dstate.stop()
            kwargs['exit_status'] = 0
            kwargs['state'] = [HEALTH['warn']]
            kwargs['check_keys'] = "{}_down".format(dstate.daemon_type)

            check_service_status(ctx, dstate, **kwargs)

            # start and verify the cluster status
            dstate.restart()
            kwargs['exit_status'] = None
            kwargs['state'] = [HEALTH['warn'], HEALTH['good']]
            kwargs['check_keys'] = None
            check_service_status(ctx, dstate, **kwargs)

            # restart daemon and verify cluster status
            dstate.restart()
            check_service_status(ctx, dstate, **kwargs)

            # reboot daemon node and verify cluster status
            reboot_node(dstate, timeout=600, interval=30)
            log.info("[ ({}, {}) ] daemon system tests Completed".format(daemon, dstate.id_))
            return True
    except KeyError as err:
        log.error("No {}(s) found".format(daemon))
        assert False, err
    finally:
        log.info("Daemon service system tests Completed")
