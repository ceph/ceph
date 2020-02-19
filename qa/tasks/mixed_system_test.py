import contextlib
import logging

from tasks.mixed_system_tests import system
from tasks.mixed_system_tests import ios

log = logging.getLogger(__name__)


@contextlib.contextmanager
def rgw_ios(ctx, config):
    """
    Task to run RGW IO's using ceph-QE-scripts repo.
    Args:
        ctx: cluster obj
        config: test data
    example:
    tasks:
    - rgw-system-test:
        test: <test-name>
        script: <script-name>        | default value is <test-name>.py
        test_version: <test-version> | ex: v1 or v2, default value is v2
        clients: <clients list>      | ex: [client.0, client.1]
                                        default value is ['client.0]
        config: <configuration of the test-name> |
            default values is the yaml file config from ceph-qe-scripts
    """
    rgw_ios_internal = ios.rgw_ios(ctx, config)
    try:
        rgw_ios_internal.__enter__()
        yield
    except Exception as err:
        log.info(err)
        assert False, err
    finally:
        rgw_ios_internal.__exit__()


@contextlib.contextmanager
def restart_tests(ctx, config):
    """
    Perform restart test scenarios based on the daemon sequentially
        1) stop daemon.
            a. verify IO & cluster health.
            b. start daemon and wait for cluster status to be healthy.
        2) restart daemon.
            a. verify IO & cluster health.
            b. wait for cluster status to be healthy.
        3) reboot daemon node.
            a. verify IO & cluster health.
            b. wait for node up & running, and cluster status to be healthy.
    Args:
        ctx: context obj
        config: test configuration
    example:
        mixed_system_test.restart_tests:
            config:
                daemon: ["mon", "mgr", "mds"]
    """
    daemons = config.get('daemons')
    try:
        for daemon in daemons:
            assert system.ceph_daemon_system_test(ctx, daemon)
            log.info("{} completed".format(daemon))
        yield
    except Exception as err:
        assert False, err
    finally:
        log.info("Daemon(s) Service system tests completed")
