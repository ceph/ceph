"""
Handle osdfailsafe configuration settings (nearfull ratio and full ratio)
"""
from io import StringIO
import logging
import time

from teuthology.orchestra import run
from tasks.util.rados import rados
from teuthology import misc as teuthology

log = logging.getLogger(__name__)

def task(ctx, config):
    """
    Test handling of osd_failsafe_nearfull_ratio and osd_failsafe_full_ratio
    configuration settings

    In order for test to pass must use log-ignorelist as follows

        tasks:
            - chef:
            - install:
            - ceph:
                log-ignorelist: ['OSD near full', 'OSD full dropping all updates']
            - osd_failsafe_enospc:

    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'osd_failsafe_enospc task only accepts a dict for configuration'

    # Give 2 seconds for injectargs + osd_op_complaint_time (30) + 2 * osd_heartbeat_interval (6) + 6 padding
    sleep_time = 50

    # something that is always there
    dummyfile = '/etc/fstab'
    dummyfile2 = '/etc/resolv.conf'

    manager = ctx.managers['ceph']

    # create 1 pg pool with 1 rep which can only be on osd.0
    osds = manager.get_osd_dump()
    for osd in osds:
        if osd['osd'] != 0:
            manager.mark_out_osd(osd['osd'])

    log.info('creating pool foo')
    manager.create_pool("foo")
    manager.raw_cluster_cmd('osd', 'pool', 'set', 'foo', 'size', '1')

    # State NONE -> NEAR
    log.info('1. Verify warning messages when exceeding nearfull_ratio')

    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.keys()

    proc = mon.run(
             args=[
                 'sudo',
                 'daemon-helper',
                 'kill',
                 'ceph', '-w'
             ],
             stdin=run.PIPE,
             stdout=StringIO(),
             wait=False,
        )

    manager.raw_cluster_cmd('tell', 'osd.0', 'injectargs', '--osd_failsafe_nearfull_ratio .00001')

    time.sleep(sleep_time)
    proc.stdin.close() # causes daemon-helper send SIGKILL to ceph -w
    proc.wait()

    lines = proc.stdout.getvalue().split('\n')

    count = len(filter(lambda line: '[WRN] OSD near full' in line, lines))
    assert count == 2, 'Incorrect number of warning messages expected 2 got %d' % count
    count = len(filter(lambda line: '[ERR] OSD full dropping all updates' in line, lines))
    assert count == 0, 'Incorrect number of error messages expected 0 got %d' % count

    # State NEAR -> FULL
    log.info('2. Verify error messages when exceeding full_ratio')

    proc = mon.run(
             args=[
                 'sudo',
                 'daemon-helper',
                 'kill',
                 'ceph', '-w'
             ],
             stdin=run.PIPE,
             stdout=StringIO(),
             wait=False,
        )

    manager.raw_cluster_cmd('tell', 'osd.0', 'injectargs', '--osd_failsafe_full_ratio .00001')

    time.sleep(sleep_time)
    proc.stdin.close() # causes daemon-helper send SIGKILL to ceph -w
    proc.wait()

    lines = proc.stdout.getvalue().split('\n')

    count = len(filter(lambda line: '[ERR] OSD full dropping all updates' in line, lines))
    assert count == 2, 'Incorrect number of error messages expected 2 got %d' % count

    log.info('3. Verify write failure when exceeding full_ratio')

    # Write data should fail
    ret = rados(ctx, mon, ['-p', 'foo', 'put', 'newfile1', dummyfile])
    assert ret != 0, 'Expected write failure but it succeeded with exit status 0'

    # Put back default
    manager.raw_cluster_cmd('tell', 'osd.0', 'injectargs', '--osd_failsafe_full_ratio .97')
    time.sleep(10)

    # State FULL -> NEAR
    log.info('4. Verify write success when NOT exceeding full_ratio')

    # Write should succeed
    ret = rados(ctx, mon, ['-p', 'foo', 'put', 'newfile2', dummyfile2])
    assert ret == 0, 'Expected write to succeed, but got exit status %d' % ret

    log.info('5. Verify warning messages again when exceeding nearfull_ratio')

    proc = mon.run(
             args=[
                 'sudo',
                 'daemon-helper',
                 'kill',
                 'ceph', '-w'
             ],
             stdin=run.PIPE,
             stdout=StringIO(),
             wait=False,
        )

    time.sleep(sleep_time)
    proc.stdin.close() # causes daemon-helper send SIGKILL to ceph -w
    proc.wait()

    lines = proc.stdout.getvalue().split('\n')

    count = len(filter(lambda line: '[WRN] OSD near full' in line, lines))
    assert count == 1 or count == 2, 'Incorrect number of warning messages expected 1 or 2 got %d' % count
    count = len(filter(lambda line: '[ERR] OSD full dropping all updates' in line, lines))
    assert count == 0, 'Incorrect number of error messages expected 0 got %d' % count

    manager.raw_cluster_cmd('tell', 'osd.0', 'injectargs', '--osd_failsafe_nearfull_ratio .90')
    time.sleep(10)

    # State NONE -> FULL
    log.info('6. Verify error messages again when exceeding full_ratio')

    proc = mon.run(
             args=[
                 'sudo',
                 'daemon-helper',
                 'kill',
                 'ceph', '-w'
             ],
             stdin=run.PIPE,
             stdout=StringIO(),
             wait=False,
        )

    manager.raw_cluster_cmd('tell', 'osd.0', 'injectargs', '--osd_failsafe_full_ratio .00001')

    time.sleep(sleep_time)
    proc.stdin.close() # causes daemon-helper send SIGKILL to ceph -w
    proc.wait()

    lines = proc.stdout.getvalue().split('\n')

    count = len(filter(lambda line: '[WRN] OSD near full' in line, lines))
    assert count == 0, 'Incorrect number of warning messages expected 0 got %d' % count
    count = len(filter(lambda line: '[ERR] OSD full dropping all updates' in line, lines))
    assert count == 2, 'Incorrect number of error messages expected 2 got %d' % count

    # State FULL -> NONE
    log.info('7. Verify no messages settings back to default')

    manager.raw_cluster_cmd('tell', 'osd.0', 'injectargs', '--osd_failsafe_full_ratio .97')
    time.sleep(10)

    proc = mon.run(
             args=[
                 'sudo',
                 'daemon-helper',
                 'kill',
                 'ceph', '-w'
             ],
             stdin=run.PIPE,
             stdout=StringIO(),
             wait=False,
        )

    time.sleep(sleep_time)
    proc.stdin.close() # causes daemon-helper send SIGKILL to ceph -w
    proc.wait()

    lines = proc.stdout.getvalue().split('\n')

    count = len(filter(lambda line: '[WRN] OSD near full' in line, lines))
    assert count == 0, 'Incorrect number of warning messages expected 0 got %d' % count
    count = len(filter(lambda line: '[ERR] OSD full dropping all updates' in line, lines))
    assert count == 0, 'Incorrect number of error messages expected 0 got %d' % count

    log.info('Test Passed')

    # Bring all OSDs back in
    manager.remove_pool("foo")
    for osd in osds:
        if osd['osd'] != 0:
            manager.mark_in_osd(osd['osd'])
