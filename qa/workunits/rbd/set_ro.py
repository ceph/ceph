#!/usr/bin/env python

import logging
import subprocess
import sys

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger()

def run_command(args, except_on_error=True):
    log.debug('running command "%s"', ' '.join(args))
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if out:
        log.debug('stdout: %s', out)
    if err:
        log.debug('stderr: %s', err)
    if proc.returncode:
        log.debug('ret: %d', proc.returncode)
        if except_on_error:
            raise subprocess.CalledProcessError(proc.returncode, ' '.join(args))
    return (proc.returncode, out, err)

def setup(image_name):
    run_command(['rbd', 'create', '-s', '100', image_name])
    run_command(['rbd', 'snap', 'create', image_name + '@snap'])
    run_command(['rbd', 'map', image_name])
    run_command(['rbd', 'map', image_name + '@snap'])

def teardown(image_name, fail_on_error=True):
    run_command(['rbd', 'unmap', '/dev/rbd/rbd/' + image_name + '@snap'], fail_on_error)
    run_command(['rbd', 'unmap', '/dev/rbd/rbd/' + image_name], fail_on_error)
    run_command(['rbd', 'snap', 'rm', image_name + '@snap'], fail_on_error)
    run_command(['rbd', 'rm', image_name], fail_on_error)

def write(target, expect_fail=False):
    try:
        with open(target, 'w', 0) as f:
            f.write('test')
            f.flush()
        assert not expect_fail, 'writing should have failed'
    except IOError:
        assert expect_fail, 'writing should not have failed'

def test_ro(image_name):
    dev = '/dev/rbd/rbd/' + image_name
    snap_dev = dev + '@snap'

    log.info('basic device is readable')
    write(dev)

    log.info('basic snapshot is read-only')
    write(snap_dev, True)

    log.info('cannot set snapshot rw')
    ret, _, _ = run_command(['blockdev', '--setrw', snap_dev], False)
    assert ret != 0, 'snapshot was set read-write!'
    run_command(['udevadm', 'settle'])
    write(snap_dev, True)

    log.info('set device ro')
    run_command(['blockdev', '--setro', dev])
    run_command(['udevadm', 'settle'])
    write(dev, True)

    log.info('cannot set device rw when in-use')
    with open(dev, 'r') as f:
        ret, _, _ = run_command(['blockdev', '--setro', dev], False)
        assert ret != 0, 'in-use device was set read-only!'
        run_command(['udevadm', 'settle'])

    write(dev, True)
    run_command(['blockdev', '--setro', dev])
    run_command(['udevadm', 'settle'])
    write(dev, True)

    run_command(['blockdev', '--setrw', dev])
    run_command(['udevadm', 'settle'])
    write(dev)
    run_command(['udevadm', 'settle'])
    run_command(['blockdev', '--setrw', dev])
    run_command(['udevadm', 'settle'])
    write(dev)

    log.info('cannot set device ro when in-use')
    with open(dev, 'r') as f:
        ret, _, _ = run_command(['blockdev', '--setro', dev], False)
        assert ret != 0, 'in-use device was set read-only!'
        run_command(['udevadm', 'settle'])

    run_command(['rbd', 'unmap', '/dev/rbd/rbd/' + image_name])
    run_command(['rbd', 'map', '--read-only', image_name])

    log.info('cannot write to newly mapped ro device')
    write(dev, True)

    log.info('can set ro mapped device rw')
    run_command(['blockdev', '--setrw', dev])
    run_command(['udevadm', 'settle'])
    write(dev)

def main():
    image_name = 'test1'
    # clean up any state from previous test runs
    teardown(image_name, False)
    setup(image_name)

    test_ro(image_name)

    teardown(image_name)

if __name__ == '__main__':
    main()
