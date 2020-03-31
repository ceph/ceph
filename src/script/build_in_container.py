#! /usr/bin/python3

import os
import sys
import pexpect
import argparse
import multiprocessing

os.chdir('{}/..'.format(os.path.dirname(os.path.abspath(__file__))))

parser = argparse.ArgumentParser(description="Build HEAD in a container")
parser.add_argument('-c', '--cpus', default=multiprocessing.cpu_count(),
                    help="Number of cpus to pass to make/ctest")
parser.add_argument('-t', '--test', default=False, action='store_true', help="Run ctest tests")
parser.add_argument('-o', '--os', default="fedora", help="Distro to build")
parser.add_argument('-v', '--version', default="31", help="Version to build")
args = parser.parse_args()

child = pexpect.spawn(
        './test/docker-test.sh --os-type {} --os-version {} --shell'.format(
            args.os, args.version))
child.timeout=21600 # six hours
child.logfile_read = sys.stdout.buffer
child.expect(['bash-5.0\$ ', '~/working/src/ceph-ubuntu-18.04-.*\$ '])
child.sendline('./do_cmake.sh')
child.expect(['bash-5.0\$ ', '~/working/src/ceph-ubuntu-18.04-.*\$ '])
child.sendline('cd build')
child.expect(['bash-5.0\$ ', '~/working/src/ceph-ubuntu-18.04-.*\$ '])
child.sendline('make -j{} all tests'.format(args.cpus))
child.expect(['bash-5.0\$ ', '~/working/src/ceph-ubuntu-18.04-.*\$ '])
if (args.test):
    child.sendline('ctest -j{}'.format(args.cpus))
    child.expect(['bash-5.0\$ ', '~/working/src/ceph-ubuntu-18.04-.*\$ '])
child.logfile_read = None
child.interact()
if child.isalive():
    child.close()
