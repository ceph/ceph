#!/usr/bin/python
# -*- mode:python -*-
# vim: ts=4 sw=4 smarttab expandtab
#
# Copyright (C) 2017 Red Hat <contact@redhat.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#

import json
import rados
import shlex
import subprocess
import time

def cleanup(cluster):
    cluster.delete_pool('large-omap-test-pool')
    cluster.shutdown()

def init():
    # For local testing
    #cluster = rados.Rados(conffile='./ceph.conf')
    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
    print("\nCluster ID: " + cluster.get_fsid())
    cluster.create_pool('large-omap-test-pool')
    ioctx = cluster.open_ioctx('large-omap-test-pool')
    ioctx.write_full('large-omap-test-object1', "Lorem ipsum")
    op = ioctx.create_write_op()

    keys = []
    values = []
    for x in range(20001):
        keys.append(str(x))
        values.append("X")

    ioctx.set_omap(op, tuple(keys), tuple(values))
    ioctx.operate_write_op(op, 'large-omap-test-object1', 0)
    ioctx.release_write_op(op)

    ioctx.write_full('large-omap-test-object2', "Lorem ipsum dolor")
    op = ioctx.create_write_op()

    buffer = ("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do "
              "eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut "
              "enim ad minim veniam, quis nostrud exercitation ullamco laboris "
              "nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in "
              "reprehenderit in voluptate velit esse cillum dolore eu fugiat "
              "nulla pariatur. Excepteur sint occaecat cupidatat non proident, "
              "sunt in culpa qui officia deserunt mollit anim id est laborum.")

    keys = []
    values = []
    for x in xrange(20000):
        keys.append(str(x))
        values.append(buffer)

    ioctx.set_omap(op, tuple(keys), tuple(values))
    ioctx.operate_write_op(op, 'large-omap-test-object2', 0)
    ioctx.release_write_op(op)
    ioctx.close()
    return cluster

def get_deep_scrub_timestamp(pgid):
    cmd = ['ceph', 'pg', 'dump', '--format=json-pretty']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out = proc.communicate()[0]
    for stat in json.loads(out)['pg_stats']:
        if stat['pgid'] == pgid:
            return stat['last_deep_scrub_stamp']

def wait_for_scrub():
    osds = set();
    pgs = dict();
    cmd = ['ceph', 'osd', 'map', 'large-omap-test-pool',
           'large-omap-test-object1', '--format=json-pretty']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out = proc.communicate()[0]
    osds.add(json.loads(out)['acting_primary'])
    pgs[json.loads(out)['pgid']] = get_deep_scrub_timestamp(json.loads(out)['pgid'])
    cmd = ['ceph', 'osd', 'map', 'large-omap-test-pool',
           'large-omap-test-object2', '--format=json-pretty']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out = proc.communicate()[0]
    osds.add(json.loads(out)['acting_primary'])
    pgs[json.loads(out)['pgid']] = get_deep_scrub_timestamp(json.loads(out)['pgid'])

    for osd in osds:
        command = "ceph osd deep-scrub osd." + str(osd)
        subprocess.check_call(shlex.split(command))

    for pg in pgs:
        RETRIES = 0
        while RETRIES < 60 and pgs[pg] == get_deep_scrub_timestamp(pg):
            time.sleep(10)
            RETRIES += 1

def check_health_output():
    RETRIES = 0
    result = 0
    while RETRIES < 6 and result != 2:
        result = 0
        RETRIES += 1
        output = subprocess.check_output(["ceph", "health", "detail"])
        for line in output.splitlines():
            result += int(line.find('2 large omap objects') != -1)
        time.sleep(10)

    if result != 2:
        print("Error, got invalid output:")
        print(output)
        raise Exception

def main():
    cluster = init()
    wait_for_scrub()
    check_health_output()

    cleanup(cluster)

if __name__ == '__main__':
    main()
