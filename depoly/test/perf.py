import os
from depoly_tools import exec_cmd

target=exec_cmd("hostname")
osd_id = 0
#exec_cmd("ceph daemon osd.{oid} config set debug_bdev 5/10".format(oid=osd_id))
exec_cmd("ceph daemon osd.{oid} perf dump".format(oid=osd_id))
#exec_cmd("ceph daemon osd.{oid} perf reset all".format(oid=osd_id))
#exec_cmd("ceph daemon osd.{oid} perf dump".format(oid=osd_id))
