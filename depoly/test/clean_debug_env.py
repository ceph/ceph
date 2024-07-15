import os
from depoly_tools import exec_cmd

target=exec_cmd("hostname")

#exec_cmd("ceph osd pool create debugpool 32 32")
#exec_cmd("ceph osd pool set debugpool size 1")
exec_cmd("sleep 1")

for i in range(10):
    # writearound
    exec_cmd("echo writethrough > /sys/block/bcache{cache_id}/bcache/cache_mode".format(cache_id=i))
    exec_cmd("sleep 2")
    pass

#exec_cmd("rbd create rbd1 size 50T")
for i in range(10):
    exec_cmd("ceph osd pool rm pool{osd_id} pool{osd_id} --yes-i-really-really-mean-it".format(osd_id=i))
    exec_cmd("ceph osd crush rule rm rule{osd_id}".format(osd_id=i))
    exec_cmd("ceph osd crush remove root{osd_id}".format(osd_id=i))

exec_cmd("ceph osd pool rm poolx poolx --yes-i-really-really-mean-it")
exec_cmd("ceph osd crush rule rm rulex")
exec_cmd("ceph osd crush rm rootx")

exec_cmd("ceph osd pool rm pooly pooly --yes-i-really-really-mean-it")
exec_cmd("ceph osd crush rule rm ruley")
exec_cmd("ceph osd crush rm rooty")
#exec_cmd("ceph osd pool create pool001 32 32")
