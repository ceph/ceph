import os
from depoly_tools import exec_cmd

target=exec_cmd("hostname")
exec_cmd("sleep 1")
nice="min"
if (nice == "min"):
    exec_cmd("ceph tell osd.* injectargs '--osd_scrub_sleep 3 --osd_scrub_chunk_max 1 --osd_scrub_chunk_min 1 --osd_max_scrubs 1'")
else:
    exec_cmd("ceph tell osd.* injectargs '--osd_scrub_sleep 0 --osd_scrub_chunk_max 64 --osd_scrub_chunk_min 64 --osd_max_scrubs 64'")
