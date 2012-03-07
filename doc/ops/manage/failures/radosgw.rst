=================================
 Recovering from radosgw failure
=================================


HTTP request errors
===================

Examining the access and error logs for the web server itself is
probably the first step in identifying what is going on.  If there is
a 500 error, that usually indicates a problem communicating with the
radosgw daemon.  Ensure the daemon is running, its socket path is
configured, and that the web server is looking for it in the proper
location.


Crashed radosgw process
=======================

If the ``radosgw`` process dies, you will normally see a 500 error
from the web server (apache, nginx, etc.).  In that situation, simply
restarting radosgw will restore service.

To diagnose the cause of the crash, check the log in ``/var/log/ceph``
and/or the core file (if one was generated).


Blocked radosgw requests
========================

If some (or all) radosgw requests appear to be blocked, you can get
some insight into the internal state of the ``radosgw`` daemon via
its admin socket.  By default, there will be a socket configured to
reside in ``/var/run/ceph``, and the daemon can be queried with::

 $ ceph --admin-daemon /var/run/ceph/client.rgw help
 help                list available commands
 objecter_requests   show in-progress osd requests
 perfcounters_dump   dump perfcounters value
 perfcounters_schema dump perfcounters schema
 version             get protocol version

Of particular interest::

 $ ceph --admin-daemon /var/run/ceph/client.rgw objecter_requests
 ...

will dump information about current in-progress requests with the
RADOS cluster.  This allows one to identify if any requests are blocked
by a non-responsive ceph-osd.

