====================================
 Kernel client troubleshooting (FS)
====================================

If there is an issue with the cephfs kernel client, the most important thing is
figuring out whether the problem is with the client or the MDS. Generally,
this is easy to work out. If the kernel client broke directly, there
will be output in dmesg. Collect it and any appropriate kernel state. If
the problem is with the MDS, there will be hung requests that the client
is waiting on. Look in ``/sys/kernel/debug/ceph/*/`` and cat the ``mdsc`` file to
get a listing of requests in progress. If one of them remains there, the
MDS has probably "forgotten" it.
We can get hints about what's going on by dumping the MDS cache:
ceph mds tell 0 dumpcache /tmp/dump.txt

And if high logging levels are set on the MDS, that will almost certainly
hold the information we need to diagnose and solve the issue.
