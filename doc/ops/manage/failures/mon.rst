==================================
 Recovering from ceph-mon failure
==================================

Any single ceph-mon failure should not take down the entire monitor
cluster as long as a majority of the nodes are available.  If that
is the case--the remaining nodes are able to form a quorum--the ``ceph
health`` command will report any problems::

 $ ceph health
 HEALTH_WARN 1 mons down, quorum 0,2

and::

 $ ceph health detail
 HEALTH_WARN 1 mons down, quorum 0,2
 mon.b (rank 1) addr 192.168.106.220:6790/0 is down (out of quorum)

Generally speaking, simply restarting the affected node will repair things.

If there are not enough monitors to form a quorum, the ``ceph``
command will block trying to reach the cluster.  In this situation,
you need to get enough ``ceph-mon`` daemons running to form a quorum
before doing anything else with the cluster.


Replacing a monitor
===================

If, for some reason, a monitor data store becomes corrupt, the monitor
can be recreated and allowed to rejoin the cluster, much like a normal
monitor cluster expansion.  See :ref:`adding-mon`.




