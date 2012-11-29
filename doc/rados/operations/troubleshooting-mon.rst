==================================
 Recovering from Monitor Failures
==================================

In production clusters, we recommend running the cluster with a minimum
of three monitors. The failure of a single monitor should not take down
the entire monitor cluster, provided a majority of the monitors remain
available. If the majority of nodes are available, the remaining nodes
will be able to form a quorum.

When you check your cluster's health, you may notice that a monitor
has failed. For example:: 

	ceph health
	HEALTH_WARN 1 mons down, quorum 0,2

For additional detail, you may check the cluster status::

	ceph status
	HEALTH_WARN 1 mons down, quorum 0,2
	mon.b (rank 1) addr 192.168.106.220:6790/0 is down (out of quorum)

In most cases, you can simply restart the affected node. 
For example:: 

	service ceph -a restart {failed-mon}

If there are not enough monitors to form a quorum, the ``ceph``
command will block trying to reach the cluster.  In this situation,
you need to get enough ``ceph-mon`` daemons running to form a quorum
before doing anything else with the cluster.