=========================
 Checking Cluster Health
=========================
When you start the Ceph cluster, it may take some time to reach a healthy
state. You can check on the health of your Ceph cluster with the following::

	ceph health

If you specified non-default locations for your configuration or keyring::

   ceph -c /path/to/conf -k /path/to/keyring health

Upon starting the Ceph cluster, you will likely encounter a health
warning such as ``HEALTH_WARN XXX num pgs stale``. Wait a few moments and check
it again. When your cluster is ready, ``ceph health`` should return a message
such as ``HEALTH_OK``. At that point, it is okay to begin using the cluster.