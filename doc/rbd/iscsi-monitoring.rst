-----------------------------
Monitoring the iSCSI gateways
-----------------------------

Ceph provides an additional tool for iSCSI gateway environments
to monitor performance of exported RADOS Block Device (RBD) images.

The ``gwtop`` tool is a ``top``-like tool that displays aggregated
performance metrics of RBD images that are exported to clients over
iSCSI. The metrics are sourced from a Performance Metrics Domain Agent
(PMDA). Information from the Linux-IO target (LIO) PMDA is used to list
each exported RBD image with the connected client and its associated I/O
metrics.

**Requirements:**

-  A running Ceph iSCSI gateway

**Installing:**

#. As ``root``, install the ``ceph-iscsi-tools`` package on each iSCSI
   gateway node:

   ::

       # yum install ceph-iscsi-tools

#. As ``root``, install the performance co-pilot package on each iSCSI
   gateway node:

   ::

       # yum install pcp

#. As ``root``, install the LIO PMDA package on each iSCSI gateway node:

   ::

       # yum install pcp-pmda-lio

#. As ``root``, enable and start the performance co-pilot service on
   each iSCSI gateway node:

   ::

       # systemctl enable pmcd
       # systemctl start pmcd

#. As ``root``, register the ``pcp-pmda-lio`` agent:

   ::

       cd /var/lib/pcp/pmdas/lio
       ./Install

By default, ``gwtop`` assumes the iSCSI gateway configuration object is
stored in a RADOS object called ``gateway.conf`` in the ``rbd`` pool.
This configuration defines the iSCSI gateways to contact for gathering
the performance statistics. This can be overridden by using either the
``-g`` or ``-c`` flags. See ``gwtop --help`` for more details.

The LIO configuration determines which type of performance statistics to
extract from performance co-pilot. When ``gwtop`` starts it looks at the
LIO configuration, and if it find user-space disks, then ``gwtop``
selects the LIO collector automatically.

**Example ``gwtop`` Outputs**

::

    gwtop  2/2 Gateways   CPU% MIN:  4 MAX:  5    Network Total In:    2M  Out:    3M   10:20:00
    Capacity:   8G    Disks:   8   IOPS:  503   Clients:  1   Ceph: HEALTH_OK          OSDs:   3
    Pool.Image       Src    Size     iops     rMB/s     wMB/s   Client
    iscsi.t1703             500M        0      0.00      0.00
    iscsi.testme1           500M        0      0.00      0.00
    iscsi.testme2           500M        0      0.00      0.00
    iscsi.testme3           500M        0      0.00      0.00
    iscsi.testme5           500M        0      0.00      0.00
    rbd.myhost_1      T       4G      504      1.95      0.00   rh460p(CON)
    rbd.test_2                1G        0      0.00      0.00
    rbd.testme              500M        0      0.00      0.00

In the *Client* column, ``(CON)`` means the iSCSI initiator (client) is
currently logged into the iSCSI gateway. If ``-multi-`` is displayed,
then multiple clients are mapped to the single RBD image.
