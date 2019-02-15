=================
 Add/Remove OSDs
=================

Adding and removing Ceph OSD Daemons to your cluster may involve a few more
steps when compared to adding and removing other Ceph daemons. Ceph OSD Daemons
write data to the disk and to journals. So you need to provide a disk for the
OSD and a path to the journal partition (i.e., this is the most common
configuration, but you may configure your system to  your own needs).

In Ceph v0.60 and later releases, Ceph supports ``dm-crypt`` on disk encryption.
You may specify the ``--dmcrypt`` argument when preparing an OSD to tell
``ceph-deploy`` that you want to use encryption. You may also specify the
``--dmcrypt-key-dir`` argument to specify the location of ``dm-crypt``
encryption keys.

You should test various drive configurations to gauge their throughput before
before building out a large cluster. See `Data Storage`_ for additional details.


List Disks
==========

To list the disks on a node, execute the following command::

	ceph-deploy disk list {node-name [node-name]...}


Zap Disks
=========

To zap a disk (delete its partition table) in preparation for use with Ceph,
execute the following::

	ceph-deploy disk zap {osd-server-name}:{disk-name}
	ceph-deploy disk zap osdserver1:sdb

.. important:: This will delete all data.


Create OSDs
===========

Once you create a cluster, install Ceph packages, and gather keys, you
may create the OSDs and deploy them to the OSD node(s). If you need to
identify a disk or zap it prior to preparing it for use as an OSD,
see `List Disks`_ and `Zap Disks`_. ::

	ceph-deploy osd create --data {data-disk} {node-name}

For example::

	ceph-deploy osd create --data /dev/ssd osd-server1

For bluestore (the default) the example assumes a disk dedicated to one Ceph
OSD Daemon. Filestore is also supported, in which case a ``--journal`` flag in
addition to ``--filestore`` needs to be used to define the Journal device on
the remote host.

.. note:: When running multiple Ceph OSD daemons on a single node, and
   sharing a partioned journal with each OSD daemon, you should consider
   the entire node the minimum failure domain for CRUSH purposes, because
   if the SSD drive fails, all of the Ceph OSD daemons that journal to it
   will fail too.


List OSDs
=========

To list the OSDs deployed on a node(s), execute the following command::

 ceph-deploy osd list {node-name}


Destroy OSDs
============

.. note:: Coming soon. See `Remove OSDs`_ for manual procedures.

.. To destroy an OSD, execute the following command::

..	ceph-deploy osd destroy {node-name}:{path-to-disk}[:{path/to/journal}]

.. Destroying an OSD will take it ``down`` and ``out`` of the cluster.

.. _Data Storage: ../../../start/hardware-recommendations#data-storage
.. _Remove OSDs: ../../operations/add-or-rm-osds#removing-osds-manual
