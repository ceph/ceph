==================
 RBD Snapshotting
==================
One of the advanced features of RADOS block devices is that you can create 
snapshots of the images to retain a history of an image's state. Ceph supports
RBD snapshots from the ``rbd`` command, from a kernel object, from a 
KVM, and from cloud solutions. Once you create snapshots of an image, you 
can rollback to a snapshot, list snapshots, remove snapshots and purge 
the snapshots.

.. important:: To use use RBD snapshots, you must have a running Ceph cluster.

.. important:: Generally, you should stop i/o before snapshotting an image.
   If the image contains a filesystem, the filesystem should be in a
   consistent state before snapshotting too.
   

Create Snapshot
---------------
<placeholder>

You can take snapshots of RBD images. 

List Snapshots
--------------
<placeholder>


Rollback Snapshot
-----------------
<placeholder>


Delete a Snapshot
-----------------
<placeholder>