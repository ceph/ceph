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

To create a snapshot with ``rbd``, specify the ``snap create`` option, 
the pool name, the image name and the username. If you use ``cephx`` for 
authentication, you must also specify a key or a secret file. ::

	rbd --name {user-name} --keyfile=/path/to/secret --pool {pool-name} snap create --snap {snap-name} {image-name}

For example:: 

	rbd --name client.admin --pool rbd snap create --snap foo.snapname foo

List Snapshots
--------------

To list snapshots of an image, specify the pool name, the image name, and
the username. If you use ``cephx`` for authentication, you must also 
specify a key or a secret file. ::

	rbd --name {user-name} --keyfile=/path/to/secret --pool {pool-name} snap ls {image-name} 

For example::

	rbd --name client.admin --pool rbd snap ls foo 

Rollback Snapshot
-----------------

To rollback a snapshot with ``rbd``, specify the ``snap rollback`` option, 
the pool name, the image name and the username. If you use ``cephx`` for 
authentication, you must also specify a key or a secret file. :: 

	rbd --name {user-name} --keyfile=/path/to/secret --pool {pool-name} snap rollback --snap {snap-name} {image-name}

For example::

	rbd --name client.admin --pool rbd snap rollback --snap foo.snapname foo


Delete a Snapshot
-----------------
To delete a snapshot with ``rbd``, specify the ``snap rm`` option, 
the pool name, the image name and the username. If you use ``cephx`` for 
authentication, you must also specify a key or a secret file. :: 

	rbd --name {user-name} --keyfile=/path/to/secret --pool {pool-name} snap rm --snap {snap-name} {image-name}
	
For example:: 

	rbd --name client.admin --pool rbd snap rm --snap foo.snapname foo
