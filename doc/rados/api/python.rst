.. _librados-python:

===================
 Librados (Python)
===================

The ``rados`` module is a thin Python wrapper for ``librados``.

Installation
============

To install Python libraries for Ceph, see `Getting librados for Python`_.


Getting Started
===============

You can create your own Ceph client using Python. The following tutorial will
show you how to import the Ceph Python module, connect to a Ceph cluster, and
perform object operations as a ``client.admin`` user.

.. note:: To use the Ceph Python bindings, you must have access to a
   running Ceph cluster. To set one up quickly, see `Getting Started`_.

First, create a Python source file for your Ceph client.

.. prompt:: bash

	vim client.py


Import the Module
-----------------

To use the ``rados`` module, import it into your source file.

.. code-block:: python
   :linenos:

	import rados


Configure a Cluster Handle
--------------------------

Before connecting to the Ceph Storage Cluster, create a cluster handle. By
default, the cluster handle assumes a cluster named ``ceph`` (i.e., the default
for deployment tools, and our Getting Started guides too),  and a
``client.admin`` user name. You may change these defaults to suit your needs.

To connect to the Ceph Storage Cluster, your application needs to know where to
find the  Ceph Monitor. Provide this information to your application by
specifying the path to your Ceph configuration file, which contains the location
of the initial Ceph monitors.

.. code-block:: python
   :linenos:

	import rados, sys

	#Create Handle Examples.
	cluster = rados.Rados(conffile='ceph.conf')
	cluster = rados.Rados(conffile=sys.argv[1])
	cluster = rados.Rados(conffile = 'ceph.conf', conf = dict (keyring = '/path/to/keyring'))

Ensure that the ``conffile`` argument provides the path and file name of your
Ceph configuration file. You may use the ``sys`` module to avoid hard-coding the
Ceph configuration path and file name.

Your Python client also requires a client keyring. For this example, we use the
``client.admin`` key by default. If you would like to specify the keyring when
creating the cluster handle, you may use the ``conf`` argument. Alternatively,
you may specify the keyring path in your Ceph configuration file. For example,
you may add something like the following line to your Ceph configuration file::

	keyring = /path/to/ceph.client.admin.keyring

For additional details on modifying your configuration via Python, see `Cluster Handle API`_.


Connect to the Cluster
----------------------

Once you have a cluster handle configured, you may connect to the cluster.
With a connection to the cluster, you may execute methods that return
information about the cluster.

.. code-block:: python
   :linenos:
   :emphasize-lines: 7

	import rados, sys

	cluster = rados.Rados(conffile='ceph.conf')
	print("\nlibrados version: {}".format(str(cluster.version())))
	print("Will attempt to connect to: {}".format(str(cluster.conf_get('mon host'))))

	cluster.connect()
	print("\nCluster ID: {}".format(cluster.get_fsid()))

	print("\n\nCluster Statistics")
	print("==================")
	cluster_stats = cluster.get_cluster_stats()

	for key, value in cluster_stats.items():
		print(key, value)


By default, Ceph authentication is ``on``. Your application will need to know
the location of the keyring. The ``python-ceph`` module doesn't have the default
location, so you need to specify the keyring path. The easiest way to specify
the keyring is to add it to the Ceph configuration file. The following Ceph
configuration file example uses the ``client.admin`` keyring.

.. code-block:: ini
   :linenos:

	[global]
	# ... elided configuration
	keyring = /path/to/keyring/ceph.client.admin.keyring


Manage Pools
------------

When connected to the cluster, the ``Rados`` API allows you to manage pools. You
can list pools, check for the existence of a pool, create a pool and delete a
pool.

.. code-block:: python
   :linenos:
   :emphasize-lines: 6, 13, 18, 25

	print("\n\nPool Operations")
	print("===============")

	print("\nAvailable Pools")
	print("----------------")
	pools = cluster.list_pools()

	for pool in pools:
		print(pool)

	print("\nCreate 'test' Pool")
	print("------------------")
	cluster.create_pool('test')

	print("\nPool named 'test' exists: {}".format(str(cluster.pool_exists('test'))))
	print("\nVerify 'test' Pool Exists")
	print("-------------------------")
	pools = cluster.list_pools()

	for pool in pools:
		print(pool)

	print("\nDelete 'test' Pool")
	print("------------------")
	cluster.delete_pool('test')
	print("\nPool named 'test' exists: {}".format(str(cluster.pool_exists('test'))))


Input/Output Context
--------------------

Reading from and writing to the Ceph Storage Cluster requires an input/output
context (ioctx). You can create an ioctx with the ``open_ioctx()`` or
``open_ioctx2()`` method of the ``Rados`` class. The ``ioctx_name`` parameter
is the name of the pool and ``pool_id`` is the ID of the pool you wish to use.

.. code-block:: python
   :linenos:

	ioctx = cluster.open_ioctx('data')


or

.. code-block:: python
   :linenos:

        ioctx = cluster.open_ioctx2(pool_id)


Once you have an I/O context, you can read/write objects, extended attributes,
and perform a number of other operations. After you complete operations, ensure
that you close the connection. For example:

.. code-block:: python
   :linenos:

	print("\nClosing the connection.")
	ioctx.close()


Writing, Reading and Removing Objects
-------------------------------------

Once you create an I/O context, you can write objects to the cluster. If you
write to an object that doesn't exist, Ceph creates it. If you write to an
object that exists, Ceph overwrites it (except when you specify a range, and
then it only overwrites the range). You may read objects (and object ranges)
from the cluster. You may also remove objects from the cluster. For example:

.. code-block:: python
	:linenos:
	:emphasize-lines: 2, 5, 8

	print("\nWriting object 'hw' with contents 'Hello World!' to pool 'data'.")
	ioctx.write_full("hw", "Hello World!")

	print("\n\nContents of object 'hw'\n------------------------\n")
	print(ioctx.read("hw"))

	print("\nRemoving object 'hw'")
	ioctx.remove_object("hw")


Writing and Reading XATTRs
--------------------------

Once you create an object, you can write extended attributes (XATTRs) to
the object and read XATTRs from the object. For example:

.. code-block:: python
	:linenos:
	:emphasize-lines: 2, 5

	print("\n\nWriting XATTR 'lang' with value 'en_US' to object 'hw'")
	ioctx.set_xattr("hw", "lang", "en_US")

	print("\n\nGetting XATTR 'lang' from object 'hw'\n")
	print(ioctx.get_xattr("hw", "lang"))


Listing Objects
---------------

If you want to examine the list of objects in a pool, you may
retrieve the list of objects and iterate over them with the object iterator.
For example:

.. code-block:: python
	:linenos:
	:emphasize-lines: 1, 6, 7, 13

	object_iterator = ioctx.list_objects()

	while True :

		try :
			rados_object = object_iterator.__next__()
			print("Object contents = {}".format(rados_object.read()))

		except StopIteration :
			break

	# Or alternatively
	[print("Object contents = {}".format(obj.read())) for obj in ioctx.list_objects()]

The ``Object`` class provides a file-like interface to an object, allowing
you to read and write content and extended attributes. Object operations using
the I/O context provide additional functionality and asynchronous capabilities.


Cluster Handle API
==================

The ``Rados`` class provides an interface into the Ceph Storage Cluster.

You do not need to be connected to the Ceph Storage Cluster to get and set
configuration values, read the Ceph configuration file, or parse arguments.
See `Storage Cluster Configuration`_ for details on settings. Pool operations,
cluster statistics, and the ``mon_command()``, ``osd_command()``,
``mgr_command()``, and ``pg_command()`` methods require a connection. The Ceph
CLI sends its commands through these command methods.

.. currentmodule:: rados

.. autoclass:: Rados
   :members:
   :exclude-members: require_state

   .. documented manually because it raises warnings because of *args usage in
   .. the signature

   .. py:method:: require_state(*args)

      Checks if the Rados object is in a special state

      :param args: Any number of states to check as separate arguments
      :raises: :class:`RadosStateError`


Input/Output Context API
========================

To write data to and read data from the Ceph Object Store, you must create
an Input/Output context (ioctx). The `Rados` class provides `open_ioctx()`
and `open_ioctx2()` methods. The remaining ``ioctx`` operations involve
invoking methods of the `Ioctx` and other classes.

The Ceph Storage Cluster stores data as objects. You can read and write objects
synchronously or asynchronously, and you can read and write from offsets. You
may set extended attributes (XATTRs) on an object, and you can retrieve a list
of objects or XATTRs and iterate over them.

.. autoclass:: Ioctx
   :members:


Object Interface
================

From an I/O context, you can retrieve a list of objects from a pool and iterate
over them. The object interface makes each object look like a file, and you may
perform synchronous operations on the objects. For asynchronous operations, use
the I/O context methods.

.. autoclass:: Object
   :members:


Other Classes
=============

``Ioctx`` methods return these classes for asynchronous completions, batched
read and write operations, watches, snapshots, and iteration.

.. autoclass:: Completion
   :members:

.. autoclass:: WriteOp
   :members:

.. autoclass:: ReadOp
   :members:

.. autoclass:: Watch
   :members:

.. autoclass:: Snap
   :members:

.. autoclass:: ObjectIterator
   :members:
   :special-members: __next__

.. autoclass:: XattrIterator
   :members:
   :special-members: __next__

.. autoclass:: OmapIterator
   :members:
   :special-members: __next__

.. autoclass:: SnapIterator
   :members:
   :special-members: __next__




.. _Getting Started: ../../../start
.. _Storage Cluster Configuration: ../../configuration
.. _Getting librados for Python: ../librados-intro#getting-librados-for-python
