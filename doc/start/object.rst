=======================
 Starting to use RADOS
=======================

.. highlight:: python


Introduction
============

`RADOS` is the object storage component of Ceph.

An object, in this context, means a named entity that has

- a `name`: a sequence of bytes, unique within its container, that is
  used to locate and access the object
- `content`: sequence of bytes
- `metadata`: a mapping from keys to values, for example ``color:
  blue, importance: low``

None of these have any prescribed meaning to Ceph, and can be freely
chosen by the user.

`RADOS` takes care of distributing the objects across the whole
storage cluster and replicating them for fault tolerance.


Installation
============

To use `RADOS`, you need to install a Ceph cluster. Follow the
instructions in :doc:`/ops/install/index`. Continue with these
instructions once you have a healthy cluster running.


Setup
=====

First, we need to create a `pool` that will hold our assets. Follow
the instructions in :ref:`create-new-pool`. Let's name the pool
``assets``.

Then, we need a ``client`` key that is authorized to access that
pool. Follow the instructions in :ref:`add-new-key`. Let's set the
``id`` of the key to be ``webapp``. You could set up one key per
machine running the web service, or let them share a single key; your
call. Make sure the keyring containing the new key is available on the
machine running the asset management system.

Then, authorize the key to access the new pool. Follow the
instructions in :ref:`auth-pool`.


Usage
=====

`RADOS` is accessed via a network protocol, implemented in the
:doc:`/api/librados` and :doc:`/api/libradospp` libraries. There are
also wrappers for other languages.

.. todo:: link to python, phprados here

Instead of a low-level programming library, you can also use a
higher-level service, with user accounts, access control and such
features, via the :ref:`radosgw` HTTP service. See :doc:`/ops/radosgw`
for more.


.. rubric:: Example: Asset management

Let's say we write our asset management system in Python. We'll use
the ``rados`` Python module for accessing `RADOS`.

.. todo:: link to rados.py, where ever it'll be documented

With the key we created in Setup_, we'll be able to open a RADOS
connection::

	import rados

	r=rados.Rados('webapp')
	r.conf_read_file()
	r.connect()

	ioctx = r.open_ioctx('assets')

and then write an object::

	# holding content fully in memory to make the example simpler;
	# see API docs for how to do this better
	ioctx.write_full('1.jpg', 'jpeg-content-goes-here')

and read it back::

	# holding content fully in memory to make the example simpler;
	# see API docs for how to do this better
	content = ioctx.write_full('1.jpg')


We can also manipulate the metadata related to the object::

	ioctx.set_xattr('1.jpg', 'content-type', 'image/jpeg')


Now you can use these as fits the web server framework of your choice,
passing the ``ioctx`` variable from initialization to the request
serving function.
