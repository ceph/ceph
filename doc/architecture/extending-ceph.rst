.. index:: Extensibility, Ceph Classes

Extending Ceph
==============

You can extend Ceph by creating shared object classes called 'Ceph Classes'.
Ceph loads ``.so`` classes stored in the ``osd class dir`` directory dynamically
(i.e., ``$libdir/rados-classes`` by default). When you implement a class, you
can create new object methods that have the ability to call the native methods
in the Ceph Object Store, or other class methods you incorporate via libraries
or create yourself.

On writes, Ceph Classes can call native or class methods, perform any series of
operations on the inbound data and generate a resulting write transaction  that
Ceph will apply atomically.

On reads, Ceph Classes can call native or class methods, perform any series of
operations on the outbound data and return the data to the client.

.. topic:: Ceph Class Example

   A Ceph class for a content management system that presents pictures of a
   particular size and aspect ratio could take an inbound bitmap image, crop it
   to a particular aspect ratio, resize it and embed an invisible copyright or
   watermark to help protect the intellectual property; then, save the
   resulting bitmap image to the object store.

See ``src/objclass/objclass.h``, ``src/fooclass.cc`` and ``src/barclass`` for
exemplary implementations.


Summary
=======

Ceph Storage Clusters are dynamic--like a living organism. Although many storage
appliances do not fully utilize the CPU and RAM of a typical commodity server,
Ceph does. From heartbeats, to  peering, to rebalancing the cluster or
recovering from faults,  Ceph offloads work from clients (and from a centralized
gateway which doesn't exist in the Ceph architecture) and uses the computing
power of the OSDs to perform the work. When referring to :ref:`hardware-recommendations`
and the `Network Config Reference`_,  be cognizant of the
foregoing concepts to understand how Ceph utilizes computing resources.


.. _Network Config Reference: ../../rados/configuration/network-config-ref
