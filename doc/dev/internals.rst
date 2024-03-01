================
 Ceph Internals
================

.. note:: For information on how to use Ceph as a library (from your own
   software), see :doc:`/api/index`.

Starting a Development-mode Ceph Cluster
----------------------------------------

Compile the source and then run the following commands to start a
development-mode Ceph cluster::

	cd build
	OSD=3 MON=3 MGR=3 ../src/vstart.sh -n -x
	# check that it's there
	bin/ceph health

.. rubric:: Mailing list

The ``dev@ceph.io`` list is for discussion about the development of Ceph,
its interoperability with other technology, and the operations of the
project itself.  Subscribe by sending a message to ``dev-request@ceph.io``
with the line::

 subscribe ceph-devel

in the body of the message.

The ceph-devel@vger.kernel.org list is for discussion
and patch review for the Linux kernel Ceph client component.
Subscribe by sending a message to ``majordomo@vger.kernel.org`` with the line::

 subscribe ceph-devel

in the body of the message.

.. raw:: html

   <!---

.. rubric:: Contents

.. toctree::
   :glob:

   *
   osd_internals/index*
   mds_internals/index*
   radosgw/index*
   ceph-volume/index*
   crimson/index*

.. raw:: html

   --->
