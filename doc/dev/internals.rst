================
 Ceph Internals
================

.. note:: If you're looking for how to use Ceph as a library from your
   own software, please see :doc:`/api/index`.

You can start a development mode Ceph cluster, after compiling the source, with::

	cd build
	OSD=3 MON=3 MGR=3 ../src/vstart.sh -n -x
	# check that it's there
	bin/ceph health

.. _mailing-list:

.. rubric:: Mailing list

The official development email list is ``ceph-devel@vger.kernel.org``.  Subscribe by sending
a message to ``majordomo@vger.kernel.org`` with the line::

 subscribe ceph-devel

in the body of the message.


.. rubric:: Contents

.. toctree::
   :glob:

   *
   osd_internals/index*
   mds_internals/index*
   radosgw/index*
   ceph-volume/index*
