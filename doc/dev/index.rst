==================================
 Internal developer documentation
==================================

.. note:: If you're looking for how to use Ceph as a library from your
   own software, please see :doc:`/api/index`.

You can start a development mode Ceph cluster, after compiling the source, with::

	cd src
	install -d -m0755 out dev/osd0
	./vstart.sh -n -x -l
	# check that it's there
	./ceph health

.. todo:: vstart is woefully undocumented and full of sharp sticks to poke yourself with.


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
