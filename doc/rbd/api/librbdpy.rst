.. _rbd api py:

================
 Librbd (Python)
================

.. highlight:: python

The ``rbd`` python module provides file-like access to RBD images.


Example: Creating and writing to an image
=========================================

To use ``rbd``, you must first import it and ``rados``, connect to RADOS,
and open an I/O context::

    import rados
    import rbd

    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
    ioctx = cluster.open_ioctx('rbd1701')

Then instantiate an :class:`rbd.RBD` object and use it to create the
image::

    rbd_inst = rbd.RBD()
    size = 4 * 1024**3  # 4 GiB
    rbd_inst.create(ioctx, 'image1701', size)

.. warning::
   ``Image.write()`` overwrites the data at the offset it is given, and can
   do so while another client, such as a virtual machine, is using the
   image. In this example, write only to the image that you have just
   created. If ``create()`` raised ``rbd.ImageExists``, the name belongs to
   an existing image: stop, and choose another name.

To perform I/O on the image, instantiate an :class:`rbd.Image` object::

    image = rbd.Image(ioctx, 'image1701')
    data = b'foo' * 200
    image.write(data, 0)

This writes 'foo' to the first 600 bytes of the image. Note that data
must be ``bytes``; a ``str`` raises ``TypeError``.

In the end, close the image, the I/O context and the connection to RADOS::

    image.close()
    ioctx.close()
    cluster.shutdown()

To be safe, each of these calls would need to be in a separate ``finally``
block::

    import rados
    import rbd

    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    try:
        cluster.connect()
        ioctx = cluster.open_ioctx('rbd1701')
        try:
            rbd_inst = rbd.RBD()
            size = 4 * 1024**3  # 4 GiB
            rbd_inst.create(ioctx, 'image1701', size)
            image = rbd.Image(ioctx, 'image1701')
            try:
                data = b'foo' * 200
                image.write(data, 0)
            finally:
                image.close()
        finally:
            ioctx.close()
    finally:
        cluster.shutdown()

This can be cumbersome, so the :class:`Rados`, :class:`Ioctx`, and
:class:`Image` classes can be used as context managers that close/shutdown
automatically (see :pep:`343`). Using them as context managers, the
above example becomes::

    import rados
    import rbd

    with rados.Rados(conffile='/etc/ceph/ceph.conf') as cluster:
        with cluster.open_ioctx('rbd1701') as ioctx:
            rbd_inst = rbd.RBD()
            size = 4 * 1024**3  # 4 GiB
            rbd_inst.create(ioctx, 'image1701', size)
            with rbd.Image(ioctx, 'image1701') as image:
                data = b'foo' * 200
                image.write(data, 0)

API Reference
=============

.. automodule:: rbd
    :members: RBD, Image, SnapIterator
