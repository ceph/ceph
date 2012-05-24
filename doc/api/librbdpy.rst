================
 Librbd (Python)
================

.. highlight:: python

The `rbd` python module provides file-like access to RBD images.


Example: Creating and writing to an image
=========================================

To use `rbd`, you must first connect to RADOS and open an IO
context::

    cluster = rados.Rados(conffile='my_ceph.conf')
    cluster.connect()
    ioctx = cluster.open_ioctx('mypool')

Then you instantiate an :class:rbd.RBD object, which you use to create the
image::

    rbd_inst = rbd.RBD()
    size = 4 * 1024**3  # 4 GiB
    rbd_inst.create(ioctx, 'myimage', size)

To perform I/O on the image, you instantiate an :class:rbd.Image object::

    image = rbd.Image(ioctx, 'myimage')
    data = 'foo' * 200
    image.write(data, 0)

This writes 'foo' to the first 600 bytes of the image. Note that data
cannot be :type:unicode - `Librbd` does not know how to deal with
characters wider than a :c:type:char.

In the end, you'll want to close the image, the IO context and the connection to RADOS::

    image.close()
    ioctx.close()
    cluster.shutdown()

To be safe, each of these calls would need to be in a separate :finally
block::

    cluster = rados.Rados(conffile='my_ceph_conf')
    try:
        ioctx = cluster.open_ioctx('my_pool')
        try:
            rbd_inst = rbd.RBD()
            size = 4 * 1024**3  # 4 GiB
            rbd_inst.create(ioctx, 'myimage', size)
            image = rbd.Image(ioctx, 'myimage')
            try:
                data = 'foo' * 200
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

    with rados.Rados(conffile='my_ceph.conf') as cluster:
        with cluster.open_ioctx('mypool') as ioctx:
            rbd_inst = rbd.RBD()
            size = 4 * 1024**3  # 4 GiB
            rbd_inst.create(ioctx, 'myimage', size)
            with rbd.Image(ioctx, 'myimage') as image:
                data = 'foo' * 200
                image.write(data, 0)

API Reference
=============

.. automodule:: rbd
    :members: RBD, Image, SnapIterator
