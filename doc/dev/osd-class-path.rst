=======================
 OSD class path issues
=======================

::

  2011-12-05 17:41:00.994075 7ffe8b5c3760 librbd: failed to assign a block name for image
  create error: error 5: Input/output error

This usually happens because your OSDs can't find ``cls_rbd.so``. They
search for it in ``osd_class_dir``, which may not be set correctly by
default (http://tracker.newdream.net/issues/1722).

Most likely it's looking in ``/usr/lib/rados-classes`` instead of
``/usr/lib64/rados-classes`` - change ``osd_class_dir`` in your
``ceph.conf`` and restart the OSDs to fix it.
