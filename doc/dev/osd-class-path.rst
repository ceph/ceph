=======================
 OSD class path issues
=======================

::

  $ rbd create rbd/test --size 100M
  2021-03-16 01:26:59.012 7fe41426f080 -1 librbd::PoolMetadata: list: failed listing metadata: (95) Operation not supported
  2021-03-16 01:26:59.012 7fe41426f080 -1 librbd::Config: apply_pool_overrides: failed to read pool config overrides: (95) Operation not supported
  2021-03-16 01:26:59.012 7fe400ff9640 -1 librbd::image::CreateRequest: 0x55d62341bb30 handle_add_image_to_directory: error adding image to directory: (95) Operation not supported
  rbd: create error: (95) Operation not supported

After adding ``--debug-ms=1``, you can see which OSD is contacted.
In the contacted OSD's log, you'll find the reason why this happens,
and usually it is because the "rados classes" can't be loaded at runtime with ``dlopen``:

::

  2021-03-16 01:26:59.013 7f6c6dff3640 10 _load_class rbd from /usr/local/lib64/rados-classes/libcls_rbd.so
  2021-03-16 01:26:59.013 7f6c6dff3640  0 _load_class could not stat class /usr/local/lib64/rados-classes/libcls_rbd.so: (2) No such file or directory
  2021-03-16 01:26:59.013 7f6c6dff3640 -1 osd.3 112 class rbd open got (2) No such file or directory
  2021-03-16 01:26:59.013 7f6c6dff3640  1 -- [...] --> [...] -- osd_op_reply(5 rbd_directory [call rbd.dir_add_image] v0'0 uv0 ondisk = -95 ((95) Operation not supported)) v8 -- 0x7f6c6800fed0 con 0x7f6cb80100c0

This means the OSD could not find ``libcls_rbd.so``.
You can customize the load path of these modules in ``ceph.conf`` with ``osd_class_dir``.
By default, this is ``$libdir/rados-classes``, so when developing, you likely have to adjust the path.

These class libraries are used for extending RADOS, see :ref:`rados-objclass-api-sdk`.
