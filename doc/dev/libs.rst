======================
 Library architecture
======================

Ceph is structured into libraries which are built and then combined together to
make executables and other libraries.

- libcommon: a collection of utilities which are available to nearly every ceph
  library and executable. In general, libcommon should not contain global
  variables, because it is intended to be linked into libraries such as
  libcephfs.so.

- libglobal: a collection of utilities focused on the needs of Ceph daemon
  programs. In here you will find pidfile management functions, signal
  handlers, and so forth.

.. todo:: document other libraries

