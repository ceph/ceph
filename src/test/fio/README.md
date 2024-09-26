FIO
===

Ceph uses the fio workload generator and benchmarking utility.
(https://github.com/axboe/fio.git)

FIO tool is automatically fetched to build/src/fio, and build if necessary.

RBD
---

The fio engine for rbd is located in the fio tree itself, so you'll need to
build it from source.

If you install the ceph libraries to a location that isn't in your
LD_LIBRARY_PATH, be sure to add it:

    export LD_LIBRARY_PATH=/path/to/install/lib

To build fio with rbd:

    ./configure --extra-cflags="-I/path/to/install/include -L/path/to/install/lib"
    make

If configure fails with "Rados Block Device engine   no", see config.log for
details and adjust the cflags as necessary.

If ceph was compiled with tcmalloc, it may be necessary to compile fio with:
    make EXTLIBS=tcmalloc
Otherwise fio might crash in malloc_usable_size().

To view the fio options specific to the rbd engine:

    ./fio --enghelp=rbd

See examples/rbd.fio for an example job file. To run:

    ./fio examples/rbd.fio

ObjectStore
-----------

This fio engine allows you to mount and use a ceph object store directly,
without having to build a ceph cluster or start any daemons.

Because the ObjectStore is not a public-facing interface, we build it inside
of the ceph tree and load libfio_ceph_objectstore.so into fio as an external
engine.

To build fio_ceph_objectstore run:
```
  ./do_cmake.sh -DWITH_FIO=ON
  cd build
  make fio_ceph_objectstore
```
This will fetch FIO to build/src/fio directory,
compile fio tool and libfio_ceph_objectstore.so.

If you install the ceph libraries to a location that isn't in your
LD_LIBRARY_PATH, be sure to add it:

    export LD_LIBRARY_PATH=/path/to/install/lib

To view the fio options specific to the objectstore engine:

    ./fio --enghelp=libfio_ceph_objectstore.so

The conf= option requires a ceph configuration file (ceph.conf). Example job
and conf files for each object store are provided in the same directory as
this README.

To run:

    ./fio /path/to/job.fio

RADOS
-----

By default FIO can be compiled with support for RADOS.
When ceph is installed in your system default compilation of FIO includes RADOS ioengine.
If you installed ceph in any other place (cmake -DCMAKE_INSTALL_PREFIX=${CEPH_INSTALL_ROOT} ..) you can build FIO following way:

    LIBS="-lrados -ltcmalloc" LDFLAGS="-L${CEPH_INSTALL_ROOT}/lib" EXTFLAGS="-I${CEPH_INSTALL_ROOT}/include" \
    rados=yes ./configure
    LIBS="-lrados -ltcmalloc" LDFLAGS="-L${CEPH_INSTALL_ROOT}/lib" EXTFLAGS="-I${CEPH_INSTALL_ROOT}/include" \
    rados=yes make

"-ltcmalloc" is necessary if ceph was compiled with tcmalloc.

Messenger
---------

This fio engine allows you to test CEPH messenger transport layer, without
any disk activities involved.

To build fio_ceph_messenger:
```
  ./do_cmake.sh -DWITH_FIO=ON
  cd build
  make fio_ceph_messenger
```
If you install the ceph libraries to a location that isn't in your
LD_LIBRARY_PATH, be sure to add it:

    export LD_LIBRARY_PATH=/path/to/install/lib

To view the fio options specific to the messenger engine:

    ./fio --enghelp=libfio_ceph_messenger.so

The ceph_conf_file= option requires a ceph configuration file (ceph.conf),
see ceph-messenger.conf and ceph-messenger.fio for details.

To run:

    ./fio ./ceph-messenger.fio
