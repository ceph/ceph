About
-----

Ceph Windows support is currently a work in progress. For now, the main focus
is the client side, allowing Windows hosts to consume rados, rbd and cephfs
resources.

Building
--------

At the moment, mingw gcc is the only supported compiler for building ceph
components for Windows. Support for msvc and clang will be added soon.

`win32_build.sh`_ can be used for cross compiling Ceph and its dependencies.
It may be called from a Linux environment, including Windows Subsystem for
Linux. MSYS2 and CygWin may also work but those weren't tested.

.. _win32_build.sh: win32_build.sh

The script accepts the following flags:

============  ===============================  ===============================
Flag          Description                      Default value
============  ===============================  ===============================
CEPH_DIR      The Ceph source code directory.  The same as the script.
BUILD_DIR     The directory where the          $CEPH_DIR/build
              generated artifacts will be
              placed.
DEPS_DIR      The directory where the Ceph     $CEPH_DIR/build.deps
              dependencies will be built.
NUM_WORKERS   The number of workers to use     The number of vcpus
              when building Ceph.              available
NINJA_BUILD   Use Ninja instead of make.
CLEAN_BUILD   Clean the build directory.
SKIP_BUILD    Run cmake without actually
              performing the build.
============  ===============================  ===============================

Current status
--------------

The rados and rbd binaries and libs compile successfully and can be used on
Windows, successfully connecting to the cluster and consuming pools.

The libraries have to be built statically at the moment. The reason is that
there are a few circular library dependencies or unspecified dependencies,
which isn't supported when building DLLs. This mostly affects ``cls`` libraries.

A significant number of tests from the ``tests`` directory have been port,
providing adequate coverage.
