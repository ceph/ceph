Overview
========
This is a work in progress Cmake build system.  Currently it builds alimited set of targets,
and only on Linux/posix.  The goals include faster builds (see for yourself), cleaner
builds (no libtool), and improved portability (e.g., Windows).

Building Ceph
=============
To build out of source make an empty directory called "build" and run:
$ cmake [path to top level ceph-local directory]

To build in source make an empty directory called "build" and run:
$ cmake ..

Once the Configuring is done and the Build files have been written to the current
build directory run:
$ make

To build only certain targets type in:
$ make [target name]

To install, once all the targets are built run:
$ make install
 
Options
=======
There is an option to build the Rados Gateway that is defaulted to ON
To build without the Rados Gateway:
$ cmake [path to top level ceph-local directory] -DWITH_RADOSGW=OFF

To build with debugging and alternate locations for (a couple of)
external dependencies:
$ cmake -DLEVELDB_PREFIX="/opt/hyperleveldb" -DOFED_PREFIX="/opt/ofed" \
    -DCMAKE_INSTALL_PREFIX=/opt/accelio -DCMAKE_C_FLAGS="-O0 -g3 -gdwarf-4" \
    ..

With future development efforts more options will be implemented

Targets Built
==============
ceph-mon 
ceph-osd 
ceph-mds 
cephfs 
ceph-syn 
rados 
radosgw (set ON as a default)
librados-config 
ceph-conf 
monmaptool 
osdmaptool 
crushtool 
ceph-authtool
init-ceph 
mkcephfs 
mon_store_converter
ceph-fuse

Future work will be done to build more targets, check for libraries and headers more thoroughly
and include tests to make this build become more robust. CMake allows ceph to build onto many 
platforms such as Windows though the shell scripts need bash/unix to run.

