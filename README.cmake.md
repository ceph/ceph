Overview
========

This is a work-in-progress CMake build system.  Currently it builds
a limited set of targets, and only on Linux/Posix. The goals include
faster builds (see for yourself), cleaner builds (no libtool), and
improved portability (e.g., Windows).

Building Ceph
=============

To build out of source make an empty directory (often named **build**)
and run:

    $ cmake [path to top level ceph-local directory]

To build in-source make an empty directory called (often named
**build**) and run **cmake**:

    $ mkdir build
    $ cd build
    $ cmake ..

Once the configuring is done and the build files have been written to
the current build directory run:

    $ make

To build only certain targets use:

    $ make [target name]

To install:

    $ make install
 
Options
=======

There is an option to build the RADOS gateway that is defaulted to ON
To build without the Rados Gateway:

    $ cmake -DWITH_RADOSGW=OFF [path to top level ceph-local directory]

To build with debugging and alternate locations for a couple of
external dependencies:

    $ cmake -DLEVELDB_PREFIX="/opt/hyperleveldb" -DOFED_PREFIX="/opt/ofed" \
      -DCMAKE_INSTALL_PREFIX=/opt/accelio -DCMAKE_C_FLAGS="-O0 -g3 -gdwarf-4" \
      ..

If you often pipe `make`to `less` and would like to maintain the
diagnostic colors for errors and warnings (and if your compiler
supports it), you can invoke `cmake` with:

    $ cmake -DDIAGNOSTICS_COLOR=always [...]

Then you'll get the diagnostic colors when you execute:

    $ make | less -R

Other available values for DIAGNOSTICS_COLOR are 'auto' (default) and
'never'.


**More options will be implemented in the future.**


Targets Built
=============

* ceph-mon 
* ceph-osd 
* ceph-mds 
* cephfs 
* ceph-syn 
* rados 
* radosgw (set ON as a default)
* librados-config 
* ceph-conf 
* monmaptool 
* osdmaptool 
* crushtool 
* ceph-authtool
* init-ceph 
* mkcephfs 
* mon_store_converter
* ceph-fuse

Future work will be done to build more targets, check for libraries
and headers more thoroughly, and include tests to make this build
become more robust. CMake allows ceph to build onto many platforms
such as Windows though the shell scripts need bash/unix to run.

Developer Quick-Start
=====================

This is a CMake variant of the instructions found at
<http://docs.ceph.com/docs/jewel/dev/quick_guide>.

Development
-----------

The **run-cmake-check.sh** script will install Ceph dependencies,
compile everything in debug mode, and run a number of tests to verify
that the result behaves as expected. It will also build in-source
(i.e., create a *build* directory).

    $ ./run-cmake-check.sh

Running a development deployment
--------------------------------

Assuming you ran **run-cmake-check.sh**, you'll have a **build**
directory from which you'll run the various commands.

    $ cd build

Now you can run a development deployment:

    $ ../src/vstart -d -n -x

You can also configure **vstart.sh** to use only one monitor and one
metadata server by using the following:

    $ MON=1 MDS=1 ../src/vstart.sh -d -n -x

You can stop the development deployment with:

    $ ../src/stop.sh

[See the non-cmake instructions for additional
details.](http://docs.ceph.com/docs/jewel/dev/quick_guide/)
