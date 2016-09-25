Installation
============

The LTTng libraries that ship with Ubuntu 12.04 have been very buggy, and the
generated header files using `lttng-gen-tp` have needed to be fixed just to
compile in the Ceph tree. The packages available in Ubuntu 14.04 seem to work
alright, and for older versions please install LTTng from the LTTng PPA.

    https://launchpad.net/~lttng/+archive/ppa

Then install as normal

    apt-get install lttng-tools liblttng-ust-dev

Add/Update Provider
===================

## Create tracepoint definition file

Add tracepoint definitions for the provider into a `.tp` file. Documentation
on defining a tracepoint can be found in `man lttng-ust`. By convention files
are named according to the logical sub-system they correspond to (e.g.
`mutex.tp`, `pg.tp`). And add a C source file to be compiled into the tracepoint
provider shared object, in which `TRACEPOINT_DEFINE` should be defined. See
[LTTng document](http://lttng.org/docs/#doc-dynamic-linking) for details.
Place the `.tp` and the `.c` files into the `src/tracing` directory
and modify the CMake file `src/tracing/CMakeLists.txt` accordingly.


