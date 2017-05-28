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

Function Instrumentation
========================
Ceph supports instrumentation using GCC's `-finstrument-functions` flag.
Supported CMake flags are:

*   `-DWITH_OSD_INSTRUMENT_FUNCTIONS=ON`: instrument OSD code

Note that this instrumentation adds an extra function call on each function entry
and exit of Ceph code. This option is currently only supported with GCC. Using it
with Clang has no effect.

The only function tracing implementation at the moment is done using LTTng UST.
In order to use it, Ceph needs to be configured with LTTng using `-DWITH_LTTNG=ON`.
[TraceCompass](http://www.tracecompass.org) can be used to generate flame
charts/graphs and other metrics.

It is also possible to use [libbabeltrace](http://diamon.org/babeltrace/#docs)
to write custom analysis. The entry and exit tracepoints are called
`lttng_ust_cyg_profile:func_enter` and `lttng_ust_cyg_profile:func_exit`
respectively. The payload variable `addr` holds the address of the function
called and the payload variable `call_site` holds the address where it is called.
`nm` can be used to resolve function addresses (`addr` to function name).

