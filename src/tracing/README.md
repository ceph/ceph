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
================

## Create tracepoint definition file

Add tracepoint definitions for the provider into a `.tp` file. Documentation
on defining a tracepoint can be found in `man lttng-ust`. By convention files
are named according to the logical sub-system they correspond to (e.g.
`mutex.tp`, `pg.tp`). Place the `.tp` file into the `src/tracing` directory
and modify the automake file `src/tracing/Makefile.am` accordingly.
