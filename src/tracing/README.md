Add New Provider
================

## Create tracepoint definition file

Add tracepoint definitions for the provider into a `.tp` file. Documentation
on defining a tracepoint can be found in `man lttng-ust`. By convention files
are named according to the logical sub-system they correspond to (e.g.
`mutex.tp`, `pg.tp`).

## Generate tracepoint source files

The `.tp` file is converted into source files using the `lttng-gen-tp` tool.

    lttng-gen-tp mutex.tp -o mutex.tp.h -o mutex.tp.c

## Add source files to libtracepoints.la

Modify Makefile.am to include the generated source files from the previous
step.

## Commit changes to Git

Commit both the source `.tp` file as well as the generated sources, and the
changes to Makefile.am.

Add Tracepoint to Existing Provider
===================================

New tracepoints can be added to an existing provider by updating the
corresponding `.tp` file and re-generating the source files. Make sure to
commit the updated files back into Git.
