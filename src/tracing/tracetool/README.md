# Tracetool

#### Disclaimer
This approach and some of its code are taken from the QEMU project.

## Tracepoints and subsystems

All events are declared directly and used in the source code. For now, only ``src/os/bluestore/BlueStore.cc`` can be instrumented, as such:

```
[disable] <name>(<type1> <arg1>[, <type2> <arg2>] ...) "<format-string>" <log level>
trace_<tracepoint name>(<loglevel>, <subsystem>, [<type1>, <name arg1>, <value arg1>[, <type2>, <name arg2>, <value arg2>, ...], "<format-string>");
```
For example:
```
  trace_write(10, bluestore,
              ghobject_t, oid, o->oid,
              uint64_t, offset, getOffset(),
              size_t, length, l,
              "Do write on ojbect %s, at offset = %llu with and a length of %d");
```
Where ``write`` is the tracepoint name, ``oid``, ``offset`` and ``length`` are the payload fields and ``o->oid``, ``getOffset()`` and ``l`` are their respective values. ``"Do write on ojbect %s, at offset = %llu with and a length of %d"`` is how it will be formatted (for certain backends only), ``10`` is its loglevel, and ``bluestore`` is its subsystem. The difference

#### Types
You can use objects of any class in the events files on the condition that the class implements the ``operator std::string()`` method. For example, you can have:
```
trace_read(1, bluestore, byte_u_t length, "Reading length %s")
```
As long as the ``byte_u_t`` class implements ``operator std::string()``.

#### How do I use the tracepoints?
- You need to ``#include <your subsys name>_impl.h`` in the source file where you want to invoke its tracepoints
- You need to ``#include <your subsys name>.h`` in ``src/log/Log.cc``.

## Parsetool

The parsetool first parses the source code to find tracepoints and generate code that can be built with Ceph. The parseetool is invoked as follows:
```
python src/tracing/tracetool/parsetool.py -b <backend> -t <type> --outdir <Ceph build directory>
```
Where ``backend`` is either ``dout`` or ``lttng``, and ``type`` is either ``tp`` (tracepoint file), ``c`` (C source file) or ``h`` (header file).

### How do I run this?
You don't have to, just run ``do_cmake`` with ``-DWITH_LTTNG_LOGGING=ON`` to use the ``LTTng`` backend, otherwise leave it unset to use the ``dout`` backend.

### What are the generated files?
Three files are generated for the ``lttng`` backend, and one file is generated for the ``dout`` backend. Below are some details:

## Tracing

- Have LTTng installed. Make sure that:
    - packages `lttng-tools`, `lttng-ust`, `lttng-modules` (), `babeltrace`, as well as their `-devel` counterparts when available, are installed
    - the `lttng-sessiond` daemon is running (`$> sudo lttng-sessiond -d` otherwise)
    - you belong to the `tracing` group, otherwise you'll have to run the following commands with `sudo`
- Start LTTng tracing:
    - If you have Ceph (or fio with the objectstore backend) already running, you can list the tracepoints using ``$> lttng list -u``
    - Create a tracing session: ``$> lttng create -o .``. The trace will be written in the local directory and will be called ``ust/``
    - Enable tracepoints: ``$> lttng enable-event -u -a`` to enable all userspace tracepoints. If you wish to enable only some subsystems, use ``$> lttng enable-event -u bluestore:*``. Replace ``bluestore`` with the subsystem you want to enable. You can run this command multiple times for each subsystem.
    - Start tracing: ``$> lttng start``
    - Do something with Ceph (or with fio using the objectstore backend)
    - Stop tracing: ``$> lttng stop``
    - Read the trace: ``$> lttng view`` or ``$> babeltrace <path to trace>`` (eg ``babeltrace ust/``)
    - Destroy the tracing session: ``$> lttng destroy``

## Misc
### Babeltrace
You can use Babeltrace's python bindings to parse a trace. This can be useful to write scripts that analyze a trace and extract metrics out of it.

