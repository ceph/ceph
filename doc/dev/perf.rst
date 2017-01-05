Using perf
==========

Top::

  sudo perf top -p `pidof ceph-osd`

To capture some data with call graphs::

  sudo perf record -p `pidof ceph-osd` -F 99 --call-graph dwarf -- sleep 60

To view by caller (where you can see what each top function calls)::

  sudo perf report --call-graph caller

To view by callee (where you can see who calls each top function)::  

  sudo perf report --call-graph callee

:note: If the caller/callee views look the same you may be
       suffering from a kernel bug; upgrade to 4.8 or later.

Flamegraphs
-----------

First, get things set up::

  cd ~/src
  git clone https://github.com/brendangregg/FlameGraph

Run ceph, then record some perf data::

  sudo perf record -p `pidof ceph-osd` -F 99 --call-graph dwarf -- sleep 60

Then generate the flamegraph::

  sudo perf script | ~/src/FlameGraph/stackcollapse-perf.pl > /tmp/folded
  ~/src/FlameGraph/flamegraph.pl /tmp/folded > /tmp/perf.svg
  firefox /tmp/perf.svg
