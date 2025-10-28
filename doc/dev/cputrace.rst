========
CpuTrace
========

CpuTrace is a developer tool that measures the CPU cost of execution.
It is useful when deciding between algorithms for new code and for
validating performance enhancements.
CpuTrace measures CPU instructions, clock cycles, branch mispredictions,
cache misses and thread reschedules.

Integration into Ceph
---------------------

To enable CpuTrace, build with the ``WITH_CPUTRACE`` flag:

.. code-block:: bash

  ./do_cmake.sh -DWITH_CPUTRACE=1

Once built with CpuTrace support, you can annotate specific functions
or code regions using the provided macros and helper classes.

To enable profiling in your code, include the CpuTrace header:

.. code-block:: cpp

   #include "common/cputrace.h"

Then you can mark functions for profiling using the provided helpers.

Raw counter mode
----------------

CpuTrace is using the Linux ``perf_event_open`` syscall. You can use the tool
as a simple helper to get access to hardware perf counters.

.. code-block:: cpp

  // I am profiling my code and want to know
  // how many clock cycles and how many thread switches it takes
  HW_ctx hw = HW_ctx_empty;
  HW_init(&hw, HW_PROFILE_SWI|HW_PROFILE_CYC);
  sample_t start, end;
  HW_read(&hw, &start);
  // my code starts
  // .....
  // my code ends
  HW_read(&hw, &end);
  // task_switches = end.swi - start.swi;
  // clock_cycles  = end.cyc - start.cyc;
  HW_clean(&hw);

By inspecting ``task_switches`` and ``clock_cycles`` the developer can learn that
real clock execution time of 10ms has only 1M clock cycles, but had 2 task switches.

Aggregating samples
-------------------

A single readout of execution time is usually not enough. We need more samples
to get a more realistic measurement of actual execution cost.

.. code-block:: cpp

  // a variable to hold my measurement
  static measurement_t my_code_time;
  sample_t start, end, elapsed;
  // hw initialized somewhere else
  HW_read(&hw, &start);
  // my code starts
  // .....
  // my code ends
  HW_read(&hw, &end);
  elapsed = end - start;
  // add new sample to the whole measurement
  my_code_time.sample(elapsed);

``measurement_t``
-----------------

The ``measurement_t`` type aggregates collected samples and counts the number
of measurements performed.

It produces summary statistics that include:

- **count** : total number of measurements
- **average** : mean value across all samples
- **zero / non-zero split** : how many measurements were exactly zero
  versus greater than zero (only for context switch metrics)

These statistics provide a compact and clear view of performance measurements.

``measurement_t`` can also export results in two formats:

- **Ceph Formatter** (for structured JSON/YAML/XML output):

  .. code-block:: cpp

     ceph::Formatter* jf;
     m->dump(jf, HW_PROFILE_CYC|HW_PROFILE_INS); // Select which stats to output

- **String stream** (for plain-text logging):

  .. code-block:: cpp

     std::stringstream ss;
     m->dump_to_stringstream(ss, HW_PROFILE_CYC|HW_PROFILE_INS); // Select which stats to output
     std::cout << ss.str();

This makes it easy to either integrate measurements into Ceph’s
structured output pipeline or dump them as human-readable text for debugging.

RAII samples
------------

It is usually most convenient to use RAII to collect samples.
With RAII, measurement begins automatically when the guard object is created
and ends when it goes out of scope, so no explicit start/stop calls are required.

The hardware context (``HW_ctx``) must be initialized once before creating
guards. After initialization, the same context can be reused across multiple
measurements.

``HW_guard`` takes two arguments:

- ``HW_ctx* ctx``
  Pointer to the initialized hardware context.

- ``measurement_t* m``
  Pointer to the measurement object where results will be stored.


Example:

.. code-block:: cpp

  // variable to hold measurement results
  static measurement_t my_code_time;
  {
    HW_guard guard(&hw, &my_code_time);
    // code to be measured
    // ...
  }

Named measurements
------------------

Code regions can be measured using a `named guard`.
Each ``HW_named_guard`` automatically starts measurement at construction and stops when leaving scope.

.. code-block:: cpp

  {
    HW_named_guard("function", &hw);
    // my code starts
    // ...
    // my code ends
  }

This example records the execution time of ``function``.

The guard requires a pointer to a previously initialized ``HW_ctx``.
This context must be created and set up (e.g., during program initialization)
before guards can be used.

Named guards provide a simple and consistent way to track performance metrics.

To later access the collected measurements for a given name, use:

.. code-block:: cpp

  measurement_t* m = get_named_measurement("function");
  if (m) {
    // inspect m->sum_cyc, m->sum_ins.
    // m->dump_to_stringstream(ss, HW_PROFILE_INS|HW_PROFILE_CYC);
  }

Admin socket integration
------------------------

In addition to direct instrumentation in code, CpuTrace can also be controlled
at runtime via the admin socket interface. This allows developers to start,
stop, and inspect profiling in running Ceph daemons without rebuilding or
restarting them.

To profile a function, annotate it with the provided macros:

.. code-block:: cpp

  HWProfileFunctionF(profile, __func__,
                     HW_PROFILE_CYC  | HW_PROFILE_CMISS |
                     HW_PROFILE_INS  | HW_PROFILE_BMISS |
                     HW_PROFILE_SWI);

- ``profile`` is a local variable name for the profiler object and only needs to be unique within the profiling scope.
- ``__func__`` (or any string you pass as the name) is the unique anchor name for this profiling scope.

Each unique name creates a separate anchor. Reusing the same name in multiple places will trigger an assertion failure.

This macro automatically attaches a profiler to the function scope and
collects the specified hardware counters each time the function executes.

You can combine any of the available flags:

* ``HW_PROFILE_CYC``   – CPU cycles
* ``HW_PROFILE_CMISS`` – Cache misses
* ``HW_PROFILE_BMISS`` – Branch mispredictions
* ``HW_PROFILE_INS``   – Instructions retired
* ``HW_PROFILE_SWI``   – Context switches

Available commands:

* ``cputrace start`` – Start profiling with the configured groups/counters
* ``cputrace stop`` – Stop profiling and freeze results
* ``cputrace dump`` – Dump all collected metrics (as JSON or plain text)
* ``cputrace reset`` – Reset all captured data

Profiling counters are cumulative. `cputrace stop` pauses profiling without
resetting values. `cputrace start` resumes accumulation. Use `cputrace reset`
to clear all collected metrics.

Example usage from the command line:

.. code-block:: bash

  # Start profiling on OSD.0
  ceph tell osd.0 cputrace start

  # Stop profiling
  ceph tell osd.0 cputrace stop

  # Dump results
  ceph tell osd.0 cputrace dump

  # Reset counters
  ceph tell osd.0 cputrace reset

These commands can be repeated multiple times: developers typically
``start`` before a workload, ``stop`` afterwards, and then ``dump`` the results
to analyze them.

``cputrace dump`` supports optional arguments to filter by logger or counter,
so only a subset of metrics can be reported when needed.

``cputrace reset`` clears all data, preparing for a fresh round of profiling.

API Reference
-------------

Enums
~~~~~

.. code-block:: cpp

  enum cputrace_flags {
      HW_PROFILE_SWI   = (1ULL << 0), // Context switches
      HW_PROFILE_CYC   = (1ULL << 1), // CPU cycles
      HW_PROFILE_CMISS = (1ULL << 2), // Cache misses
      HW_PROFILE_BMISS = (1ULL << 3), // Branch mispredictions
      HW_PROFILE_INS   = (1ULL << 4), // Instructions retired
  };

The bitwise ``|`` operator may be used to combine these flags.

Data structures
~~~~~~~~~~~~~~~

``sample_t`` – holds a single hardware counter snapshot.

.. code-block:: cpp

  struct sample_t {
    uint64_t swi;   //context switches
    uint64_t cyc;   //clock cycles
    uint64_t cmiss; //cache misses
    uint64_t bmiss; //branch misses
    uint64_t ins;   //instructions
  };

``measurement_t`` – accumulates multiple samples and computes totals/averages and other 
useful metrics.

.. code-block:: cpp

  struct measurement_t {
    uint64_t call_count = 0;
    uint64_t sample_count = 0;
    uint64_t sum_swi = 0, sum_cyc = 0, sum_cmiss = 0, sum_bmiss = 0, sum_ins = 0;
    uint64_t non_zero_swi_count = 0;
    uint64_t zero_swi_count = 0;
  };


``HW_ctx`` – encapsulates perf-event file descriptors for one measurement context.

.. code-block:: cpp

  extern HW_ctx HW_ctx_empty;

Low-level API
~~~~~~~~~~~~~

- ``void HW_init(HW_ctx* ctx, cputrace_flags flags)`` – initialize perf counters.
- ``void HW_read(HW_ctx* ctx, sample_t* out)`` – read current counter values.
- ``void HW_clean(HW_ctx* ctx)`` – release perf counters.
