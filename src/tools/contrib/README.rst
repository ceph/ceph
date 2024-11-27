contrib
==================

This directory houses scripts and other files that may be useful to Ceph
administrators.  Everything here is provided *as-is*, and may or may
not be up-to-date or functional.  Code may not be up to official standards.
Please do not assume any level of support.  Your mileage may vary.

Each file's header must include a tracker number and an author signed-off-by
line.


- balance-cpu.py. An utility to distribute the Seastar reactor threads over the
  (physical) CPU cores, according to two strategies:
  - OSD-based (default): allocates all the reactors of the same OSD in the same 
    NUMA socket,
  - NUMA socket: distributes the reactors of each OSD evenly in the NUMA sockets
    (normally two), so every OSD ends up with reactors running on both NUMA sockets.

- lscpu.py. A Python module to parse the output of  ``lscpu --json`` into a dictionary
  which is used by balance-cpu and tasksetcpu.py.

- tasksetcpu.py. an utility to print a grid showing the current CPU core allocation
  of Seastar reactors. Useful to validate that the allocation strategy is correct.

For further details, please see *BalanceCPUCrimson.md* in doc/dev/crimson.


