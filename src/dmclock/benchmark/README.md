# dmclock benchmarking

**IMPORTANT**: now that K_WAY_HEAP is no longer allowed to have the
value 1, the shell and Python scripts that generate the PDFs no longer
work exactly correctly. Some effort to debug is necessary.

This directory contains scripts to evaluate effects of different
branching-factors (k=1 to k=11) in the IndirectIntrusiveHeap
data-structure. IndirectIntrusiveHeap is now a k-way heap, so finding
an ideal value for k (i.e., k=2 or k=3) for a particular work-load is
important. Also, it is well-documented that the right choice of
k-value improves the caching behaviour [Syed -- citation needed
here]. As a result, the overall performance of an application using
k-way heap increases significantly [Syed -- citation needed here].

A rule of thumb is the following:
	if number of elements are <= 6, use k=1
	otherwise, use k=3.

## Prerequisites

requires python 2.7, gnuplot, and awk.
  
## Running benchmark

./run.sh [name_of_the_output] [k_way] [repeat] # [Syed -- last two command line args do not work]

The "run.sh" script looks for config files in the "configs" directory,
and the final output is generated as
"name_of_the_output.pdf". Internally, "run.sh" calls other scripts
such as data_gen.sh, data_parser.py, and plot_gen.sh.

## Modifying parameters

To modify k-value and/or the amount of times each simulation is
repeated, modify the following two variables in "run.sh" file:

    k_way=[your_value]
    repeat=[your_value]

For example, k_way=3 means, the benchmark will compare simulations
using 1-way, 2-way, and 3-way heaps.
