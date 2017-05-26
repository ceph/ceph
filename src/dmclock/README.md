# dmclock

This repository contains C++ 11 code that implements the dmclock
distributed quality of service algorithm. See __mClock: Handling
Throughput Variability for Hypervisor IO Scheduling__ by Gulati,
Merchant, and Varman for a description of the algorithm.

## Running cmake

When running cmake, set the build type with either:

    -DCMAKE_BUILD_TYPE=Debug
    -DCMAKE_BUILD_TYPE=Release

To turn on profiling, run cmake with an additional:

    -DPROFILE=yes

An optimization/fix to the published algorithm has been added and is
on by default. To disable this optimization/fix run cmake with:

    -DDO_NOT_DELAY_TAG_CALC=yes

## Running make

### Building the dmclock library

The `make` command builds a library libdmclock.a. That plus the header
files in the src directory allow one to use the implementation in
their code.

### Building unit tests

The `make dmclock-tests` command builds unit tests.

### Building simulations

The `make dmclock-sims` command builds two simulations -- *dmc_sim*
and *ssched_sim* -- which incorporate, respectively, the dmclock
priority queue or a very simple scheduler for comparison. Other
priority queue implementations could be added in the future.

## dmclock API

To be written....
