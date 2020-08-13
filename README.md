# dmclock

This repository contains C++ 11 code that implements the dmclock
distributed quality of service algorithm. See __mClock: Handling
Throughput Variability for Hypervisor IO Scheduling__ by Gulati,
Merchant, and Varman for a description of the algorithm.

## Bugs and features

There is a dmclock project under https://tracker.ceph.com/ through
which bugs can be reported and featuers requested.

## Running cmake

When running cmake, set the build type with either:

    -DCMAKE_BUILD_TYPE=Debug
    -DCMAKE_BUILD_TYPE=Release

To turn on profiling, run cmake with an additional:

    -DPROFILE=yes

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
