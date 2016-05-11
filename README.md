# dmclock
Code that implements the dmclock distributed quality of service
algorithm. See "mClock: Hanling Throughput Variability for Hypervisor
IO Scheduling" by Gulati, Merchant, and Varman.

When running cmake, set the build type with either:
    -DCMAKE_BUILD_TYPE=DEBUG
    -DCMAKE_BUILD_TYPE=RELEASE

To turn on profiling, run cmake with:
    -DPROFILE=1
