
This document describes the requirements and high-level design of the primary
balancer for Ceph.

Introduction
============

In a distributed storage system such as Ceph, there are some requirements to keep the system balanced in order to make it perform well:

#. Balance the capacity - This is a functional requirement, a system like Ceph is "as full as its fullest device". When one device is full the system can not serve write requests anymore. In order to do this we want to balance the capacity across the devices in a fair way - that each device gets capacity proportionally to its size and therefore all the devices have the same fullness level. This is a functional requirement. From performance perspective, capacity balancing creates fair share workloads on the OSDs for *write* requests.

#. Balance the workload - This is a performance requirement, we want to make sure that all the devices will receive a workload according to their performance. Assuming all the devices in a pool use the same technology and have the same bandwidth (a strong recommendation for a well configured system), and all devices in a pool have the same capacity, this means that for each pool, each device gets its fair share of primary OSDs so that the *read* requests are distributed evenly across the OSDs in the cluster. Managing workload balancing for devices with different capacities is discussed in the future enhancements section. 

Requirements
============

- For each pool, each OSD should have its fair share of PGs in which it is primary. For replicated pools, this would be the number of PGs mapped to this OSD divided by the replica size.
  - This may be improved in future releases. (see below)
- Improve the existing capacity balancer code to improve its maintainability
- Primary balancing is performed without data movement (data is moved only when balancing the capacity)
- Fix the global +/-1 balancing issue that happens since the current balancer works on a single pool at a time (this is a stretch goal for the first version)

  - Problem description: In a perfectly balanced system, for each pool, each OSD has a number of PGs that ideally would have mapped to it to create a perfect capacity balancing. This number is usually not an integer, so some OSDs get a bit more PGs mapped and some a bit less. If you have many pools and you balance on a pool-by-pool basis, it is possible that some OSDs always get the "a bit more" side. When this happens, even to a single OSD, the result is non-balanced system where one OSD is more full than the others. This may happen with the current capacity balancer. 

First release (Quincy) assumptions
----------------------------------

- Optional - In the first version the feature will be optional and by default will be disabled
- CLI only - In the first version we will probably give access to the primary balancer only by ``osdmaptool`` CLI and will not enable it in the online balancer; this way, the use of the feature is more controlled for early adopters
- No data movement

Future possible enhancements
----------------------------

- Improve the behavior for non identical OSDs in a pool
- Improve the capacity balancing behavior in extreme cases
- Add workload balancing to the online balancer
- A more futuristic feature can be to improve workload balancing based on real load statistics of the OSDs. 

High Level Design
=================

- The capacity balancing code will remain in one function ``OSDMap::calc_pg_upmaps`` (the signature might be changed)
- The workload (a.k.a primary) balancer will be implemented in a different function
- The workload balancer will do its best based on the current status of the system

  - When called on a balanced system (capacity-wise) with pools with identical devices, it will create a near optimal workload split among the OSDs
  - Calling the workload balancer on an unbalanced system (capacity-wise) may yield non optimal results, and in some cases may give worse performance than before the call

Helper functionality
--------------------

- Set a seed for random generation in ``osdmaptool`` (For regression tests)
