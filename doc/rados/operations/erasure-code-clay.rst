================
CLAY code plugin
================

CLAY (short for coupled-layer) codes are erasure codes designed to bring about significant savings 
in terms of network bandwidth and disk IO when a failed node/OSD/rack is being repaired. Let:

	d = number of OSDs contacted during repair

If *jerasure* is configured with *k=8* and *m=4*, losing one OSD requires 
reading from the *d=8* others to repair. And recovery of say a 1GiB needs
a download of 8 X 1GiB = 8GiB of information.

However, in the case of the *clay* plugin *d* is configurable within the limits:

	k+1 <= d <= k+m-1 

By default, the clay code plugin picks *d=k+m-1* as it provides the greatest savings in terms 
of network bandwidth and disk IO. In the case of the *clay* plugin configured with 
*k=8*, *m=4* and *d=11* when a single OSD fails, d=11 osds are contacted and 
250MiB is downloaded from each of them, resulting in a total download of 11 X 250MiB = 2.75GiB 
amount of information. More general parameters are provided below. The benefits are substantial 
when the repair is carried out for a rack that stores information on the order of 
Terabytes.

	+-------------+---------------------------+
	| plugin      | total amount of disk IO   |
	+=============+===========================+
	|jerasure,isa | k*S                       |
	+-------------+---------------------------+
	| clay        | d*S/(d-k+1) = (k+m-1)*S/m |
	+-------------+---------------------------+

where *S* is the amount of data stored on a single OSD undergoing repair. In the table above, we have 
used the largest possible value of *d* as this will result in the smallest amount of data download needed
to achieve recovery from an OSD failure.

Erasure-code profile examples
=============================

An example configuration that can be used to observe reduced bandwidth usage::

        $ ceph osd erasure-code-profile set CLAYprofile \
             plugin=clay \
             k=4 m=2 d=5 \
             crush-failure-domain=host
        $ ceph osd pool create claypool erasure CLAYprofile


Creating a clay profile
=======================

To create a new clay code profile::

        ceph osd erasure-code-profile set {name} \
             plugin=clay \
             k={data-chunks} \
             m={coding-chunks} \
             [d={helper-chunks}] \
             [scalar_mds={plugin-name}] \
             [technique={technique-name}] \
             [crush-failure-domain={bucket-type}] \
             [directory={directory}] \
             [--force]

Where:

``k={data chunks}``

:Description: Each object is split into **data-chunks** parts,
              each of which is stored on a different OSD.

:Type: Integer
:Required: Yes.
:Example: 4

``m={coding-chunks}``

:Description: Compute **coding chunks** for each object and store them
              on different OSDs. The number of coding chunks is also
              the number of OSDs that can be down without losing data.

:Type: Integer
:Required: Yes.
:Example: 2

``d={helper-chunks}``

:Description: Number of OSDs requested to send data during recovery of
              a single chunk. *d* needs to be chosen such that
              k+1 <= d <= k+m-1. Larger the *d*, the better the savings.

:Type: Integer
:Required: No.
:Default: k+m-1

``scalar_mds={jerasure|isa|shec}``

:Description: **scalar_mds** specifies the plugin that is used as a 
             building block in the layered construction. It can be 
             one of *jerasure*, *isa*, *shec*

:Type: String
:Required: No.
:Default: jerasure

``technique={technique}``

:Description: **technique** specifies the technique that will be picked
             within the 'scalar_mds' plugin specified. Supported techniques
             are 'reed_sol_van', 'reed_sol_r6_op', 'cauchy_orig', 
             'cauchy_good', 'liber8tion' for jerasure, 'reed_sol_van',
             'cauchy' for isa and 'single', 'multiple' for shec.

:Type: String
:Required: No.
:Default: reed_sol_van (for jerasure, isa), single (for shec)


``crush-root={root}``

:Description: The name of the crush bucket used for the first step of
              the CRUSH rule. For instance **step take default**.

:Type: String
:Required: No.
:Default: default


``crush-failure-domain={bucket-type}``

:Description: Ensure that no two chunks are in a bucket with the same
              failure domain. For instance, if the failure domain is
              **host** no two chunks will be stored on the same
              host. It is used to create a CRUSH rule step such as **step
              chooseleaf host**.

:Type: String
:Required: No.
:Default: host

``crush-device-class={device-class}``

:Description: Restrict placement to devices of a specific class (e.g.,
              ``ssd`` or ``hdd``), using the crush device class names
              in the CRUSH map.

:Type: String
:Required: No.
:Default:

``directory={directory}``

:Description: Set the **directory** name from which the erasure code
              plugin is loaded.

:Type: String
:Required: No.
:Default: /usr/lib/ceph/erasure-code

``--force``

:Description: Override an existing profile by the same name.

:Type: String
:Required: No.


Notion of sub-chunks
====================

The Clay code is able to save in terms of disk IO, network bandwidth as it
is a vector code and it is able to view and manipulate data within a chunk 
at a finer granularity termed as a sub-chunk. The number of sub-chunks within 
a chunk for a Clay code is given by:

	sub-chunk count = q\ :sup:`(k+m)/q`, where q=d-k+1


During repair of an OSD, the helper information requested
from an available OSD is only a fraction of a chunk. In fact, the number
of sub-chunks within a chunk that are accessed during repair is given by:

	repair sub-chunk count = sub-chunk count / q

Examples
--------

#. For a configuration with *k=4*, *m=2*, *d=5*, the sub-chunk count is
   8 and  the repair sub-chunk count is 4. Therefore, only half of a chunk is read 
   during repair.
#. When *k=8*, *m=4*, *d=11* the sub-chunk count is 64 and repair sub-chunk count
   is 16. A quarter of a chunk is read from an available OSD for repair of a failed 
   chunk.



How to choose a configuration given a workload
==============================================

Only a few sub-chunks are read of all the sub-chunks within a chunk. These sub-chunks
are not necessarily stored consecutively within a chunk. For best disk IO 
performance, it is helpful to read contiguous data. For this reason, it is suggested that
you choose stripe-size such that the sub-chunk size is sufficiently large.

For a given stripe-size (that's fixed based on a workload), choose ``k``, ``m``, ``d`` such that::

	sub-chunk size = stripe-size / (k*sub-chunk count) = 4KB, 8KB, 12KB ...

#. For large size workloads for which the stripe size is large, it is easy to choose k, m, d.
   For example consider a stripe-size of size 64MB, choosing *k=16*, *m=4* and *d=19* will
   result in a sub-chunk count of 1024 and a sub-chunk size of 4KB.
#. For small size workloads, *k=4*, *m=2* is a good configuration that provides both network
   and disk IO benefits.

Comparisons with LRC
====================

Locally Recoverable Codes (LRC) are also designed in order to save in terms of network
bandwidth, disk IO during single OSD recovery. However, the focus in LRCs is to keep the
number of OSDs contacted during repair (d) to be minimal, but this comes at the cost of storage overhead.
The *clay* code has a storage overhead m/k. In the case of an *lrc*, it stores (k+m)/d parities in
addition to the ``m`` parities resulting in a storage overhead (m+(k+m)/d)/k. Both *clay* and *lrc*
can recover from the failure of any ``m`` OSDs.

	+-----------------+----------------------------------+----------------------------------+
	| Parameters      | disk IO, storage overhead (LRC)  | disk IO, storage overhead (CLAY) |
	+=================+================+=================+==================================+
	| (k=10, m=4)     | 7 * S, 0.6 (d=7)                 | 3.25 * S, 0.4 (d=13)             |
	+-----------------+----------------------------------+----------------------------------+
	| (k=16, m=4)     | 4 * S, 0.5625 (d=4)              | 4.75 * S, 0.25 (d=19)            |
	+-----------------+----------------------------------+----------------------------------+


where ``S`` is the amount of data stored of single OSD being recovered.
