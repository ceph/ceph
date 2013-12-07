==========================================
 crushtool -- CRUSH map manipulation tool
==========================================

.. program:: crushtool

Synopsis
========

| **crushtool** ( -d *map* | -c *map.txt* | --build --num_osds *numosds*
  *layer1* *...* | --test ) [ -o *outfile* ]


Description
===========

**crushtool** is a utility that lets you create, compile, and
decompile CRUSH map files.

CRUSH is a pseudo-random data distribution algorithm that efficiently
maps input values (typically data objects) across a heterogeneous,
hierarchically structured device map. The algorithm was originally
described in detail in the following paper (although it has evolved
some since then):

       http://www.ssrc.ucsc.edu/Papers/weil-sc06.pdf

The tool has four modes of operation.

.. option:: -c map.txt

   will compile a plaintext map.txt into a binary map file.

.. option:: -d map

   will take the compiled map and decompile it into a plaintext source
   file, suitable for editing.

.. option:: --build --num_osds {num-osds} layer1 ...

   will create a relatively generic map with the given layer
   structure. See below for examples.

.. option:: --test

   will perform a dry run of a CRUSH mapping for a range of input object 
   names, see crushtool --help for more information.
   

Running tests
=============

The test mode will use the input crush map ( as specified with **-i
map** ) and perform a dry run of CRUSH mapping or random placement (
if **--simulate** is set ). On completion, two kinds of reports can be
created. The **--show-...** options output human readable informations
on stderr. The **--output-csv** option creates CSV files that are
documented by the **--help-output** option. 

.. option:: --show-statistics

   for each rule display the mapping of each object. For instance::

       CRUSH rule 1 x 24 [11,6]

   shows that object **24** is mapped to devices **[11,6]** by rule
   **1**. At the end of the mapping details, a summary of the
   distribution is displayed. For instance::

       rule 1 (metadata) num_rep 5 result size == 5:	1024/1024

   shows that rule **1** which is named **metadata** successfully
   mapped **1024** objects to **result size == 5** devices when trying
   to map them to **num_rep 5** replicas. When it fails to provide the
   required mapping, presumably because the number of **tries** must
   be increased, a breakdown of the failures is displays. For instance::

       rule 1 (metadata) num_rep 10 result size == 8:	4/1024
       rule 1 (metadata) num_rep 10 result size == 9:	93/1024
       rule 1 (metadata) num_rep 10 result size == 10:	927/1024

   shows that although **num_rep 10** replicas were required, **4**
   out of **1024** objects ( **4/1024** ) were mapped to **result size
   == 8** devices only.

.. option:: --show-bad-mappings

   display which object failed to be mapped to the required number of
   devices. For instance::

     bad mapping rule 1 x 781 num_rep 7 result [8,10,2,11,6,9]

   shows that when rule **1** was required to map **7** devices, it
   could only map six : **[8,10,2,11,6,9]**. 

.. option:: --show-utilization

   display the expected and actual utilisation for each device, for
   each number of replicas. For instance::

     device 0: stored : 951	 expected : 853.333
     device 1: stored : 963	 expected : 853.333
     ...
   
   shows that device **0** stored **951** objects and was expected to store **853**.
   Implies **--show-statistics**.

.. option:: --show-utilization-all

   displays the same as **--show-utilization** but does not suppress
   output when the weight of a device is zero.
   Implies **--show-statistics**.

.. option:: --show-choose-tries

   display how many attempts were needed to find a device mapping.
   For instance::

      0:     95224
      1:      3745
      2:      2225
      ..

   shows that **95224** mappings succeeded without retries, **3745**
   mappings succeeded with one attempts, etc. There are as many rows
   as the value of the **--set-choose-total-tries** option.

.. option:: --output-csv

   create CSV files (in the current directory) containing information
   documented by **--help-output**. The files are named after the rule
   used when collecting the statistics. For instance, if the rule
   metadata is used, the CSV files will be::

      metadata-absolute_weights.csv
      metadata-device_utilization.csv
      ...

   The first line of the file shortly explains the column layout. For
   instance::

      metadata-absolute_weights.csv
      Device ID, Absolute Weight
      0,1
      ...

.. option:: --output-name NAME

   prepend **NAME** to the file names generated when **--output-csv**
   is specified. For instance **--output-name FOO** will create
   files::

      FOO-metadata-absolute_weights.csv
      FOO-metadata-device_utilization.csv
      ...

The **--set-...** options can be used to modify the tunables of the
input crush map. The input crush map is modified in
memory. For example::

      $ crushtool -i mymap --test --show-bad-mappings
      bad mapping rule 1 x 781 num_rep 7 result [8,10,2,11,6,9]

could be fixed by increasing the **choose-total-tries** as follows:

      $ crushtool -i mymap --test \
          --show-bad-mappings \
          --set-choose-total-tries 500

Building a map
==============

The build mode will generate relatively generic hierarchical maps. The
first argument simply specifies the number of devices (leaves) in the
CRUSH hierarchy. Each layer describes how the layer (or raw devices)
preceding it should be grouped.

Each layer consists of::

       name ( uniform | list | tree | straw ) size

The first element is the name for the elements in the layer
(e.g. "rack"). Each element's name will be append a number to the
provided name.

The second component is the type of CRUSH bucket.

The third component is the maximum size of the bucket. If the size is
0, a single bucket will be generated that includes everything in the
preceding layer.


Example
=======

Suppose we have two rows with two racks each and 20 nodes per rack. Suppose
each node contains 4 storage devices for Ceph OSD Daemons. This configuration
allows us to deploy 320 Ceph OSD Daemons. Lets assume a 42U rack with 2U nodes,
leaving an extra 2U for a rack switch.

To reflect our hierarchy of devices, nodes, racks and rows, we would execute
the following::

	crushtool -o crushmap --build --num_osds 320 node straw 4 rack straw 20 row straw 2

To adjust the default (generic) mapping rules, we can run::

       # decompile
       crushtool -d crushmap -o map.txt

       # edit
       vi map.txt

       # recompile
       crushtool -c map.txt -o crushmap


Availability
============

**crushtool** is part of the Ceph distributed storage system. Please
refer to the Ceph documentation at http://ceph.com/docs for more
information.


See also
========

:doc:`ceph <ceph>`\(8),
:doc:`osdmaptool <osdmaptool>`\(8),
