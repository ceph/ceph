Erasure Coded pool
==================

Purpose
-------

Erasure coded pools requires less storage space compared to replicated
pools. It has higher computation requirements and only supports a
subset of the operations allowed on an object (no partial write for
instance).

Use cases
---------

Cold storage
~~~~~~~~~~~~

An erasure coded pool is created to store a large number of 1GB
objects (imaging, genomics, etc.) and 10% of them are read per
month. New objects are added every day and the objects are not
modified after being written. On average there is one write for 10,000
reads.

A replicated pool is created and set as a cache tier for the
replicated pool. An agent demotes objects (i.e. moves them from the
replicated pool to the erasure coded pool) if they have not been
accessed in a week.

The erasure coded pool crush ruleset targets hardware designed for
cold storage with high latency and slow access time. The replicated
pool crush ruleset targets faster hardware to provide better response
times.

Cheap multidatacenter storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ten datacenters are connected with dedicated network links. Each
datacenters contain the same amount of storage, have no power supply
backup and no air cooling system.

An erasure coded pool is created with a crush map ruleset that will
ensure no data loss if at most three datacenters fail
simultaneously. The overhead is 50% with erasure code configured to
split data in six (k=6) and create three coding chunks (m=3). With
replication the overhead would be 400% (four replicas).

Interface
---------

Set up an erasure coded pool::

 ceph osd create ecpool 12 12 erasure

Set up an erasure coded pool and the associated crush ruleset::

 ceph osd crush rule create-erasure ecruleset
 ceph osd pool create ecpool 12 12 erasure \
   crush_ruleset=ecruleset

Set the ruleset failure domain to osd instead of the host which is the default::

 ceph osd pool create ecpool 12 12 erasure \
   erasure-code-ruleset-failure-domain=osd

Control the parameters of the erasure code plugin::

 ceph osd pool create ecpool 12 12 erasure \
   erasure-code-k=2 erasure-code-m=1

Choose an alternate erasure code plugin::

 ceph osd create ecpool 12 12 erasure \
   erasure-code-plugin=example

