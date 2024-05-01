============
Purge Queue
============

MDS maintains a data structure known as **Purge Queue** which is responsible
for managing and executing the parallel deletion of files.
There is a purge queue for every MDS rank. Purge queues consist of purge items
which contain nominal information from the inodes such as size and the layout
(i.e. all other un-needed metadata information is discarded making it
independent of all metadata structures).

Deletion process
================

When a client requests deletion of a directory (say ``rm -rf``):

- MDS queues the files and subdirectories (purge items) from pq (purge queue)
  journal in the purge queue.
- Processes the deletion of inodes in background in small and manageable
  chunks.
- MDS instructs underlying OSDs to clean up the associated objects in data
  pool.
- Updates the journal.

.. note:: If the users delete the files more quickly than the
          purge queue can process then the data pool usage might increase
          substantially over time. In extreme scenarios, the purge queue
          backlog can become so huge that it can slacken the capacity reclaim
          and the linux ``du`` command for CephFS might report inconsistent
          data compared to the CephFS Data pool.

There are a few tunable configs that MDS uses internally to throttle purge
queue processing:

.. confval:: filer_max_purge_ops
.. confval:: mds_max_purge_files
.. confval:: mds_max_purge_ops
.. confval:: mds_max_purge_ops_per_pg

Generally, the defaults are adequate for most clusters. However, in
case of pretty huge clusters, if the need arises like ``pq_item_in_journal``
(counter of things pending deletion) reaching gigantic figure then the configs
can be tuned to 4-5 times of the default value as a starting point and
further increments are subject to more requirements.

Start from the most trivial config ``filer_max_purge_ops``, which should help
reclaim the space more quickly::

    $ ceph config set mds filer_max_purge_ops 40

Incrementing ``filer_max_purge_ops`` should just work for most
clusters but if it doesn't then move ahead with tuning other configs::

    $ ceph config set mds mds_max_purge_files 256
    $ ceph config set mds mds_max_purge_ops 32768
    $ ceph config set mds mds_max_purge_ops_per_pg 2

.. note:: Setting these values won't immediately break anything except
          inasmuch as they control how many delete ops we issue to the
          underlying RADOS cluster, but might eat up some cluster performance
          if the values set are staggeringly high.

.. note:: The purge queue is not an auto-tuning system in terms of its work
          limits as compared to what is going on. So it is advised to make
          a conscious decision while tuning the configs based on the cluster
          size and workload.

Examining purge queue perf counters
===================================

When analysing MDS perf dumps, the purge queue statistics look like::

    "purge_queue": {
        "pq_executing_ops": 56655,
        "pq_executing_ops_high_water": 65350,
        "pq_executing": 1,
        "pq_executing_high_water": 3,
        "pq_executed": 25,
        "pq_item_in_journal": 6567004
    }

Let us understand what each of these means:

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - Name
     - Description
   * - pq_executing_ops
     - Purge queue operations in flight
   * - pq_executing_ops_high_water
     - Maximum number of executing purge operations recorded
   * - pq_executing 
     - Purge queue files being deleted
   * - pq_executing_high_water 
     - Maximum number of executing file purges
   * - pq_executed 
     - Purge queue files deleted
   * - pq_item_in_journal
     - Purge items (files) left in journal

.. note:: ``pq_executing`` and ``pq_executing_ops`` might look similar but
          there is a small nuance. ``pq_executing`` tracks number of files
          in the purge queue while ``pq_executing_ops`` is the count of RADOS
          objects from all the files in purge queue.
