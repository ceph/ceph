======================
CephFS MDS Partitioner
======================

CephFS provides mds_partitioner as mgr module for optimizing mds performance. Unlike the existing MDS Balancer,
it employes ceph.dir.pin and ceph.dir.bal.mask to minimize metadata migration overhead.
In addition, it considers the workload characteristics of each subtree and attempts to optimize
performance by distributing it across all mds ranks. It also provides two modes: manual mode, which is directly controlled by the administrator,
and scheduler mode, which runs automatically and periodically.


Recommendations
===============

* When performance deteriorates due to metadata migration
* Difficult to manage due to the large number of mds ranks and subtrees (or subvolumes)


mds_partitioner structure
=========================

This module consists of a command line interface, workload policy manager, analyzer, mover, and scheduler.

* CLI: this module provides various commands to manipulate mds_partitioner.
* workload policy manager: this module manages various workload policies.
* analyzer: this module analyzes the workload for the subtrees of the MDS and determines how to distribute it for a given policy. 
* mover: this module distributes the subtree into several mds ranks based on the policy determined through analyzer.
* scheduler: this module automatically performs analyzer and mover periodically.


Quick Start
===========

Initialization of mds_partitioner
---------------------------------

mds_partitioner can operate individually for each fs with::

 ceph mds_partitioner enable $fs_name

workload policy can be created with::

 ceph mds_partitioner workload_policy create $fs_name $policy_name

To add subtrees to the workload policy, run the following command::

 ceph mds_partitioner workload_policy dir_path add $fs_name $policy_name $dir_path

To activate workload policy among several policies, run the following command::

  ceph mds_partitioner workload_policy activate $fs_name $policy_name

mds_partitioner provides two modes for distributing subtrees across MDS ranks: manual and scheduler.
The manual mode requires executing manually analyzer for workload analysis and mover for subtree distribution.
The scheduler mode run analyzer and mover automatically and periodically.

Manual mode
-----------

To analyze workloads, run the following command::

  ceph mds_partitioner analyzer start $fs_name $policy_name $duration $interval

To check the status of the analyzer, run the following command::

  ceph mds_partitioner analyzer status $fs_name

When the workload analysis is complete, the analyzer can be stopped through:: 

  ceph mds_partitioner analyzer stop $fs_name

mover can be invoked to distribute subtrees::

  ceph mds_partitioner mover start $fs_name $policy

To check the status of the mover, run the following command::

  ceph mds_partitioner mover status $fs_name

When migrating subtrees is done, mover can be stopped through::
  
 ceph mds_partitioner mover stop $fs_name


Scheduler mode
--------------

If using manual mode is cumbersome or the policy has been validated by the manual mode, it can be automated through the scheduler.
See also :ref:`Scheduler<cephfs_mds_partitioner_scheduler>` for examples on scheduler.

Clean up
--------

To deactivate the workload policy, run the following command::

  ceph mds_partitioner workload_policy deactivate $fs_name $policy_name

To disable the mds_partitioner, run the following command::

  ceph mds_partitioner disable $fs_name

Command Line Interface
======================

mds_partitioner can be configured separately for each file system. 

Enable/Disable mds_partitioner
------------------------------

To enable/disable mds_partitioner, run the following command::

  ceph mds_partitioner enable $fs_name
  ceph mds_partitioner disable $fs_name

To the list of file systems using mds_partitioner, run the following command::

  ceph mds_partitioner list

To see the status of the partitioner, run the following command::

  ceph mds_partitioner status $fs_name

Workload policy management
--------------------------

Various workload policies can be defined for each file system with::

  ceph mds_partitioner workload_policy create $fs_name $policy_name

To manipulate subtrees to the workload policy, run the following command::

  ceph mds_partitioner workload_policy dir_path add $fs_name $policy_name $dir_path
  ceph mds_partitioner workload_policy dir_path list $fs_name $policy_name
  ceph mds_partitioner workload_policy dir_path rm $fs_name $policy_name $dir_path

mds_partitioner provides three modes for partitioning: pin, pin_bal_rank_mask, and pin_dir_bal_mask.

* pin mode uses ceph.dir.pin to distribute subtrees to several mds ranks.
* pin_bal_rank_mask mode distributes subtrees using ceph.dir.pin and bal_rank_mask of mdsmap.
* pin_dir_bal_mask mode distributes subtrees using ceph.dir.pin and ceph.dir.bal.mask.

For example::

  ceph mds_partitioner workooad_policy set partitioner_mode $fs_name $policy_name $partitioner_mode

To see the detail of the defined policy, run the following command::

  ceph mds_partitioner workload_policy show $fs_name $policy_name

To activate workload policy among several policies, run the following command::

  ceph mds_partitioner workload_policy activate $fs_name $policy_name

.. note:: Only activated policy can be used in analyzer, mover, and scheduler.

Use the following command to permanently store the workload policy.
Since the policy is stored in the kV store, it can be recovered even if the module is restarted.

Example::

  ceph mds_partitioner workload_policy save $fs_name $policy_name

To deactivate the workload policy, run the following command::

  ceph mds_partitioner workload_policy deactivate $fs_name $policy_name

To check the list of policies, run the following command::

  ceph mds_partitioner workload_policy list $fs_name

To remove the policy, run the following command::

  ceph mds_partitioner workload_policy remove $fs_name $policy_name

To remove all policies, run the following command::

  ceph mds_partitioner workload_policy remove-all $fs_name

History management
------------------

To check histories of mds_partitioner, run the following command::

  ceph mds_partitioner workload_policy history list $fs_name

To see the detail of the history, run the following command::

  ceph mds_partitioner workload_policy history show $fs_name $history_id

To remove the history, run the following command::

  ceph mds_partitioner workload_poilcy history rm $fs_name $history_id

To remove all histories, run the following command::

  ceph mds_partitioner workload_policy history rm-all $fs_name

To save history, run the following command::

  ceph mds_partitioner workload_policy history freeze $fs_name

Analyzer
--------

To analyze workloads, run the following command::

  ceph mds_partitioner analyzer start $fs_name $policy_name $duration $interval

To check the status of the analyzer, run the following command::

  ceph mds_partitioner analyzer status $fs_name

When the workload analysis is complete, the analyzer can be stopped through:: 

  ceph mds_partitioner analyzer stop $fs_name

Mover
-----

mover can be invoked to distribute subtrees::

  ceph mds_partitioner mover start $fs_name $policy

To check the status of the mover, run the following command::

  ceph mds_partitioner mover status $fs_name

When migrating subtrees is done, mover can be stopped through::
  
 ceph mds_partitioner mover stop $fs_name


.. _cephfs_mds_partitioner_scheduler:

Scheduler
---------

To invoke scheduler, run the following command. analyzer_period refers to the time
the analyzer analyzes mds workload and is in seconds. If this value is 3600 seconds,
the analyzer ends analysis after 1 hour. scheduler_period refers to the scheduler
execution period. If this value is 7200 seconds, analyzer and mover will be run every 2 hours.::

  ceph mds_partitioner scheduler start $fs_name $policy_name $analyzer_period $scheduler_period

To check the status of the scheduler, run the following command::

  ceph mds_partitioner scheduler status $fs_name

To stop the scheduler, run the following command::

  ceph mds_partitioner scheduler stop $fs_name


Global configuration
====================

mds_partitioner divides workload into heavy and moderate workload based on subtree's rentries.
In case of heavy workload, balancer is performed within limited ranks by setting mdsmap's bal_rank_mask or ceph.dir.bal.mask.
On the other hand, in case of moderate workload, it is assigned to a specific rank only through ceph.dir.pin.
To perform heavy_rentries_threshold, execute the following command::

  ceph config set mgr mgr/mds_partitioner/heavy_rentries_threshold $rentries
  # ceph config set mgr mgr/mds_partitioner/heavy_rentries_threshold 10000000

mds_partitioner tries to distribute the entire workload evenly across multiple mds ranks.
mds_partitioner calculates the average mds workload and moves subtrees to evenly balance the workload.
The workload of each subtree can be calculated as follows::

  workload per subtree = ( rentries / total rentreis + working set size / total working set size  + average latency / total average latency ) * 1000

The workload calculation policy can be changed based on key_metrics such as workload, rentries, avg_lat, and wss.
workload is an option that takes into account rentries, working set size, and average latency above.  
For the rest, rentries, avg_lat, and wss consider only each attribute of the subtree and match the distribution to the entire mds ranks.

Example::

  ceph config set mgr mgr/mds_partitioner/key_metric $key_metric


mds_partitioner tries to equalize the workload of mds ranks. However, frequent subtree movements may occur due to slight differences in workload.
This can be prevented through key_metric_delta_threshold::

  ceph config set mgr mgr/mds_partitioner/key_metric_delta_threshold
