.. _rados-operations:

====================
 Cluster Operations
====================

.. raw:: html

	<table><colgroup><col width="50%"><col width="50%"></colgroup><tbody valign="top"><tr><td><h3>High-level Operations</h3>

High-level cluster operations consist primarily of starting, stopping, and
restarting a cluster with the ``ceph`` service;  checking the cluster's health;
and, monitoring an operating cluster.

.. toctree::
	:maxdepth: 1

	operating
	health-checks
	monitoring
	monitoring-osd-pg
	user-management
        pgcalc/index

.. raw:: html

	</td><td><h3>Data Placement</h3>

Once you have your cluster up and running, you may begin working with data
placement. Ceph supports petabyte-scale data storage clusters, with storage
pools and placement groups that distribute data across the cluster using Ceph's
CRUSH algorithm.

.. toctree::
	:maxdepth: 1

	data-placement
	pools
	erasure-code
	cache-tiering
	placement-groups
	upmap
        read-balancer
        balancer
	crush-map
	crush-map-edits
	stretch-mode
	change-mon-elections



.. raw:: html

	</td></tr><tr><td><h3>Low-level Operations</h3>

Low-level cluster operations consist of starting, stopping, and restarting a
particular daemon within a cluster; changing the settings of a particular
daemon or subsystem; and, adding a daemon to the cluster or removing a	daemon
from the cluster. The most common use cases for low-level operations include
growing or shrinking the Ceph cluster and replacing legacy or failed hardware
with new hardware.

.. toctree::
	:maxdepth: 1

	add-or-rm-osds
	add-or-rm-mons
	devices
	bluestore-migration
	Command Reference <control>



.. raw:: html

	</td><td><h3>Troubleshooting</h3>

Ceph is still on the leading edge, so you may encounter situations that require
you to evaluate your Ceph configuration and modify your logging and debugging
settings to identify and remedy issues you are encountering with your cluster.

.. toctree::
	:maxdepth: 1

	../troubleshooting/community
	../troubleshooting/troubleshooting-mon
	../troubleshooting/troubleshooting-osd
	../troubleshooting/troubleshooting-pg
	../troubleshooting/log-and-debug
	../troubleshooting/cpu-profiling
	../troubleshooting/memory-profiling




.. raw:: html

	</td></tr></tbody></table>

