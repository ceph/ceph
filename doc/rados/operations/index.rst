====================
 Cluster Operations
====================

.. raw:: html

	<table><colgroup><col width="50%"><col width="50%"></colgroup><tbody valign="top"><tr><td><h3>High-level Operations</h3>

High-level cluster operations consist primarily of starting, stopping, and
restarting a cluster with the ``ceph`` service;  checking the cluster's health;
and, monitoring an operating cluster.

.. toctree::
	:maxdepth: 2 
	
	operating
	monitoring
	monitoring-osd-pg
	cpu-profiling
	memory-profiling
	troubleshooting
	debug

.. raw:: html 

	</td><td><h3>Data Placement</h3>

Once you have your cluster up and running, you may begin working with data
placement. Ceph supports petabyte-scale data storage clusters, with storage
pools and placement groups that distribute data across the cluster using Ceph's
CRUSH algorithm.

.. toctree::

	data-placement
	pools
	placement-groups
	crush-map



.. raw:: html

	</td></tr><tr><td><h3>Authentication and Authorization</h3>

Once you have data placement policies in place, you can begin creating users
and assigning them capabilities, such as the ability to read and write data
to one or more pools, or the cluster as a whole.  

.. toctree:: 

	Authentication Overview <auth-intro>
	Cephx Authentication <authentication>
	


.. raw:: html

	</td><td><h3>Daemon Operations</h3>

Low-level cluster operations consist of starting, stopping, and restarting a
particular daemon within a cluster; changing the settings of a particular
daemon or subsystem; and, adding a daemon to the cluster or removing a  daemon
from the cluster. The most common use cases for low-level operations include
growing or shrinking the Ceph cluster and replacing legacy or failed hardware
with new hardware.

.. toctree:: 

	add-or-rm-osds
	add-or-rm-mons
	Command Reference <control>


.. raw:: html

	</td></tr></tbody></table>

