=======================
 Debugging and Logging
=======================

You may view Ceph log files under ``/var/log/ceph`` (the default location). 

.. important:: If you enable or increase the rate of Ceph logging, ensure
   that you have sufficient disk space on your OS disk. Verbose logging
   can generate over 1GB of data per hour. If your OS disk reaches its 
   capacity, the node will stop working.
   
.. topic:: Accelerating Log Rotation

   If your OS disk is relatively full, you can accelerate log rotation by
   modifying the Ceph log rotation file at ``/etc/logrotate.d/ceph``. Add 
   a size setting after the rotation frequency to accelerate log rotation
   (via cronjob) if your logs exceed the size setting. For example, the 
   default setting looks like this::
   
   	rotate 7
   	weekly
   	compress
   	sharedscripts
   	
   Modify it by adding a ``size`` setting. ::
   
   	rotate 7
   	weekly
   	size 500M
   	compress
   	sharedscripts

   Then, start the crontab editor for your user space. ::
   
   	crontab -e
	
   Finally, add an entry to check the ``etc/logrotate.d/ceph`` file. ::
   
   	30 * * * * /usr/sbin/logrotate /etc/logrotate.d/ceph >/dev/null 2>&1

   The preceding example checks the ``etc/logrotate.d/ceph`` file every 30 minutes.


Ceph is still on the leading edge, so you may encounter situations that require
using Ceph's debugging and logging. To activate and configure Ceph's debug
logging, refer to `Ceph Logging and Debugging`_. For additional logging
settings, refer to the `Logging and Debugging Config Reference`_. 

.. _Ceph Logging and Debugging: ../../configuration/ceph-conf#ceph-logging-and-debugging
.. _Logging and Debugging Config Reference: ../../configuration/log-and-debug-ref

You can change the logging settings at runtime so that you don't have to 
stop and restart the cluster. Refer to `Ceph Configuration - Runtime Changes`_
for additional details. 

Debugging may also require you to track down memory and threading issues. 
You can run a single daemon, a type of daemon, or the whole cluster with 
Valgrind. You should only use Valgrind when developing or debugging Ceph. 
Valgrind is computationally expensive, and will slow down your system otherwise. 
Valgrind messages are logged to ``stderr``. 

.. _Ceph Configuration - Runtime Changes: ../../configuration/ceph-conf#ceph-runtime-config
