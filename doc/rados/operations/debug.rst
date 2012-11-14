=======================
 Debugging and Logging
=======================

You may view Ceph log files under ``/var/log/ceph`` (the default location). 

Ceph is still on the leading edge, so you may encounter situations that require
using Ceph's debugging and logging. To activate and configure Ceph's debug
logging,  refer to `Ceph Logging and Debugging`_. For additional logging
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
