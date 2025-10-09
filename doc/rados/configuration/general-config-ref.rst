==========================
 General Config Reference
==========================

.. confval:: admin_socket
   :default: /var/run/ceph/$cluster-$name.asok
.. confval:: pid_file
.. confval:: chdir
.. confval:: fatal_signal_handlers
.. describe:: max_open_files

   If set, when the :term:`Ceph Storage Cluster` starts, Ceph sets
   the max open FDs at the OS level (i.e., the max # of file
   descriptors). A suitably large value prevents Ceph Daemons from running out
   of file descriptors.

   :Type: 64-bit Integer
   :Required: No
   :Default: ``0``
