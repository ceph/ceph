===============================
 Resizing the metadata cluster
===============================

Adding new MDSes
================


Setting up standby and standby-replay MDSes
-------------------------------------------


Removing MDSes
==============

.. topic:: Status as of 2011-09:

   You can remove an MDS from the system by executing "ceph mds deactivate x",
   where x is numerical ID of the MDS to shut down.
   Beware: shrinking the number of MDSes is not well tested.
