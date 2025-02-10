===============
 CPU Profiling
===============

If you built Ceph from source and compiled Ceph for use with `oprofile`_
you can profile Ceph's CPU usage. See `Installing Oprofile`_ for details.


Initializing oprofile
=====================

``oprofile`` must be initalized the first time it is used. Locate the
``vmlinux`` image that corresponds to the kernel you are running:

.. prompt:: bash $

   ls /boot
   sudo opcontrol --init
   sudo opcontrol --setup --vmlinux={path-to-image} --separate=library --callgraph=6


Starting oprofile
=================

Run the following command to start ``oprofile``: 

.. prompt:: bash $

   opcontrol --start


Stopping oprofile
=================

Run the following command to stop ``oprofile``: 

.. prompt:: bash $

   opcontrol --stop
    
    
Retrieving oprofile Results
===========================

Run the following command to retrieve the top ``cmon`` results: 

.. prompt:: bash $

   opreport -gal ./cmon | less    
    

Run the following command to retrieve the top ``cmon`` results, with call
graphs attached: 

.. prompt:: bash $

   opreport -cal ./cmon | less    
    
.. important:: After you have reviewed the results, reset ``oprofile`` before
   running it again. The act of resetting ``oprofile`` removes data from the
   session directory.


Resetting oprofile
==================

Run the following command to reset ``oprofile``:  

.. prompt:: bash $

   sudo opcontrol --reset   
   
.. important:: Reset ``oprofile`` after analyzing data. This ensures that 
   results from prior tests do not get mixed in with the results of the current
   test. 

.. _oprofile: http://oprofile.sourceforge.net/about/
.. _Installing Oprofile: ../../../dev/cpu-profiler


