===============
 CPU Profiling
===============

If you built Ceph from source and compiled Ceph for use with `oprofile`_
you can profile Ceph's CPU usage. See `Installing Oprofile`_ for details.


Initializing oprofile
=====================

The first time you use ``oprofile`` you need to initialize it. Locate the
``vmlinux`` image corresponding to the kernel you are now running. :: 

	ls /boot
	sudo opcontrol --init
	sudo opcontrol --setup --vmlinux={path-to-image} --separate=library --callgraph=6


Starting oprofile
=================

To start ``oprofile`` execute the following command:: 

	opcontrol --start

Once you start ``oprofile``, you may run some tests with Ceph. 


Stopping oprofile
=================

To stop ``oprofile`` execute the following command:: 

	opcontrol --stop
	
	
Retrieving oprofile Results
===========================

To retrieve the top ``cmon`` results, execute the following command:: 

	opreport -gal ./cmon | less	
	

To retrieve the top ``cmon`` results with call graphs attached, execute the
following command:: 

	opreport -cal ./cmon | less	
	
.. important:: After reviewing results, you should reset ``oprofile`` before
   running it again. Resetting ``oprofile`` removes data from the session 
   directory.


Resetting oprofile
==================

To reset ``oprofile``, execute the following command:: 

	sudo opcontrol --reset   
   
.. important:: You should reset ``oprofile`` after analyzing data so that 
   you do not commingle results from different tests.

.. _oprofile: http://oprofile.sourceforge.net/about/
.. _Installing Oprofile: ../../../dev/cpu-profiler
