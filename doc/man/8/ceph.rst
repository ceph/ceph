==========================================
 ceph -- ceph file system control utility
==========================================

.. program:: ceph

Synopsis
========

| **ceph** [ -m *monaddr* ] [ -w | *command* ... ]


Description
===========

**ceph** is a control utility for communicating with the monitor
cluster of a running Ceph distributed file system.

There are three basic modes of operation.

Interactive mode
----------------

To start in interactive mode, no arguments are necessary. Control-d or
'quit' will exit.

Watch mode
----------

Watch mode shows cluster state changes as they occur. For example::

       ceph -w

Command line mode
-----------------

Finally, to send a single instruction to the monitor cluster (and wait
for a response), the command can be specified on the command line.


Options
=======

.. option:: -i infile

   will specify an input file to be passed along as a payload with the
   command to the monitor cluster. This is only used for specific
   monitor commands.

.. option:: -o outfile

   will write any payload returned by the monitor cluster with its
   reply to outfile.  Only specific monitor commands (e.g. osd getmap)
   return a payload.

.. option:: -c ceph.conf, --conf=ceph.conf

   Use ceph.conf configuration file instead of the default
   /etc/ceph/ceph.conf to determine monitor addresses during startup.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through ceph.conf).


Examples
========

To grab a copy of the current OSD map::

       ceph -m 1.2.3.4:6789 osd getmap -o osdmap

To get a dump of placement group (PG) state::

       ceph pg dump -o pg.txt


Monitor commands
================

A more complete summary of commands understood by the monitor cluster can be found in the
wiki, at

       http://ceph.com/docs/master/rados/operations/control


Availability
============

**ceph** is part of the Ceph distributed file system. Please refer to the Ceph documentation at
http://ceph.com/docs for more information.


See also
========

:doc:`ceph <ceph>`\(8),
:doc:`mkcephfs <mkcephfs>`\(8)
