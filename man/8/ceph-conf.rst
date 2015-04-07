==================================
 ceph-conf -- ceph conf file tool
==================================

.. program:: ceph-conf

Synopsis
========

| **ceph-conf** -c *conffile* --list-all-sections
| **ceph-conf** -c *conffile* -L
| **ceph-conf** -c *conffile* -l *prefix*
| **ceph-conf** *key* -s *section1* ...
| **ceph-conf** [-s *section* ] --lookup *key*
| **ceph-conf** [-s *section* ] *key*


Description
===========

**ceph-conf** is a utility for getting information about a ceph
configuration file. As with most Ceph programs, you can specify which
Ceph configuration file to use with the ``-c`` flag.


Actions
=======

.. TODO format this like a proper man page

**ceph-conf** will perform one of the following actions:

--list-all-sections or -L prints out a list of all the section names in the configuration
file.

--list-sections or -l prints out a list of all the sections that begin
with a given prefix. For example, --list-sections mon would list all
sections beginning with mon.

--lookup will search the configuration for a given value.  By default, the sections  that
are searched are determined by the Ceph name that we are using. The Ceph name defaults to
client.admin. It can be specified with --name.

For example, if we specify  --name  osd.0,  the  following  sections  will  be  searched:
[osd.0], [osd], [global]

You  can  specify  additional  sections to search with --section or -s.  These additional
sections will be searched before the sections that would normally be searched. As always,
the first matching entry we find will be returned.

Note:  --lookup is the default action. If no other actions are given on the command line,
we will default to doing a lookup.


Examples
========

To find out what value osd 0 will use for the "osd data" option::

        ceph-conf -c foo.conf  --name osd.0 --lookup "osd data"

To find out what value will mds a use for the "log file" option::

        ceph-conf -c foo.conf  --name mds.a "log file"

To list all sections that begin with osd::

        ceph-conf -c foo.conf -l osd

To list all sections::

        ceph-conf -c foo.conf -L


Availability
============

**ceph-conf** is part of Ceph, a massively scalable, open-source, distributed storage system.  Please refer
to the Ceph documentation at http://ceph.com/docs for more
information.


See also
========

:doc:`ceph <ceph>`\(8),
