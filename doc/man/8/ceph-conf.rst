:orphan:

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
| **ceph-conf** [-s *section* ] [-r] --lookup *key*
| **ceph-conf** [-s *section* ] *key*


Description
===========

**ceph-conf** is a utility for getting information from a ceph
configuration file. As with most Ceph programs, you can specify which
Ceph configuration file to use with the ``-c`` flag.

Note that unlike other ceph tools, **ceph-conf** will *only* read from
config files (or return compiled-in default values)--it will *not*
fetch config values from the monitor cluster.  For this reason it is
recommended that **ceph-conf** only be used in legacy environments
that are strictly config-file based.  New deployments and tools should
instead rely on either querying the monitor explicitly for
configuration (e.g., ``ceph config get <daemon> <option>``) or use
daemons themselves to fetch effective config options (e.g.,
``ceph-osd -i 123 --show-config-value osd_data``).  The latter option
has the advantages of drawing from compiled-in defaults (which
occasionally vary between daemons), config files, and the monitor's
config database, providing the exact value that that daemon would be
using if it were started.

Actions
=======

**ceph-conf** performs one of the following actions:

.. option:: -L, --list-all-sections

   list all sections in the configuration file.

.. option:: -l, --list-sections *prefix*

   list the sections with the given *prefix*. For example, ``--list-sections mon``
   would list all sections beginning with ``mon``.

.. option:: --lookup *key*

   search and print the specified configuration setting. Note:  ``--lookup`` is
   the default action. If no other actions are given on the command line, we will
   default to doing a lookup.

.. option:: -h, --help

   print a summary of usage.


Options
=======

.. option:: -c *conffile*

   the Ceph configuration file.

.. option:: --filter-key *key*

   filter section list to only include sections with given *key* defined.

.. option:: --filter-key-value *key* ``=`` *value*

   filter section list to only include sections with given *key*/*value* pair.

.. option:: --name *type.id*

   the Ceph name in which the sections are searched (default 'client.admin').
   For example, if we specify ``--name osd.0``, the following sections will be
   searched: [osd.0], [osd], [global]

.. option:: --pid *pid*

   override the ``$pid`` when expanding options. For example, if an option is
   configured like ``/var/log/$name.$pid.log``, the ``$pid`` portion in its
   value will be substituted using the PID of **ceph-conf** instead of the
   PID of the process specified using the ``--name`` option.

.. option:: -r, --resolve-search

   search for the first file that exists and can be opened in the resulted
   comma delimited search list.

.. option:: -s, --section

   additional sections to search.  These additional sections will be searched
   before the sections that would normally be searched. As always, the first
   matching entry we find will be returned.


Examples
========

To find out what value osd 0 will use for the "osd data" option::

        ceph-conf -c foo.conf  --name osd.0 --lookup "osd data"

To find out what value will mds a use for the "log file" option::

        ceph-conf -c foo.conf  --name mds.a "log file"

To list all sections that begin with "osd"::

        ceph-conf -c foo.conf -l osd

To list all sections::

        ceph-conf -c foo.conf -L

To print the path of the "keyring" used by "client.0"::

       ceph-conf --name client.0 -r -l keyring


Files
=====

``/etc/ceph/$cluster.conf``, ``~/.ceph/$cluster.conf``, ``$cluster.conf``

the Ceph configuration files to use if not specified.


Availability
============

**ceph-conf** is part of Ceph, a massively scalable, open-source, distributed storage system.  Please refer
to the Ceph documentation at https://docs.ceph.com for more
information.


See also
========

:doc:`ceph <ceph>`\(8),
