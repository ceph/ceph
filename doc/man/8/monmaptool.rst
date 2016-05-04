:orphan:

==========================================================
 monmaptool -- ceph monitor cluster map manipulation tool
==========================================================

.. program:: monmaptool

Synopsis
========

| **monmaptool** *mapfilename* [ --clobber ] [ --print ] [ --create ]
  [ --add *ip*:*port* *...* ] [ --rm *ip*:*port* *...* ]


Description
===========

**monmaptool** is a utility to create, view, and modify a monitor
cluster map for the Ceph distributed storage system. The monitor map
specifies the only fixed addresses in the Ceph distributed system.
All other daemons bind to arbitrary addresses and register themselves
with the monitors.

When creating a map with --create, a new monitor map with a new,
random UUID will be created. It should be followed by one or more
monitor addresses.

The default Ceph monitor port is 3300, as assigned by the IANA.  Prior to
the kraken 11.2.z release the default was 6789.


Options
=======

.. option:: --print

   will print a plaintext dump of the map, after any modifications are
   made.

.. option:: --clobber

   will allow monmaptool to overwrite mapfilename if changes are made.

.. option:: --create

   will create a new monitor map with a new UUID (and with it, a new,
   empty Ceph file system).

.. option:: --generate

   generate a new monmap based on the values on the command line or specified
   in the ceph configuration.  This is, in order of preference,

      #. ``--monmap filename`` to specify a monmap to load
      #. ``--mon-host 'host1,ip2'`` to specify a list of hosts or ip addresses
      #. ``[mon.foo]`` sections containing ``mon addr`` settings in the config

.. option:: --filter-initial-members

   filter the initial monmap by applying the ``mon initial members``
   setting.  Monitors not present in that list will be removed, and
   initial members not present in the map will be added with dummy
   addresses.

.. option:: --add name ip:port

   will add a monitor with the specified ip:port to the map.

.. option:: --rm name

    will remove the monitor with the specified ip:port from the map.

.. option:: --fsid uuid

    will set the fsid to the given uuid.  If not specified with --create, a random fsid will be generated.


Example
=======

To create a new map with three monitors (for a fresh Ceph file system)::

        monmaptool  --create  --add  mon.a 192.168.0.10:3300 --add mon.b 192.168.0.11:3300 \
          --add mon.c 192.168.0.12:3300 --clobber monmap

To display the contents of the map::

        monmaptool --print monmap

To replace one monitor::

        monmaptool --rm mon.a --add mon.a 192.168.0.9:3300 --clobber monmap


Availability
============

**monmaptool** is part of Ceph, a massively scalable, open-source, distributed 
storage system. Please refer to the Ceph documentation at http://ceph.com/docs 
for more information.


See also
========

:doc:`ceph <ceph>`\(8),
:doc:`crushtool <crushtool>`\(8),
