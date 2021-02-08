:orphan:

==========================================================
 monmaptool -- ceph monitor cluster map manipulation tool
==========================================================

.. program:: monmaptool

Synopsis
========

| **monmaptool** <action> [options] *mapfilename*


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

The default Ceph monitor port for messenger protocol v1 is 6789, and
3300 for protocol v2.

Multiple actions can be performed per invocation.


Options
=======

.. option:: --print

   print a plaintext dump of the map, after any modifications are
   made.

.. option:: --feature-list [plain|parseable]

   list the enabled features as well as the available ones.

   By default, a human readable output is produced.

.. option:: --create

   create a new monitor map with a new UUID (and with it, a new,
   empty Ceph cluster).

.. option:: --clobber

   allow monmaptool to create a new mapfilename in place of an existing map.

   Only useful when *--create* is used.

.. option:: --generate

   generate a new monmap based on the values on the command line or specified
   in the ceph configuration.  This is, in order of preference,

      #. ``--monmap filename`` to specify a monmap to load
      #. ``--mon-host 'host1,ip2'`` to specify a list of hosts or ip addresses
      #. ``[mon.foo]`` sections containing ``mon addr`` settings in the config. Note that this method is not recommended and support will be removed in a future release.

.. option:: --filter-initial-members

   filter the initial monmap by applying the ``mon initial members``
   setting.  Monitors not present in that list will be removed, and
   initial members not present in the map will be added with dummy
   addresses.

.. option:: --add name ip[:port]

   add a monitor with the specified ip:port to the map.

   If the *nautilus* feature is set, and the port is not, the monitor
   will be added for both messenger protocols.

.. option:: --addv name [protocol:ip:port[,...]]

   add a monitor with the specified version:ip:port to the map.

.. option:: --rm name

   remove the monitor with the specified name from the map.

.. option:: --fsid uuid

   set the fsid to the given uuid.  If not specified with *--create*, a random fsid will be generated.

.. option:: --feature-set value [--optional|--persistent]

   enable a feature.

.. option:: --feature-unset value [--optional|--persistent]

   disable a feature.

.. option:: --enable-all-features

   enable all supported features.

.. option:: --set-min-mon-release release

   set the min_mon_release.

Example
=======

To create a new map with three monitors (for a fresh Ceph cluster)::

        monmaptool --create --add nodeA 192.168.0.10 --add nodeB 192.168.0.11 \
          --add nodeC 192.168.0.12 --enable-all-features --clobber monmap

To display the contents of the map::

        monmaptool --print monmap

To replace one monitor::

        monmaptool --rm nodeA monmap
        monmaptool --add nodeA 192.168.0.9 monmap


Availability
============

**monmaptool** is part of Ceph, a massively scalable, open-source, distributed 
storage system. Please refer to the Ceph documentation at http://ceph.com/docs 
for more information.


See also
========

:doc:`ceph <ceph>`\(8),
:doc:`crushtool <crushtool>`\(8),
