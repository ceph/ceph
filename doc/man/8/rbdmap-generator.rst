:orphan:

============================================================================
 rbdmap-generator -- Systemd unit generator for map RBD devices at boot time
============================================================================

.. program:: rbdmap-generator

Synopsis
========

| **/lib/systemd/system-generators/rbdmap-generator**


Description
===========

:program:`rbdmap-generator` is a generator that translates /etc/ceph/rbdtab into native
systemd units early at boot and when configuration of the system manager is
reloaded with ``systemctl daemon-reload``. This will create rbdmap@.service
units as necessary. When these units are activated or deactivated, ``rbd device
map`` or ``rbd device unmap`` command is called as appropriate.

:program:`rbdmap-generator` implements :manpage:`systemd.generator(7)`

After creating the images and populating the ``/etc/ceph/rbdtab`` file, making
the images get automatically mapped and mounted at boot is just a matter of
enabling that unit (which may already be done on package installation)::

    systemctl enable rbdmap.target

For capability reason, if /etc/ceph/rbdtab file is not present, /etc/ceph/rbdmap
file will be read and parsed in a way compatible with the old ``rbdmap`` script.

Options
=======

None


Availability
============

**rbdmap-generator** is part of Ceph, a massively scalable, open-source,
distributed storage system. Please refer to the Ceph documentation at
https://docs.ceph.com for more information.


See also
========

:doc:`rbdtab <../5/rbdtab>`\(5)
:doc:`rbdmap <../5/rbdmap>`\(5)
:doc:`rbd <rbd>`\(8),
:manpage:`systemd.generator(7)`
