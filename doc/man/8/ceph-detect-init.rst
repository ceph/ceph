:orphan:

============================================================
 ceph-detect-init -- display the init system Ceph should use
============================================================

.. program:: ceph-detect-init

Synopsis
========

| **ceph-detect-init** [--verbose] [--use-rhceph] [--default *init*]

Description
===========

:program:`ceph-detect-init` is a utility that prints the init system
Ceph uses. It can be one of ``sysvinit``, ``upstart`` or ``systemd``.
The init system Ceph uses may not be the default init system of the
host operating system. For instance on Debian Jessie, Ceph may use
``sysvinit`` although ``systemd`` is the default.

If the init system of the host operating system is unknown, return on
error, unless :option:`--default` is specified.

Options
=======

.. option:: --use-rhceph

   When an operating system identifies itself as Red Hat, it is
   treated as if it was CentOS. With :option:`--use-rhceph` it is
   treated as RHEL instead.

.. option:: --default INIT

   If the init system of the host operating system is unkown, return
   the value of *INIT* instead of failing with an error.

.. option:: --verbose

   Display additional information for debugging.

Availability
============

:program:`ceph-detect-init` is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.

See also
========

:doc:`ceph-disk <ceph-disk>`\(8),
:doc:`ceph-deploy <ceph-deploy>`\(8)
