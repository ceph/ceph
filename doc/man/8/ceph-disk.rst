:orphan:

===================================================================
 ceph-disk -- Ceph disk utility for OSD
===================================================================

.. program:: ceph-disk

Synopsis
========

| **ceph-disk** [-h] [-v] [--log-stdout] [--prepend-to-path PATH]
                [--statedir PATH] [--sysconfdir PATH]
                [--setuser USER] [--setgroup GROUP]
                ...

| optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         be more verbose
  --log-stdout          log to stdout
  --prepend-to-path PATH
                        prepend PATH to $PATH for backward compatibility (default /usr/bin)
  --statedir PATH       directory in which ceph state is preserved (default /var/lib/ceph)
  --sysconfdir PATH     directory in which ceph configuration files are found (default /etc/ceph)
  --setuser USER        use the given user for subprocesses, rather than ceph or root
  --setgroup GROUP      use the given group for subprocesses, rather than ceph or root

| subcommands:

    prepare              Prepare a directory or disk for a Ceph OSD
    activate             Activate a Ceph OSD
    activate-lockbox     Activate a Ceph lockbox
    activate-block       Activate an OSD via its block device
    activate-journal     Activate an OSD via its journal device
    activate-all         Activate all tagged OSD partitions
    list                 List disks, partitions, and Ceph OSDs
    suppress-activate    Suppress activate on a device (prefix)
    unsuppress-activate  Stop suppressing activate on a device (prefix)
    deactivate           Deactivate a Ceph OSD
    destroy              Destroy a Ceph OSD
    zap                  Zap/erase/destroy a device's partition table (and contents)
    trigger              Trigger an event (caled by udev)

Description
===========

:program:`ceph-disk` is a utility that can prepare and activate a disk, partition or
directory as a Ceph OSD. It is run directly or triggered by :program:`ceph-deploy`
or ``udev``. It can also be triggered by other deployment utilities like ``Chef``,
``Juju``, ``Puppet`` etc.

It actually automates the multiple steps involved in manual creation and start
of an OSD into two steps of preparing and activating the OSD by using the
subcommands ``prepare`` and ``activate``.

:program:`ceph-disk` also automates the multiple steps involved to manually stop
and destroy an OSD into two steps of deactivating and destroying the OSD by using
the subcommands ``deactivate`` and ``destroy``.

The documentation for each subcommand (prepare, activate, etc.) can be displayed
with its ``--help`` option. For instance ``ceph-disk prepare --help``.

Availability
============

:program:`ceph-disk` is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.

See also
========

:doc:`ceph-osd <ceph-osd>`\(8),
:doc:`ceph-deploy <ceph-deploy>`\(8)
