.. _ceph-volume-lvm-create:

``create``
===========
This subcommand wraps the two-step process to provision a new osd (calling
``prepare`` first and then ``activate``) into a single
one. The reason to prefer ``prepare`` and then ``activate`` is to gradually
introduce new OSDs into a cluster, and avoiding large amounts of data being
rebalanced.

The single-call process unifies exactly what :ref:`ceph-volume-lvm-prepare` and
:ref:`ceph-volume-lvm-activate` do, with the convenience of doing it all at
once.

There is nothing different to the process except the OSD will become up and in
immediately after completion.

The backing objectstore can be specified with:

* :ref:`--bluestore <ceph-volume-lvm-prepare_bluestore>`

All command line flags and options are the same as ``ceph-volume lvm prepare``.
Please refer to :ref:`ceph-volume-lvm-prepare` for details.
