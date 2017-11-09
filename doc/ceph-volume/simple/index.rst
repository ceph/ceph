.. _ceph-volume-simple:

``simple``
==========
Implements the functionality needed to manage OSDs from the ``simple`` subcommand:
``ceph-volume simple``

**Command Line Subcommands**

* :ref:`ceph-volume-simple-scan`

* :ref:`ceph-volume-simple-activate`

* :ref:`ceph-volume-simple-systemd`


By *taking over* management, it disables all ``ceph-disk`` systemd units used
to trigger devices at startup, relying on basic (customizable) JSON
configuration and systemd for starting up OSDs.
