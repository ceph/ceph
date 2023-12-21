.. _ceph-volume-lvm-activate:

``activate``
============

After :ref:`ceph-volume-lvm-prepare` has completed its run, the volume can be
activated. 

Activating the volume involves enabling a ``systemd`` unit that persists the
``OSD ID`` and its ``UUID`` (which is also called the ``fsid`` in the Ceph CLI
tools). After this information has been persisted, the cluster can determine
which OSD is enabled and must be mounted.

.. note:: The execution of this call is fully idempotent. This means that the
   call can be executed multiple times without changing the result of its first
   successful execution.

For information about OSDs deployed by cephadm, refer to
:ref:`cephadm-osd-activate`.

New OSDs
--------
To activate newly prepared OSDs both the :term:`OSD id` and :term:`OSD uuid`
need to be supplied. For example::

    ceph-volume lvm activate --bluestore 0 0263644D-0BF1-4D6D-BC34-28BD98AE3BC8

.. note:: The UUID is stored in the ``fsid`` file in the OSD path, which is
          generated when :ref:`ceph-volume-lvm-prepare` is used.

Activating all OSDs
-------------------

.. note:: For OSDs deployed by cephadm, please refer to :ref:`cephadm-osd-activate`
          instead.

It is possible to activate all existing OSDs at once by using the ``--all``
flag. For example::

    ceph-volume lvm activate --all

This call will inspect all the OSDs created by ceph-volume that are inactive
and will activate them one by one. If any of the OSDs are already running, it
will report them in the command output and skip them, making it safe to rerun
(idempotent).

requiring uuids
^^^^^^^^^^^^^^^
The :term:`OSD uuid` is being required as an extra step to ensure that the
right OSD is being activated. It is entirely possible that a previous OSD with
the same id exists and would end up activating the incorrect one.


dmcrypt
^^^^^^^
If the OSD was prepared with dmcrypt by ceph-volume, there is no need to
specify ``--dmcrypt`` on the command line again (that flag is not available for
the ``activate`` subcommand). An encrypted OSD will be automatically detected.


Discovery
---------
With OSDs previously created by ``ceph-volume``, a *discovery* process is
performed using :term:`LVM tags` to enable the systemd units.

The systemd unit will capture the :term:`OSD id` and :term:`OSD uuid` and
persist it. Internally, the activation will enable it like::

    systemctl enable ceph-volume@lvm-$id-$uuid

For example::

    systemctl enable ceph-volume@lvm-0-8715BEB4-15C5-49DE-BA6F-401086EC7B41

Would start the discovery process for the OSD with an id of ``0`` and a UUID of
``8715BEB4-15C5-49DE-BA6F-401086EC7B41``.

.. note:: for more details on the systemd workflow see :ref:`ceph-volume-lvm-systemd`

The systemd unit will look for the matching OSD device, and by looking at its
:term:`LVM tags` will proceed to:

#. Mount the device in the corresponding location (by convention this is
``/var/lib/ceph/osd/<cluster name>-<osd id>/``)

#. Ensure that all required devices are ready for that OSD.

#. Start the ``ceph-osd@0`` systemd unit

.. note:: The system infers the objectstore type by
          inspecting the LVM tags applied to the OSD devices

Existing OSDs
-------------
For existing OSDs that have been deployed with ``ceph-disk``, they need to be
scanned and activated :ref:`using the simple sub-command <ceph-volume-simple>`.
If a different tool was used then the only way to port them over to the new
mechanism is to prepare them again (losing data). See
:ref:`ceph-volume-lvm-existing-osds` for details on how to proceed.

Summary
-------
To recap the ``activate`` process for :term:`bluestore`:

#. Require both :term:`OSD id` and :term:`OSD uuid`
#. Enable the system unit with matching id and uuid
#. Create the ``tmpfs`` mount at the OSD directory in
   ``/var/lib/ceph/osd/$cluster-$id/``
#. Recreate all the files needed with ``ceph-bluestore-tool prime-osd-dir`` by
   pointing it to the OSD ``block`` device.
#. The systemd unit will ensure all devices are ready and linked
#. The matching ``ceph-osd`` systemd unit will get started
