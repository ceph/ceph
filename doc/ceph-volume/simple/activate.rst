.. _ceph-volume-simple-activate:

``activate``
============
Once :ref:`ceph-volume-simple-scan` has been completed, and all the metadata
captured for an OSD has been persisted to ``/etc/ceph/osd/{id}-{uuid}.json``
the OSD is now ready to get "activated".

This activation process **disables** all ``ceph-disk`` systemd units by masking
them, to prevent the UDEV/ceph-disk interaction that will attempt to start them
up at boot time.

The disabling of ``ceph-disk`` units is done only when calling ``ceph-volume
simple activate`` directly, but is avoided when being called by systemd when
the system is booting up.

The activation process requires using both the :term:`OSD id` and :term:`OSD uuid`
To activate parsed OSDs::

    ceph-volume simple activate 0 6cc43680-4f6e-4feb-92ff-9c7ba204120e

The above command will assume that a JSON configuration will be found in::

    /etc/ceph/osd/0-6cc43680-4f6e-4feb-92ff-9c7ba204120e.json

Alternatively, using a path to a JSON file directly is also possible::

    ceph-volume simple activate --file /etc/ceph/osd/0-6cc43680-4f6e-4feb-92ff-9c7ba204120e.json

requiring uuids
^^^^^^^^^^^^^^^
The :term:`OSD uuid` is being required as an extra step to ensure that the
right OSD is being activated. It is entirely possible that a previous OSD with
the same id exists and would end up activating the incorrect one.


Discovery
---------
With OSDs previously scanned by ``ceph-volume``, a *discovery* process is
performed using ``blkid`` and ``lvm``. There is currently support only for
devices with GPT partitions and LVM logical volumes.

The GPT partitions will have a ``PARTUUID`` that can be queried by calling out
to ``blkid``, and the logical volumes will have a ``lv_uuid`` that can be
queried against ``lvs`` (the LVM tool to list logical volumes).

This discovery process ensures that devices can be correctly detected even if
they are repurposed into another system or if their name changes (as in the
case of non-persisting names like ``/dev/sda1``)

The JSON configuration file used to map what devices go to what OSD will then
coordinate the mounting and symlinking as part of activation.

To ensure that the symlinks are always correct, if they exist in the OSD
directory, the symlinks will be re-done.

A systemd unit will capture the :term:`OSD id` and :term:`OSD uuid` and
persist it. Internally, the activation will enable it like::

    systemctl enable ceph-volume@simple-$id-$uuid

For example::

    systemctl enable ceph-volume@simple-0-8715BEB4-15C5-49DE-BA6F-401086EC7B41

Would start the discovery process for the OSD with an id of ``0`` and a UUID of
``8715BEB4-15C5-49DE-BA6F-401086EC7B41``.


The systemd process will call out to activate passing the information needed to
identify the OSD and its devices, and it will proceed to:

# mount the device in the corresponding location (by convention this is
  ``/var/lib/ceph/osd/<cluster name>-<osd id>/``)

# ensure that all required devices are ready for that OSD and properly linked. 
The symbolic link will **always** be re-done to ensure that the correct device is linked.

# start the ``ceph-osd@0`` systemd unit
