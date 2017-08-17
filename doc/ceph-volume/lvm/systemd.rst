.. _ceph-volume-systemd:

systemd
=======
As part of the :ref:`ceph-volume-lvm-activate` process, a few systemd units will get enabled
that will use the OSD id and uuid as part of their name. These units will be
run when the system boots, and will proceed to activate their corresponding
volumes.

The API for activation requires both the :term:`OSD id` and :term:`OSD uuid`,
which get persisted by systemd. Internally, the activation process enables the
systemd unit using the following convention::

    ceph-volume@<type>-<extra metadata>

Where ``type`` is the sub-command used to parse the extra metadata, and ``extra
metadata`` is any additional information needed by the sub-command to be able
to activate the OSD. For example an OSD with an ID of 0, for the ``lvm``
sub-command would look like::

    systemctl enable ceph-volume@lvm-0-0A3E1ED2-DA8A-4F0E-AA95-61DEC71768D6


Process
-------
The systemd unit is a :term:`systemd oneshot` service, meant to start at boot after the
local filesystem is ready to be used.

Upon startup, it will identify the logical volume using :term:`LVM tags`,
finding a matching ID and later ensuring it is the right one with
the :term:`OSD uuid`.

After identifying the correct volume it will then proceed to mount it by using
the OSD destination conventions, that is::

    /var/lib/ceph/osd/<cluster name>-<osd id>

For our example OSD with an id of ``0``, that means the identified device will
be mounted at::


    /var/lib/ceph/osd/ceph-0

Once that process is complete, a call will be made to start the OSD::

    systemctl start ceph-osd@0
