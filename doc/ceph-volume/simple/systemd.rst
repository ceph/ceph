.. _ceph-volume-simple-systemd:

systemd
=======
Upon startup, it will identify the logical volume by loading the JSON file in
``/etc/ceph/osd/{id}-{uuid}.json`` corresponding to the instance name of the
systemd unit.

After identifying the correct volume it will then proceed to mount it by using
the OSD destination conventions, that is::

    /var/lib/ceph/osd/{cluster name}-{osd id}

For our example OSD with an id of ``0``, that means the identified device will
be mounted at::


    /var/lib/ceph/osd/ceph-0


Once that process is complete, a call will be made to start the OSD::

    systemctl start ceph-osd@0

The systemd portion of this process is handled by the ``ceph-volume simple
trigger`` sub-command, which is only in charge of parsing metadata coming from
systemd and startup, and then dispatching to ``ceph-volume simple activate`` which
would proceed with activation.
