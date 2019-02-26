.. _ceph-volume-lvm-zap:

``zap``
=======

This subcommand is used to zap lvs, partitions or raw devices that have been used
by ceph OSDs so that they may be reused. If given a path to a logical
volume it must be in the format of vg/lv. Any filesystems present
on the given lv or partition will be removed and all data will be purged.

.. note:: The lv or partition will be kept intact.

.. note:: If the logical volume, raw device or partition is being used for any ceph related
          mount points they will be unmounted.

Zapping a logical volume::

    ceph-volume lvm zap {vg name/lv name}

Zapping a partition::

    ceph-volume lvm zap /dev/sdc1

Removing Devices
----------------
When zapping, and looking for full removal of the device (lv, vg, or partition)
use the ``--destroy`` flag. A common use case is to simply deploy OSDs using
a whole raw device. If you do so and then wish to reuse that device for another
OSD you must use the ``--destroy`` flag when zapping so that the vgs and lvs
that ceph-volume created on the raw device will be removed.

.. note:: Multiple devices can be accepted at once, to zap them all

Zapping a raw device and destroying any vgs or lvs present::

    ceph-volume lvm zap /dev/sdc --destroy


This action can be performed on partitions, and logical volumes as well::

    ceph-volume lvm zap /dev/sdc1 --destroy
    ceph-volume lvm zap osd-vg/data-lv --destroy


Finally, multiple devices can be detected if filtering by OSD ID and/or OSD
FSID. Either identifier can be used or both can be used at the same time. This
is useful in situations where multiple devices associated with a specific ID
need to be purged. When using the FSID, the filtering is stricter, and might
not match other (possibly invalid) devices associated to an ID.

By ID only::

    ceph-volume lvm zap --destroy --osd-id 1

By FSID::

    ceph-volume lvm zap --destroy --osd-fsid 2E8FBE58-0328-4E3B-BFB7-3CACE4E9A6CE

By both::

    ceph-volume lvm zap --destroy --osd-fsid 2E8FBE58-0328-4E3B-BFB7-3CACE4E9A6CE --osd-id 1


.. warning:: If the systemd unit associated with the OSD ID to be zapped is
             detected as running, the tool will refuse to zap until the daemon is stopped.
