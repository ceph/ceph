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

If you are zapping a raw device or partition and would like any vgs or lvs created
from that device removed use the ``--destroy`` flag. A common use case is to simply
deploy OSDs using a whole raw device. If you do so and then wish to reuse that device for
another OSD you must use the ``--destroy`` flag when zapping so that the vgs and lvs that
ceph-volume created on the raw device will be removed.

Zapping a raw device and destroying any vgs or lvs present::

      ceph-volume lvm zap /dev/sdc --destroy
