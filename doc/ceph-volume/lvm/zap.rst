.. _ceph-volume-lvm-zap:

``zap``
=======

This subcommand is used to zap lvs or partitions that have been used
by ceph OSDs so that they may be reused. If given a path to a logical
volume it must be in the format of vg/lv. Any filesystems present
on the given lv or partition will be removed and all data will be purged.

.. note:: The lv or partition will be kept intact.

Zapping a logical volume::

      ceph-volume lvm zap {vg name/lv name}

Zapping a partition::

      ceph-volume lvm zap /dev/sdc1
