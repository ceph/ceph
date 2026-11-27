.. warning::

   The iSCSI gateway is deprecated. It is no longer maintained or supported,
   and it may be removed from Ceph in a future release. New deployments should
   not use it. Existing deployments should migrate to direct RBD access from
   clients (``librbd``, QEMU, Kubernetes CSI and similar) or to the
   :ref:`NVMe-oF gateway <ceph-nvmeof>`.
