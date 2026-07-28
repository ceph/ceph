=============
iSCSI Targets
=============

Traditionally, block-level access to a Ceph storage cluster has been
limited to QEMU and ``librbd``, which is a key enabler for adoption
within OpenStack environments. Starting with the Ceph Luminous release,
block-level access is expanding to offer standard iSCSI support allowing
wider platform usage, and potentially opening new use cases.

-  Red Hat Enterprise Linux or CentOS Stream 7.5 (or newer); Rocky Linux 8 (or newer); Linux kernel v4.16 (or newer)

  .. note::

     **Rocky Linux:** If the Ceph container image is based on Rocky, the packaged `ceph-iscsi
     <https://github.com/ceph/ceph-iscsi>`__ tooling must be new enough to treat
     Rocky like other RHEL-family systems, or ``gwcli`` may fail to add gateways.
     See `Orchestrator #75359 <https://tracker.ceph.com/issues/75359>`__ for
     background.

-  A working Ceph Storage cluster, deployed with ``ceph-ansible`` or using the command-line interface

-  iSCSI gateways nodes, which can either be colocated with OSD nodes or on dedicated nodes

-  Separate network subnets for iSCSI front-end traffic and Ceph back-end traffic

A choice of using Ansible or the command-line interface are the
available deployment methods for installing and configuring the Ceph
iSCSI gateway:

.. toctree::
  :maxdepth: 1

  Using Ansible <iscsi-target-ansible>
  Using the Command Line Interface <iscsi-target-cli>
