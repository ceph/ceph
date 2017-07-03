=============
iSCSI Targets
=============

Traditionally, block-level access to a Ceph storage cluster has been
limited to QEMU and ``librbd``, which is a key enabler for adoption
within OpenStack environments. Starting with the Ceph Jewel release,
block-level access is expanding to offer standard iSCSI support allowing
wider platform usage, and potentially opening new use cases and
customers.

-  CentOS 7.3 for Ansible deployments

-  CentOS 7.4 for command-line deployments

-  A working Ceph Storage cluster, deployed with ``ceph-ansible`` or using the command-line interface

-  iSCSI gateways nodes, which can either be colocated with OSD nodes or on dedicated nodes

-  Separate network subnets for iSCSI front-end traffic and Ceph back-end traffic

A choice of using Ansible or the command-line interface are the
available deployment methods for installing and configuring the Ceph
iSCSI gateway:

-  `Using Ansible <Configuring the iSCSI Target using Ansible>`_

-  `Using the Command Line Interface <Configuring the iSCSI Target using the Command Line Interface>`_
