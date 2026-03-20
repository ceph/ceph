:orphan:

==========================
Installing Ceph on Windows
==========================

The Ceph client tools and libraries can be natively used on Windows. This avoids
the need for additional layers such as iSCSI gateways or SMB shares,
drastically improving the performance.

Prerequisites
=============

Supported platforms
-------------------
.. note::

  Please see the `OS recommendations`_ regarding client package support.

Windows Server 2019 and Windows Server 2016 are supported. Previous Windows
Server versions, including Windows client versions such as Windows 10, might
work but haven't been tested.

Windows Server 2016 does not provide Unix sockets, in which case some commands
might be unavailable.

Secure boot
-----------

The ``WNBD`` driver hasn't been signed by Microsoft, which means that Secure Boot
must be disabled.

Dokany
------

In order to mount Ceph filesystems, ``ceph-dokan`` requires Dokany to be
installed. You may fetch the installer as well as the source code from the
Dokany GitHub repository: https://github.com/dokan-dev/dokany/releases

Make sure to install Dokany 2.0.5 or later.

Unlike ``WNBD``, Dokany isn't included in the Ceph MSI installer.

MSI installer
=============

Using the MSI installer is the recommended way of installing Ceph on Windows.
It can be downloaded from here: https://cloudbase.it/ceph-for-windows/

As mentioned earlier, the Ceph installer does not include Dokany, which has
to be installed separately.

A server reboot is required after uninstalling the driver, otherwise subsequent
install attempts may fail.

The following project allows building the MSI installer:
https://github.com/cloudbase/ceph-windows-installer. It can either use prebuilt
Ceph and WNBD binaries or compile them from scratch.

Manual installation
===================

The following document describes the build process and manual installation:
https://github.com/ceph/ceph/blob/master/README.windows.rst

Configuration
=============

Please check the `Windows configuration sample`_ to get started.

You'll also need a keyring file. The `General CephFS Prerequisites`_ page provides a
simple example, showing how a new CephX user can be created and how its secret
key can be retrieved.

For more details on CephX user management, see the `Client Authentication`_
and :ref:`User Management <user-management>`.

Further reading
===============

* `RBD Windows documentation`_
* `CephFS Windows documentation`_
* `Windows troubleshooting`_

.. _CephFS Windows documentation: ../../cephfs/ceph-dokan
.. _Windows configuration sample: ../windows-basic-config
.. _RBD Windows documentation: ../../rbd/rbd-windows/
.. _Windows troubleshooting: ../windows-troubleshooting
.. _General CephFS Prerequisites: ../../cephfs/mount-prerequisites
.. _Client Authentication: ../../cephfs/client-auth
.. _Windows testing: ../dev/tests-windows
.. _OS recommendations: ../../start/os-recommendations
