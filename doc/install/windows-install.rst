:orphan:

.. _install-windows:

=======================================
 Installing the Ceph Client on Windows
=======================================

.. meta::
   :description: Install the Ceph client on a Windows host from the MSI installer, which includes the WNBD driver for RBD images, and install Dokany separately for CephFS mounts.
   :ceph-page-type: procedure

Install the :term:`Ceph client <Ceph Client>` tools and libraries on a
Windows host. They run natively, without an iSCSI gateway or an SMB share in
between, which improves performance. The host can map :term:`RBD` images as
local disks through the WNBD (Windows Network Block Device) kernel driver,
and mount :term:`CephFS<Ceph File System>` file systems.

Prerequisites
=============

- A supported Windows version:

  - Windows Server 2019 or Windows Server 2022.
  - Windows 10 LTSC (Long-Term Servicing Channel) or Windows 11, for
    development and testing only.

  Windows Server 2016 is not supported: the WNBD driver needs Windows Server
  2019 (build 17763) or later. For the support status of the Windows client
  packages, see :ref:`os-recommendations`.
- An account with administrator rights. Run every command on this page in
  PowerShell opened with Run as administrator.
- Secure Boot disabled in the UEFI firmware settings, because the WNBD driver
  has not been signed by Microsoft. ``Confirm-SecureBootUEFI`` prints
  ``False`` when Secure Boot is disabled. On a host that boots in BIOS mode
  or has no Secure Boot support, it prints "Cmdlet not supported on this
  platform", which also means that Secure Boot is off.

  .. warning::

     On a host that uses BitLocker, changing the Secure Boot setting can
     start BitLocker recovery at the next boot, and the drive stays locked
     until someone enters its recovery key. Suspend BitLocker and keep the
     recovery key at hand before you change the setting. See `BitLocker
     recovery overview
     <https://learn.microsoft.com/en-us/windows/security/operating-system-security/data-protection/bitlocker/recovery-overview>`__.

.. _dokany:

.. _msi-installer:

Procedure
=========

#. If you will mount CephFS, install Dokany 2.0.5 or later from
   https://github.com/dokan-dev/dokany/releases (``Dokan_x64.msi`` or
   ``DokanSetup.exe``).

   Dokany is a Windows driver for file systems that run in user space,
   similar to FUSE. :ref:`ceph-dokan <ceph-dokan>`, the CephFS client for
   Windows, needs it. The Ceph installer includes the WNBD driver but not
   Dokany. The Dokany releases page also has the Dokany source code.

   Check that Dokany is installed:

   .. prompt:: powershell

      Test-Path C:\Windows\System32\dokan2.dll

   The output is ``True``.

#. Download the Ceph MSI installer (a Windows Installer package), the
   recommended way to install the client, from
   https://cloudbase.it/ceph-for-windows/.

   The download is one ``.msi`` file, named after the Ceph release.

   .. note::

      To build the MSI installer yourself, use
      https://github.com/cloudbase/ceph-windows-installer. It can use
      prebuilt Ceph and WNBD binaries or compile them from scratch. To build
      and install the client without the MSI installer, see
      https://github.com/ceph/ceph/blob/main/README.windows.rst.

#. If Ceph for Windows is already installed on the host, remove it first.
   The installer cannot upgrade an installed version: every Ceph for Windows
   MSI has the same product code, so the installer stops with "Another
   version of this product is already installed."

   .. warning::

      The installer stops the ``ceph-rbd`` service, which disconnects the
      mapped disks. Uninstalling, or removing the **Windows Ceph RBD driver**
      feature, also removes the WNBD driver. A disk that is still in use when
      this happens is disconnected by force, without waiting for pending
      writes: data that is not yet written is lost, and virtual machines or
      services that use the disks fail.

   Stop the workloads that use mapped RBD images, unmap every image with
   ``rbd device unmap``, and check that no image is mapped:

   .. prompt:: powershell

      rbd device list

   The output shows only the column headings, with no images. Then uninstall
   the **Ceph for Windows** entry, for example **Ceph for Windows (Squid)**,
   in **Programs and Features**, and restart the host.

#. Start the installer from PowerShell with a log file, and keep both
   features, **Ceph CLI** and **Windows Ceph RBD driver**, selected:

   .. prompt:: powershell

      msiexec.exe /i .\ceph.msi /l*v .\ceph-install.log

   Replace ``ceph.msi`` with the name of the file that you downloaded. The
   last page of the installer reads "Thank you for installing Ceph for
   Windows." The installer adds its ``bin`` folder,
   ``C:\Program Files\Ceph\bin`` by default, to the system ``PATH``.

   The installer asks for a restart after it installs or removes the WNBD
   driver. After you uninstall the driver, restart the host before you run
   the installer again, or the driver installation can fail. In a silent
   installation (``msiexec /qn``), Windows restarts the host at the end
   without asking. To restart later instead, add ``REBOOT=ReallySuppress``
   to the ``msiexec`` command.

#. Restart the host when the installer asks you to, or later with this
   command:

   .. prompt:: powershell

      Restart-Computer

   Windows restarts the host.

Verification
============

Open a new PowerShell window with Run as administrator, then run these
checks:

- Check the Ceph command-line tools:

  .. prompt:: powershell

     rbd --version

  The output starts with ``ceph version``.

- Check the WNBD driver:

  .. prompt:: powershell

     wnbd-client -v

  The output lists the versions of ``wnbd-client.exe``, ``libwnbd.dll``, and
  the driver, ``wnbd.sys``.

- If you installed Dokany, check the CephFS client:

  .. prompt:: powershell

     ceph-dokan --help

  The output shows ``Usage: ceph-dokan.exe -l <mountpoint>`` and the list of
  options.

Troubleshooting
===============

- **"This installation package could not be opened."** ``msiexec`` looks for
  ``.\ceph.msi`` in the current directory, which in PowerShell opened with
  Run as administrator is often ``C:\Windows\system32``. Change to the folder
  that holds the file, for example with ``Set-Location $HOME\Downloads``, and
  run the installer command again.
- **"Another version of this product is already installed."** Ceph for
  Windows is already installed on the host. Remove it and restart the host
  as in step 3, then run the installer again.
- **"Failed to install the Windows Ceph RBD WNBD driver. A reboot might be
  required. Check the MSI log for more details."** The driver can fail to
  install if the host has not restarted since a previous version of the
  driver was removed, and it refuses to install on Windows older than the
  build that Prerequisites names. Restart the host and run the installer
  again. The log from step 4, ``ceph-install.log``, shows the failed
  ``wnbd-client.exe install-driver`` command and its return code. For the
  driver installation log, see :ref:`install-windows-troubleshooting`.
- **"The term 'rbd' is not recognized".** The PowerShell window was opened
  before the installer added its ``bin`` folder to the system ``PATH``. Open
  a new PowerShell window.
- **"wnbd-client -v" prints no "wnbd.sys" line.** The output ends with an
  error such as "No WNBD adapter found." instead: the WNBD driver is not
  loaded. Check that ``Confirm-SecureBootUEFI`` prints ``False``, and restart
  the host if it has not restarted since you ran the installer.
- **ceph-dokan prints nothing, and "$LASTEXITCODE" is -1073741515.** Windows
  cannot find ``dokan2.dll``, which Dokany installs. Install Dokany as in
  step 1.

Next Steps
==========

- Create the configuration file and copy the :term:`keyring<Keyring>`. See
  :ref:`install-windows-basic-config`.

Additional Resources
====================

- :ref:`install-windows-troubleshooting`
- :ref:`os-recommendations`
- :doc:`/rbd/rbd-windows`
- :ref:`ceph-dokan`
