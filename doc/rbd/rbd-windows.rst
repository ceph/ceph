==============
RBD on Windows
==============

The ``rbd`` command can be used to create, remove, import, export, map or
unmap images exactly like it would on Linux. Make sure to check the
`RBD basic commands`_ guide.

``librbd.dll`` is also available for applications that can natively use Ceph.

Please check the `installation guide`_ to get started.

Windows service
===============
On Windows, ``rbd-wnbd`` daemons are managed by a centralized service. This allows
decoupling the daemons from the Windows session from which they originate. At
the same time, the service is responsible of recreating persistent mappings,
usually when the host boots.

Note that only one such service may run per host.

By default, all image mappings are persistent. Non-persistent mappings can be
requested using the ``-onon-persistent`` ``rbd`` flag.

Persistent mappings are recreated when the service starts, unless explicitly
unmapped. The service disconnects the mappings when being stopped. This also
allows adjusting the Windows service start order so that RBD images can be
mapped before starting services that may depend on it, such as VMMS.

In order to be able to reconnect the images, ``rbd-wnbd`` stores mapping
information in the Windows registry at the following location:
``SYSTEM\CurrentControlSet\Services\rbd-wnbd``.

The following command can be used to configure the service. Please update
the ``rbd-wnbd.exe`` path accordingly::

    New-Service -Name "ceph-rbd" `
                -Description "Ceph RBD Mapping Service" `
                -BinaryPathName "c:\ceph\rbd-wnbd.exe service" `
                -StartupType Automatic

Note that the Ceph MSI installer takes care of creating the ``ceph-rbd``
Windows service.

Usage
=====

Integration
-----------

RBD images can be exposed to the OS and host Windows partitions or they can be
attached to Hyper-V VMs in the same way as iSCSI disks.

Starting with Openstack Wallaby, the Nova Hyper-V driver can attach RBD Cinder
volumes to Hyper-V VMs.

Mapping images
--------------

The workflow and CLI is similar to the Linux counterpart, with a few
notable differences:

* device paths cannot be requested. The disk number and path will be picked by
  Windows. If a device path is provided by the used when mapping an image, it
  will be used as an identifier, which can also be used when unmapping the
  image.
* the ``show`` command was added, which describes a specific mapping.
  This can be used for retrieving the disk path.
* the ``service`` command was added, allowing ``rbd-wnbd`` to run as a Windows service.
  All mappings are by default persistent, being recreated when the service
  stops, unless explicitly unmapped. The service disconnects the mappings
  when being stopped.
* the ``list`` command also includes a ``status`` column.

The purpose of the ``service`` mode is to ensure that mappings survive reboots
and that the Windows service start order can be adjusted so that RBD images can
be mapped before starting services that may depend on it, such as VMMS.

The mapped images can either be consumed by the host directly or exposed to
Hyper-V VMs.

Hyper-V VM disks
----------------

The following sample imports an RBD image and boots a Hyper-V VM using it::

    # Feel free to use any other image. This one is convenient to use for
    # testing purposes because it's very small (~15MB) and the login prompt
    # prints the pre-configured password.
    wget http://download.cirros-cloud.net/0.5.1/cirros-0.5.1-x86_64-disk.img `
         -OutFile cirros-0.5.1-x86_64-disk.img

    # We'll need to make sure that the imported images are raw (so no qcow2 or vhdx).
    # You may get qemu-img from https://cloudbase.it/qemu-img-windows/
    # You can add the extracted location to $env:Path or update the path accordingly.
    qemu-img convert -O raw cirros-0.5.1-x86_64-disk.img cirros-0.5.1-x86_64-disk.raw

    rbd import cirros-0.5.1-x86_64-disk.raw
    # Let's give it a hefty 100MB size.
    rbd resize cirros-0.5.1-x86_64-disk.raw --size=100MB

    rbd device map cirros-0.5.1-x86_64-disk.raw

    # Let's have a look at the mappings.
    rbd device list
    Get-Disk

    $mappingJson = rbd-wnbd show cirros-0.5.1-x86_64-disk.raw --format=json
    $mappingJson = $mappingJson | ConvertFrom-Json

    $diskNumber = $mappingJson.disk_number

    New-VM -VMName BootFromRBD -MemoryStartupBytes 512MB
    # The disk must be turned offline before it can be passed to Hyper-V VMs
    Set-Disk -Number $diskNumber -IsOffline $true
    Add-VMHardDiskDrive -VMName BootFromRBD -DiskNumber $diskNumber
    Start-VM -VMName BootFromRBD

Windows partitions
------------------

The following sample creates an empty RBD image, attaches it to the host and
initializes a partition::

    rbd create blank_image --size=1G
    rbd device map blank_image -onon-persistent

    $mappingJson = rbd-wnbd show blank_image --format=json
    $mappingJson = $mappingJson | ConvertFrom-Json

    $diskNumber = $mappingJson.disk_number

    # The disk must be online before creating or accessing partitions.
    Set-Disk -Number $diskNumber -IsOffline $false

    # Initialize the disk, partition it and create a filesystem.
    Get-Disk -Number $diskNumber | `
        Initialize-Disk -PassThru | `
        New-Partition -AssignDriveLetter -UseMaximumSize | `
        Format-Volume -Force -Confirm:$false

    # Show the partition letter (for example, "D:" or "F:"):
    (Get-Partition -DiskNumber $diskNumber).DriveLetter

SAN policy
----------

The Windows SAN policy determines which disks will be automatically mounted.
The default policy (``offlineShared``) specifies that:

  All newly discovered disks that do not reside on a shared bus (such as SCSI
  and iSCSI) are brought online and made read-write. Disks that are left
  offline will be read-only by default."

Note that recent WNBD driver versions report rbd-wnbd disks as SAS, which is
also considered a shared bus. As a result, the disks will be offline and
read-only by default.

In order to turn a disk online (mounting the disk partitions) and clear the
read-only flag, use the following commands::

  Set-Disk -Number $diskNumber -IsOffline $false
  Set-Disk -Number $diskNumber -IsReadOnly $false

Please check the `Limitations`_ section to learn about the Windows limitations
that affect automatically mounted disks.

Windows documentation:

* `SAN policy reference`_
* `san command`_
* `StorageSetting command`_

Limitations
-----------

CSV support
~~~~~~~~~~~

Microsoft Failover Cluster requires SCSI Persistent Reservation support.

This ``rbd-wnbd`` feature is currently experimental and can be enabled using
the ``enable-pr`` flag::

    rbd device map test_image -o enable-pr

The workflow resembles the ``target_core_rbd`` module provided by some SUSE
distributions and commonly used with the ceph iSCSI gateway. Specifically, the
persistent reservations are stored as rados extended attributes and are
verified before every IO operation.

However, there are a few drawbacks:

* the performance can be affected by the additional round-trip performed for
  every IO request.
* data consistency concerns - if the node temporarily loses connection, gets
  preempted and then recovers network connectivity, queued IO operations may
  submitted, ignoring the current reservations. This might be mitigated
  by carefully configuring timeouts.
* only Windows hosts are aware of the scsi reservations

Some of the above limitations may be eventually addressed using features such
as rbd locks, rados blocklists and compound operations such as "compare and
write".

Hyper-V disk addressing
~~~~~~~~~~~~~~~~~~~~~~~

.. warning::
  Hyper-V identifies passthrough VM disks by number instead of SCSI ID, although
  the disk number can change across host reboots. This means that the VMs can end
  up using incorrect disks after rebooting the host, which is an important
  security concern. This issue also affects iSCSI and Fibre Channel disks.

There are a few possible ways of avoiding this Hyper-V limitation:

* use an NTFS/ReFS partition to store VHDX image files instead of directly
  attaching the RBD image. This may slightly impact the IO performance.
* use the Hyper-V ``AutomaticStartAction`` setting to prevent the VMs from
  booting with the incorrect disks and have a script that updates VM disks
  attachments before powering them back on. The ``ElementName`` field of the
  `Msvm_StorageAllocationSettingData`_ `WMI`_ class may be used to label VM
  disk attachments.
* use the Openstack Hyper-V driver, which automatically refreshes the VM disk
  attachments before powering them back on.

Automatically mounted disks
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Disks that are marked as "online" or "writable" will remain so after being
reconnected (e.g. due to host reboots, Ceph service restarts, etc).

Unfortunately, Windows restores the disk status based on the disk number,
ignoring the disk unique identifier. However, the disk numbers can change
after being reconnected. This issue also affects iSCSI and Fibre Channel disks.

Let's assume that the `SAN policy`_ is set to ``offlineShared``, three
RBD images are attached and disk 1 is turned online. After a reboot, disk 1
will become online but it may now correspond to a different RBD image. This can
be an issue if the disk that was mounted on the host was actually meant for a
VM.

Troubleshooting
===============

Please consult the `Windows troubleshooting`_ page.

.. _Windows troubleshooting: ../../install/windows-troubleshooting
.. _installation guide: ../../install/windows-install
.. _RBD basic commands: ../rados-rbd-cmds
.. _WNBD driver: https://github.com/cloudbase/wnbd
.. _Msvm_StorageAllocationSettingData: https://docs.microsoft.com/en-us/windows/win32/hyperv_v2/msvm-storageallocationsettingdata
.. _WMI: https://docs.microsoft.com/en-us/windows/win32/wmisdk/wmi-start-page
.. _san command: https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/san
.. _StorageSetting command: https://learn.microsoft.com/en-us/powershell/module/storage/set-storagesetting?view=windowsserver2022-ps
.. _SAN policy reference: https://learn.microsoft.com/en-us/windows-hardware/customize/desktop/unattend/microsoft-windows-partitionmanager-sanpolicy
