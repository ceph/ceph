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
  All mappings are by default perisistent, being recreated when the service
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

    # Initialize the disk, partition it and create a fileystem.
    Get-Disk -Number $diskNumber | `
        Initialize-Disk -PassThru | `
        New-Partition -AssignDriveLetter -UseMaximumSize | `
        Format-Volume -Force -Confirm:$false

Limitations
-----------

At the moment, the Microsoft Failover Cluster can't use WNBD disks as
Cluster Shared Volumes (CSVs) underlying storage. The main reason is that
``WNBD`` and ``rbd-wnbd`` don't support the *SCSI Persistent Reservations*
feature yet.

Troubleshooting
===============

Please consult the `Windows troubleshooting`_ page.

.. _Windows troubleshooting: ../../install/windows-troubleshooting
.. _installation guide: ../../install/windows-install
.. _RBD basic commands: ../rados-rbd-cmds
.. _WNBD driver: https://github.com/cloudbase/wnbd
