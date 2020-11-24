About
-----

Ceph Windows support is currently a work in progress. For now, the main focus
is the client side, allowing Windows hosts to consume rados, rbd and cephfs
resources.

Building
--------

At the moment, mingw gcc >= 8 is the only supported compiler for building ceph
components for Windows. Support for msvc and clang will be added soon.

`win32_build.sh`_ can be used for cross compiling Ceph and its dependencies.
It may be called from a Linux environment, including Windows Subsystem for
Linux. MSYS2 and CygWin may also work but those weren't tested.

.. _win32_build.sh: win32_build.sh

The script accepts the following flags:

============  ===============================  ===============================
Flag          Description                      Default value
============  ===============================  ===============================
CEPH_DIR      The Ceph source code directory.  The same as the script.
BUILD_DIR     The directory where the          $CEPH_DIR/build
              generated artifacts will be
              placed.
DEPS_DIR      The directory where the Ceph     $CEPH_DIR/build.deps
              dependencies will be built.
NUM_WORKERS   The number of workers to use     The number of vcpus
              when building Ceph.              available
CLEAN_BUILD   Clean the build directory.
SKIP_BUILD    Run cmake without actually
              performing the build.
============  ===============================  ===============================

Current status
--------------

The rados and rbd binaries and libs compile successfully and can be used on
Windows, successfully connecting to the cluster and consuming pools.

Ceph filesystems can be mounted using the ``ceph-dokan`` command, which
requires the Dokany package to be installed. Note that dokany is a well
maintained fork of the Dokan project, allowing filesystems to be implemented
in userspace, pretty much like Fuse.

RBD images can be mounted using the ``rbd`` or ``rbd-wnbd`` commands. The
``WNBD`` driver is required for mapping RBD images on Windows.

The libraries have to be built statically at the moment. The reason is that
there are a few circular library dependencies or unspecified dependencies,
which isn't supported when building DLLs. This mostly affects ``cls`` libraries.

A significant number of tests from the ``tests`` directory have been ported,
providing adequate coverage.

Supported platforms
===================

Windows Server 2019 and Windows Server 2016 are supported. Previous Windows
Server versions, including Windows client versions such as Windows 10, might
work but haven't been tested.

Windows Server 2016 does not provide unix sockets, in which case the Ceph admin
socket feature will be unavailable.

Compatibility
=============

RBD images can be exposed to the OS and host Windows partitions or they can be
attached to Hyper-V VMs in the same way as iSCSI disks.

At the moment, the Microsoft Failover Cluster can't use WNBD disks as
Cluster Shared Volumes (CSVs) underlying storage. The main reason is that
``WNBD`` and ``rbd-wnbd`` don't support the *SCSI Persistent Reservations*
feature yet.

OpenStack integration has been proposed as well and will most probably be
included in the next OpenStack release, allowing RBD images managed by OpenStack
Cinder to be attached to Hyper-V VMs managed by OpenStack Nova.

.. _installing:
Installing
----------

Soon we're going to provide an MSI installed for Ceph. For now, unzip the
binaries that you may have obtained by following the building_ step.

You may want to update the environment PATH variable, including the Ceph
path. Assuming that you've copied the Ceph binaries to ``C:\Ceph``, you may
use the following Powershell command:

.. code:: bash

    [Environment]::SetEnvironmentVariable("Path", "$env:PATH;C:\ceph", "Machine")

In order to mount Ceph filesystems, you will have to install Dokany.
You may fetch the installer as well as the source code from the Dokany
Github repository: https://github.com/dokan-dev/dokany/releases

Make sure to use 1.3.1, which at time of the writing is the latest
stable release.

In order to map RBD images, the ``WNBD`` driver must be installed. Please
check out this page for more details about ``WNBD`` and the install process:
https://github.com/cloudbase/wnbd

Configuring
-----------

ceph.conf
=========

The default location for the ``ceph.conf`` file on Windows is
``%ProgramData%\ceph\ceph.conf``, which usually expands to
``C:\ProgramData\ceph\ceph.conf``.

Below you may find a sample. Please fill in the monitor addresses
accordingly.

.. code:: ini

    [global]
        log to stderr = true

        run dir = C:/ProgramData/ceph/out
        crash dir = C:/ProgramData/ceph/out
    [client]
        keyring = C:/ProgramData/ceph/keyring
        ; log file = C:/ProgramData/ceph/out/$name.$pid.log
        admin socket = C:/ProgramData/ceph/out/$name.$pid.asok
    [global]
        mon host =  [v2:xx.xx.xx.xx:40623,v1:xx.xx.xx.xx:40624] [v2:xx.xx.xx.xx:40625,v1:xx.xx.xx.xx:40626] [v2:xx.xx.xx.xx:40627,v1:xx.xx.xx.xx:40628]

Assuming that you're going to use this config sample, don't forget to
also copy your keyring file to the specified location and make sure
that the configured directories exist (e.g. ``C:\ProgramData\ceph\out``).

Please use slashes ``/`` instead of backslashes ``\`` as path separators
within ``ceph.conf`` for the time being. Also, don't forget to include a
newline at the end of the file, Ceph will complain otherwise.

.. _windows_service:
Windows service
===============
In order to ensure that rbd-wnbd mappings survive host reboot, you'll have
to configure it to run as a Windows service. Only one such service may run per
host.

All mappings are currently persistent, being recreated when the service starts,
unless explicitly unmapped. The service disconnects the mappings when being
stopped. This also allows adjusting the Windows service start order so that rbd
images can be mapped before starting services that may depend on it, such as
VMMS.

In order to be able to reconnect the images, ``rbd-wnbd`` stores mapping
information in the Windows registry at the following location:
``SYSTEM\CurrentControlSet\Services\rbd-wnbd``.

The following command can be used to configure the service. Please update
the ``rbd-wnbd.exe`` path accordingly.

.. code:: PowerShell

    New-Service -Name "ceph-rbd" `
                -Description "Ceph RBD Mapping Service" `
                -BinaryPathName "c:\ceph\rbd-wnbd.exe service" `
                -StartupType Automatic

Usage
-----

Cephfs
======

In order to mount a ceph filesystem, the following command can be used:

.. code:: PowerShell

    ceph-dokan.exe -c c:\ceph.conf -l x

The above command will mount the default ceph filesystem using the drive
letter ``x``. If ``ceph.conf`` is placed at the default location, which
is ``%ProgramData%\ceph\ceph.conf``, then this argument becomes optional.

The ``-l`` argument also allows using an empty folder as a mountpoint
instead of a drive letter.

The uid and gid used for mounting the filesystem defaults to 0 and may be
changed using the ``-u`` and ``-g`` arguments. ``-n`` can be used in order
to skip enforcing permissions on client side. Be aware that Windows ACLs
are ignored. Posix ACLs are supported but cannot be modified using the
current CLI. In the future, we may add some command actions to change
file ownership or permissions.

For debugging purposes, ``-d`` and ``s`` might be used. The first one will
enable debug output and the latter will enable stderr logging. By default,
debug messages are sent to a connected debugger.

You may use ``--help`` to get the full list of available options. The
current syntax is up for discussion and might change.

RBD
===

The ``rbd`` command can be used to create, remove, import, export, map or
unmap images exactly like it would on Linux.

Mapping images
..............

In order to map RBD images, please install ``WNBD``, as mentioned by the
installing_ guide.

The behavior and CLI is similar to the Linux counterpart, with a few
notable differences:

* device paths cannot be requested. The disk number and path will be picked by
  Windows. If a device path is provided by the used when mapping an image, it
  will be used as an identifier, which can also be used when unmapping the
  image.
* the ``show`` command was added, which describes a specific mapping.
  This can be used for retrieving the disk path.
* the ``service`` command was added, allowing rbd-wnbd to run as a Windows service.
  All mappings are currently perisistent, being recreated when the service
  stops, unless explicitly unmapped. The service disconnects the mappings
  when being stopped.
* the ``list`` command also includes a ``status`` column.

The purpose of the ``service`` mode is to ensure that mappings survive reboots
and that the Windows service start order can be adjusted so that rbd images can
be mapped before starting services that may depend on it, such as VMMS.

Please follow the windows_service_ guide in order to configure the service.

The mapped images can either be consumed by the host directly or exposed to
Hyper-V VMs.

Hyper-V VM disks
~~~~~~~~~~~~~~~~

The following sample imports an RBD image and boots a Hyper-V VM using it.

.. code:: PowerShell

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

    rbd-wnbd map cirros-0.5.1-x86_64-disk.raw

    # Let's have a look at the mappings.
    rbd-wnbd list
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
~~~~~~~~~~~~~~~~~~

The following sample creates an empty RBD image, attaches it to the host and
initializes a partition.

.. code:: PowerShell

    rbd create blank_image --size=1G
    rbd-wnbd map blank_image

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
