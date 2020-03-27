About
-----

Ceph Windows support is currently a work in progress. For now, the main focus
is the client side, allowing Windows hosts to consume rados, rbd and cephfs
resources.

.. _building:
Building
--------

At the moment, mingw gcc >= 8 is the only supported compiler for building ceph
components for Windows. Support for msvc and clang will be added soon.

`win32_build.sh`_ can be used for cross compiling Ceph and its dependencies.
It may be called from a Linux environment, including Windows Subsystem for
Linux. MSYS2 and CygWin may also work but those weren't tested.

This script currently supports Ubuntu 18.04 and openSUSE Tumbleweed, but it
may be easily adapted to run on other Linux distributions, taking into
account different package managers, package names or paths (e.g. mingw paths).

.. _win32_build.sh: win32_build.sh

The script accepts the following flags:

============  ===============================  ===============================
Flag          Description                      Default value
============  ===============================  ===============================
OS            Host OS distribution, for mingw  ubuntu (also valid: suse)
              and other OS specific settings.
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
SKIP_TESTS    Skip building Ceph tests.
BUILD_ZIP     Build a zip archive containing
              the generated binaries.
ZIP_DEST      Where to put a zip containing    $BUILD_DIR/ceph.zip
              the generated binaries.
STRIP_ZIPPED  If set, the zip will contain
              stripped binaries.
============  ===============================  ===============================

In order to build debug binaries as well as an archive containing stripped
binaries that may be easily moved around, one may use the following:

.. code:: bash

    BUILD_ZIP=1 STRIP_ZIPPED=1 SKIP_TESTS=1 ./win32_build.sh

In order to disable a flag, such as ``CLEAN_BUILD``, leave it undefined.

Debug binaries can be quite large, the following parameters may be passed to
``win32_build.sh`` to reduce the amount of debug information:

.. code:: bash

    CFLAGS="-g1" CXXFLAGS="-g1" CMAKE_BUILD_TYPE="Release"

``win32_build.sh`` will fetch dependencies using ``win32_deps_build.sh``. If
all dependencies are successfully prepared, this potentially time consuming
step will be skipped by subsequent builds. Be aware that you may have to do
a clean build (using the ``CLEAN_BUILD`` flag) when the dependencies change
(e.g. after switching to a more recent Ceph version by doing a ``git pull``).

Make sure to explicitly pass the "OS" parameter when directly calling
``win32_deps_build.sh``. Also, be aware of the fact that it will use the distro
specific package manager, which will require privileged rights.

Current status
--------------

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

The following project allows building an MSI installer that bundles ``ceph`` and
the ``WNBD`` driver: https://github.com/cloudbase/ceph-windows-installer

In order to manually install ``ceph``, start by unzipping the
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
within ``ceph.conf`` for the time being.

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
~~~~~~~~~~~~~~~~~~

The following sample creates an empty RBD image, attaches it to the host and
initializes a partition.

.. code:: PowerShell

    rbd create blank_image --size=1G
    rbd device map blank_image

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

Troubleshooting
...............

Wnbd
~~~~

For ``WNBD`` troubleshooting, please check this page: https://github.com/cloudbase/wnbd#troubleshooting

Privileges
~~~~~~~~~~

Most ``rbd-wnbd`` and ``rbd device`` commands require privileged rights. Make
sure to use an elevated PowerShell or CMD command prompt.

Crash dumps
~~~~~~~~~~~

Userspace crash dumps can be placed at a configurable location and enabled for all
applications or just predefined ones, as outlined here:
https://docs.microsoft.com/en-us/windows/win32/wer/collecting-user-mode-dumps.

Whenever a Windows application crashes, an event will be submitted to the ``Application``
Windows Event Log, having Event ID 1000. The entry will also include the process id,
the faulting module name and path as well as the exception code.

Please note that in order to analyze crash dumps, the debug symbols are required.
We're currently buidling Ceph using ``MinGW``, so by default ``DWARF`` symbols will
be embedded in the binaries. ``windbg`` does not support such symbols but ``gdb``
can be used.

``gdb`` can debug running Windows processes but it cannot open Windows minidumps.
The following ``gdb`` fork may be used until this functionality is merged upstream:
https://github.com/ssbssa/gdb/releases. As an alternative, ``DWARF`` symbols
can be converted using ``cv2pdb`` but be aware that this tool has limitted C++
support.

ceph tool
~~~~~~~~~

The ``ceph`` Python tool can't be used on Windows natively yet. With minor
changes it may run, but the main issue is that Python doesn't currently allow
using ``AF_UNIX`` on Windows: https://bugs.python.org/issue33408

As an alternative, the ``ceph`` tool can be used through Windows Subsystem
for Linux (WSL). For example, running Windows RBD daemons may be contacted by
using:

.. code:: bash

    ceph daemon /mnt/c/ProgramData/ceph/out/ceph-client.admin.61436.1209215304.asok help

IO counters
~~~~~~~~~~~

Along with the standard RBD perf counters, the ``libwnbd`` IO counters may be
retrieved using:

.. code:: PowerShell

    rbd-wnbd stats $imageName

At the same time, WNBD driver counters can be fetched using:

.. code:: PowerShell

    wnbd-client stats $mappingId

Note that the ``wnbd-client`` mapping identifier will be the full RBD image spec
(the ``device`` column of the ``rbd device list`` output).
