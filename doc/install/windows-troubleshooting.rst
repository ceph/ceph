:orphan:

===============================
Troubleshooting Ceph on Windows
===============================

MSI installer
~~~~~~~~~~~~~

The MSI source code can be consulted here:
https://github.com/cloudbase/ceph-windows-installer

The following command can be used to generate MSI logs

.. code:: PowerShell

    msiexec.exe /i $msi_full_path /l*v! $log_file

WNBD driver installation failures will be logged here: ``C:\Windows\inf\setupapi.dev.log``.
A server reboot is required after uninstalling the driver, otherwise subsequent
install attempts may fail.

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
can be converted using ``cv2pdb`` but be aware that this tool has limited C++
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

Missing libraries
~~~~~~~~~~~~~~~~~

The Ceph tools can silently exit with a -1073741515 return code if one of the
required DLLs is missing or unsupported.

The `Dependency Walker`_ tool can be used to determine the missing library.

.. _Dependency Walker: https://www.dependencywalker.com/
