About
-----

The Ceph client tools and libraries can be natively used on Windows. This avoids
the need of having additional layers such as iSCSI gateways or SMB shares,
drastically improving the performance.

.. _building:
Building
--------

At the moment, mingw gcc >= 8 is the only supported compiler for building ceph
components for Windows. Support for msvc and clang will be added soon.

`win32_build.sh`_ can be used for cross compiling Ceph and its dependencies.
It may be called from a Linux environment, including Windows Subsystem for
Linux. MSYS2 and CygWin may also work but those weren't tested.

This script currently supports Ubuntu 20.04 and openSUSE Tumbleweed, but it
may be easily adapted to run on other Linux distributions, taking into
account different package managers, package names or paths (e.g. mingw paths).

.. _win32_build.sh: win32_build.sh

The script accepts the following flags:

=================  ===============================  ===============================
Flag               Description                      Default value
=================  ===============================  ===============================
OS                 Host OS distribution, for mingw  ubuntu (also valid: suse)
                   and other OS specific settings.
TOOLCHAIN          Mingw toolchain: mingw-llvm or   mingw-llvm
                   mingw-gcc.
CEPH_DIR           The Ceph source code directory.  The same as the script.
BUILD_DIR          The directory where the          $CEPH_DIR/build
                   generated artifacts will be
                   placed.
DEPS_DIR           The directory where the Ceph     $CEPH_DIR/build.deps
                   dependencies will be built.
NUM_WORKERS        The number of workers to use     The number of vcpus
                   when building Ceph.              available
CLEAN_BUILD        Clean the build directory.
SKIP_BUILD         Run cmake without actually
                   performing the build.
SKIP_TESTS         Skip building Ceph tests.
SKIP_ZIP           If unset, we'll build a zip
                   archive containing the
                   generated binaries.
ZIP_DEST           Where to put a zip containing    $BUILD_DIR/ceph.zip
                   the generated binaries.
EMBEDDED_DBG_SYM   By default, the generated
                   archive will contain a .debug
                   subfolder, having the debug
                   symbols. If this flag is set,
                   the debug symbols will remain
                   embedded in the executables.
ENABLE_SHARED      Dynamically link Ceph libs.      ON
=================  ===============================  ===============================

The following command will build the binaries and add them to a zip archive
along with all the required DLLs. By default, the debug symbols are extracted
from the binaries and placed in the ".debug" folder of the archive.

.. code:: bash

    SKIP_TESTS=1 ./win32_build.sh

In order to disable a flag, such as ``CLEAN_BUILD``, leave it undefined.

``win32_build.sh`` will fetch dependencies using ``win32_deps_build.sh``. If
all dependencies are successfully prepared, this potentially time consuming
step will be skipped by subsequent builds. Be aware that you may have to do
a clean build (using the ``CLEAN_BUILD`` flag) when the dependencies change
(e.g. after switching to a more recent Ceph version by doing a ``git pull``).

Make sure to explicitly pass the "OS" parameter when directly calling
``win32_deps_build.sh``. Also, be aware of the fact that it will use the distro
specific package manager, which will require privileged rights.

.. _installing:
Installing
----------

MSI installer
=============

Using the MSI installer is the recommended way of installing Ceph on Windows.
Please check the `installation guide`_ for more details.

Manual installation
===================

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

Make sure to install Dokany 2.0.5 or later.

In order to map RBD images, the ``WNBD`` driver must be installed. Please
check out this page for more details about ``WNBD`` and the install process:
https://github.com/cloudbase/wnbd

The following command can be used to configure the ``ceph-rbd`` service.
Please update the ``rbd-wnbd.exe`` path accordingly.

.. code:: PowerShell

    New-Service -Name "ceph-rbd" `
                -Description "Ceph RBD Mapping Service" `
                -BinaryPathName "c:\ceph\rbd-wnbd.exe service" `
                -StartupType Automatic

Further reading
---------------

* `installation guide`_
* `RBD Windows documentation`_
* `Ceph Dokan documentation`_
* `Windows troubleshooting`_

.. _Ceph Dokan documentation: https://docs.ceph.com/en/latest/cephfs/ceph-dokan/
.. _RBD Windows documentation: https://docs.ceph.com/en/latest/rbd/rbd-windows/
.. _Windows troubleshooting: https://docs.ceph.com/en/latest/install/windows-troubleshooting
.. _installation guide: https://docs.ceph.com/en/latest/install/windows-install
