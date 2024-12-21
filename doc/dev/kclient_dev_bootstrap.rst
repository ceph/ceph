Kernel Development in Virtual Machines
======================================

Kernel development is typically conducted within virtual machines (VMs) due to
the unique safety, flexibility, and control they provide. Any error during kernel
development can lead to system crashes, data loss, or even hardware damage. By
using VMs, we isolate the kernel environment from the dev machine's operating
system, allowing testing of potentially unstable code without taking undue
risks. Using VMs accelerates development iterations by enabling rapid testing
and easy rollback through snapshots, allowing developers to quickly test
changes and revert to previous states without lengthy setup times.
Additionally, VMs provide controlled hardware emulation, enabling the testing
of kernel functionality across various simulated architectures and
configurations (e.g., multiple NICs or storage devices).

This document focuses on using `QEMU <https://www.qemu.org/docs/master/>`_,
which is available on Linux and WSL2 (Windows). The principles discussed here
apply to native Windows and to macOS environments as well.

Installing a QEMU VM Using an ISO Image
---------------------------------------

Preparation
^^^^^^^^^^^

To install a virtual machine in QEMU using an ISO image, first ensure that QEMU
is installed on your system. Tool installation is beyond the scope of this
document and will vary depending on your Linux distribution, so consult your
package manager’s documentation to install the necessary QEMU packages.

Download the ISO and Create a Virtual Disk
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Begin by downloading the ISO file of the operating system you wish to install,
such as a Linux distribution like Ubuntu, Fedora, or CentOS. After the ISO is
ready, create a virtual disk for the OS installation using `qemu-img`, which
allows for flexible disk management and supports features like snapshots. You
can create a 128 GB virtual disk in the QCOW2 format with the following
command:

.. prompt:: bash #

   qemu-img create -f qcow2 my_vm_disk.qcow2 128G

Boot and Install the OS
Next, boot the VM from the ISO to install the OS by using the `qemu-system` command, specifying both the ISO and the virtual disk:

.. code-block:: bash

        qemu-system-x86_64    -m 2048                                   \
                        -enable-kvm                                     \
                        -boot d                                         \
                        -cdrom /path/to/your.iso                        \
                        -drive file=my_vm_disk.qcow2,format=qcow2       \
                        -vnc :1

In this command:

- ``-m 2048`` allocates 2 GB of memory.
- ``-enable-kvm`` enables hardware acceleration for better performance if
  available.
- ``-boot d`` specifies booting from the CD-ROM drive (the ISO image).
- ``-cdrom /path/to/your.iso`` provides the path to the ISO.
- ``-drive file=my_vm_disk.qcow2,format=qcow2`` links the virtual disk file.
- ``-vnc :1`` enables VNC access on display `1`, which corresponds to
  ``localhost:5901`` (for VNC viewers, this address would be ``localhost:1``).


Finishing Up
^^^^^^^^^^^^

To access the VM display, connect to ``localhost:5901`` with a VNC client, where
you will be able to interact with the VM’s graphical installer.

After the VM boots, proceed with the OS installation process as prompted. After
the OS is fully installed, you can remove the ISO reference from future
commands in order to boot directly from the virtual disk.

SSH Port Forwarding for Remote VNC Access
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you need to access the VM’s VNC display from a remote machine, you can set
up SSH port forwarding to securely tunnel the VNC connection over SSH. This
allows you to access VNC as if it were on your local network, even when
connecting remotely.

On your **local machine** (from which you want to access the remote VM), run
the following SSH command:

.. prompt:: bash #

   ssh -L 5901:localhost:5901 user@remote_host

In this command:

- ``-L 5901:localhost:5901`` sets up local port forwarding, mapping your local
  port ``5901`` to ``localhost:5901`` on the remote host.
- ``user@remote_host`` is the SSH username and address of the remote host where
  the VM is running.

Once connected, open your VNC client on the local machine and connect to
``localhost:5901``. This setup will forward the VNC traffic securely through
SSH, allowing you to interact with the VM from a remote location.

Differential Copies with QCOW2 and qemu-img
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The QCOW2 (QEMU Copy On Write) disk format in QEMU is highly versatile, as it
supports snapshotting and differential copies. With ``qemu-img``, you can
create a differential copy (or "backing file") that references a base image
without duplicating the entire disk. This approach allows for efficient storage
and versioning of virtual machine states.

To create a differential image, use ``qemu-img`` to specify a new QCOW2 image
with a backing file. For example:

.. prompt:: bash #

  qemu-img create -f qcow2 -b base_image.qcow2 differential_image.qcow2

Here:

- ``-f qcow2`` specifies the QCOW2 format.
- ``-b base_image.qcow2`` sets ``base_image.qcow2`` as the backing file, which
  holds the base disk state.
- ``differential_image.qcow2`` will store only changes made since the base
  image, saving disk space.

When the VM runs with ``differential_image.qcow2``, QEMU reads data from the
base file unless there are modifications in the differential image. This allows
you to maintain a series of snapshots, with each differential image capturing
incremental changes, useful for testing, development iterations, or backup
purposes.

See :ref:`doc-dev-9p-share`.


Launch the VM with TAP Networking
---------------------------------

Enabling ``vhost`` in QEMU with TAP networking provides a high-performance
network interface for virtual machines by offloading packet processing to the
host kernel. Follow these steps to bring up a QEMU VM with TAP networking and
``vhost`` enabled.


Create a TAP Interface
^^^^^^^^^^^^^^^^^^^^^^

Begin by creating a TAP interface on the host, which will act as the network
interface for the VM.

.. code-block:: bash 

        sudo ip tuntap add dev tap0 mode tap
        sudo ip link set tap0 up

- ``ip tuntap add dev tap0 mode tap``: Creates a TAP interface named ``tap0``.
- ``ip link set tap0 up``: Brings the TAP interface online.

Configure the TAP Interface with a Bridge
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you need the VM to access the outside network, connect ``tap0`` to a bridge
(such as ``virbr0``) on the host.

.. code-block:: bash 

        sudo ip link add name virbr0 type bridge
        sudo ip link set virbr0 up
        sudo ip link set tap0 master virbr0

- ``ip link add name virbr0 type bridge``: Creates a new bridge interface
  called ``virbr0``.
- ``ip link set virbr0 up``: Brings the bridge interface online.
- ``ip link set tap0 master virbr0``: Connects the ``tap0`` TAP interface to
  the ``virbr0`` bridge, allowing network traffic to flow between the VM and
  external networks.

Launch the QEMU VM with TAP and vhost
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Start the VM with ``vhost=on`` to enable vhost for the TAP interface, improving
packet processing performance.

.. code-block:: bash 

        sudo qemu-system-x86_64                                                                  \
                                -enable-kvm                                                      \
                                -m 4096                                                          \
                                -cpu host                                                        \
                                -netdev tap,id=net0,ifname=tap0,script=no,downscript=no,vhost=on \
                                -device virtio-net-pci,netdev=net0                               \
                                -drive file=$QCOW,format=qcow2                                   \
                                -fsdev local,id=shared_folder,path=$SHR,security_model=none      \
                                -device virtio-9p-pci,fsdev=shared_folder,mount_tag=hostshare    \
                                -daemonize \
                                -vnc :1

- ``-netdev tap,id=net0,ifname=tap0,script=no,downscript=no,vhost=on``: Sets up
  TAP networking with ``vhost`` enabled.
- ``vhost=on``: offloads packet processing to the host kernel for improved
  performance.
- ``-device virtio-net-pci,netdev=net0``: Configures a ``virtio`` network device
  for the VM, linked to the ``net0`` network backend.
- ``-daemonize``: The qemu process runs in a background releasing the shell.

Sepia Access and DNS Resolution
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The VMs on ``virbr0`` will have access to Sepia’s resources as if they were
connected directly, benefiting from the `VPN`_ tunnel established on the host.
The VMs will share the host’s DNS resolution, enabling seamless name resolution
for addresses within the Sepia network.

Build the Kernel
----------------

.. note:: The text assumes all prerequisites for building the kernel are
   installed both on the host and guest.

Clone the Git Repository
^^^^^^^^^^^^^^^^^^^^^^^^

Start by cloning the kernel source repository into the shared folder so that it
is accessible from both the host and guest.

.. code-block:: bash

    git init linux && cd linux
    git remote add torvalds git://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git
    git remote add ceph https://github.com/ceph/ceph-client.git
    git fetch && git checkout torvalds/master

Configure the Kernel
^^^^^^^^^^^^^^^^^^^^

From within the **guest VM**, copy the current kernel configuration file from
``/boot/config-$(uname -r)`` to ``.config`` in the kernel source directory.
This ensures that the new build will have a configuration similar to the
currently running guest kernel.

.. code-block:: bash

   cp /boot/config-$(uname -r) /path/to/linux/.config
   make localmodconfig
   make olddefconfig

- ``make localmodconfig``: Updates ``.config`` to include only the modules
  currently loaded on the host.
- ``make olddefconfig``: Fills in any missing configuration options in
  ``.config`` with default values.

For more information on this process, refer to `the localmodconfig
documentation at
<https://www.kernel.org/doc/Documentation/admin-guide/README.rst>`_. This
command will ensure that all necessary modules are configured.

.. note:: You can alternatively use the `Ceph Kernel QA Config`_ for building
   the kernel.

We now have a kernel config with reasonable defaults for the architecture
you're building on. The next thing to do is to enable configs which will build
Ceph and provide the functionality needed to do testing.

.. code-block:: bash

    cat > ~/.ceph.config <<EOF
    CONFIG_CEPH_LIB=m
    CONFIG_CEPH_FS=m
    CONFIG_CEPH_FSCACHE=y
    CONFIG_CEPH_FS_POSIX_ACL=y
    CONFIG_CEPH_FS_SECURITY_LABEL=y
    CONFIG_CEPH_LIB_PRETTYDEBUG=y
    CONFIG_DYNAMIC_DEBUG=y
    CONFIG_DYNAMIC_DEBUG_CORE=y
    CONFIG_FRAME_POINTER=y
    CONFIG_FSCACHE=y
    CONFIG_FSCACHE_STATS=y
    CONFIG_FS_ENCRYPTION=y
    CONFIG_FS_ENCRYPTION_ALGS=y
    CONFIG_KGDB=y
    CONFIG_KGDB_SERIAL_CONSOLE=y
    CONFIG_XFS_FS=y
    EOF

In addition to enabling Ceph-related configs, we are also enabling some useful
debug configs and XFS (as an alternative to ext4 if needed for our root file
system).

Merge the configs:


.. code-block:: bash 


   scripts/kconfig/merge_config.sh .config ~/.ceph.config


Build the Kernel
^^^^^^^^^^^^^^^^

Compile the kernel using one of the following commands. Adjust the ``-j`` value
as needed to match the number of cores available on the host for parallel
processing.

.. code-block:: bash

   make

See :ref:`doc-dev-kclient-kernel-build-alternate` for another example of a
``make`` command that compiles the kernel.


Install Modules on the Guest
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note:: If you are not using kernel modules, then these steps are optional.

After the build completes, return to the guest and install the compiled modules
and kernel:

.. prompt:: bash $

   sudo make modules_install && sudo make install

This process installs the kernel modules and copy the kernel image to the
appropriate locations on the guest for booting.

.. note:: When cephfs is compiled as a module, a reboot may not be needed. The
   modifed kernel module can be reloaded using modprobe.

Testing kernel changes in teuthology
------------------------------------

There are three  static branches in the `ceph kernel git repository`_ managed
by the Ceph team:

* `for-linus <https://github.com/ceph/ceph-client/tree/for-linus>`_: A branch
  managed by the primary Ceph maintainer to share changes with Linus Torvalds
  (upstream). Do not push to this branch.
* `master <https://github.com/ceph/ceph-client/tree/master>`_: A staging ground
  for patches planned to be sent to Linus. Do not push to this branch.
* `testing <https://github.com/ceph/ceph-client/tree/testing>`_ A staging
  ground for miscellaneous patches that need wider QA testing (via nightlies or
  regular Ceph QA testing). Push patches you believe to be nearly ready for
  upstream acceptance.

You may also push a ``wip-$feature`` branch to the ``ceph-client.git``
repository which will be built by Jenkins. Then view the results of the build
in `Shaman <https://shaman.ceph.com/builds/kernel/>`_.

After a kernel branch is built, you can test it via the ``fs`` CephFS QA suite:

.. code-block:: bash 

   teuthology-suite ... --suite fs --kernel wip-$feature --filter k-testing


The ``k-testing`` filter looks for the fragment which normally sets the
``testing`` branch of the kernel for routine QA. That is, the ``fs`` suite
regularly runs tests against whatever is in the ``testing`` branch of the
kernel. We are overriding that choice of kernel branch via the ``--kernel
wip-$featuree`` switch.

.. note:: Without filtering for ``k-testing``, the ``fs`` suite will also run
   jobs using ceph-fuse or stock kernel, libcephfs tests, and other tests that
   may not be of interest to you when evaluating changes to the kernel.

The actual override is controlled using Lua merge scripts in the
``k-testing.yaml`` fragment. See that file for more details.


.. _VPN: https://wiki.sepia.ceph.com/doku.php?id=vpnaccess
.. _Ceph Kernel QA Config: https://github.com/ceph/ceph-build/tree/899d0848a0f487f7e4cee773556aaf9529b8db26/kernel/build
.. _ceph kernel git repository: https://github.com/ceph/ceph-client
