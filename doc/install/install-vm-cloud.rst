=========================================
 Install Virtualization for Block Device
=========================================

If you intend to use Ceph Block Devices and the Ceph Storage Cluster as a
backend for Virtual Machines (VMs) or :term:`Cloud Platforms`, install QEMU/KVM
and ``libvirt`` from your distribution's packages. Ceph does not ship these
packages. QEMU accesses Ceph Block Devices through ``librbd``, and ``libvirt``
configures QEMU.


.. ditaa::

            +---------------------------------------------------+
            |                     libvirt                       |
            +------------------------+--------------------------+
                                     |
                                     | configures
                                     v
            +---------------------------------------------------+
            |                       QEMU                        |
            +---------------------------------------------------+
            |                      librbd                       |
            +---------------------------------------------------+
            |                     librados                      |
            +------------------------+-+------------------------+
            |          OSDs          | |        Monitors        |
            +------------------------+ +------------------------+


Install QEMU and libvirt
========================

On Debian and Ubuntu, RBD support for QEMU is in the ``qemu-block-extra``
package. Run the following command (replace ``qemu-system-x86`` with the
package for your host architecture):

.. prompt:: bash $

   sudo apt install qemu-system-x86 qemu-utils qemu-block-extra libvirt-daemon-system libvirt-clients

On RHEL, CentOS Stream, Rocky Linux and other Enterprise Linux (EL)
distributions, run the following command:

.. prompt:: bash $

   sudo dnf install qemu-kvm qemu-kvm-block-rbd qemu-img libvirt

To confirm that the installed QEMU supports RBD, list the options of the
``rbd`` format:

.. prompt:: bash $

   qemu-img create -f rbd -o help

The command prints ``Supported rbd options:`` followed by a list of options,
and creates nothing. If it prints ``qemu-img: Unknown file format 'rbd'``, the
driver is not installed: install ``qemu-block-extra`` on Debian and Ubuntu or
``qemu-kvm-block-rbd`` on EL distributions.


Next steps
==========

To use QEMU with Ceph Block Devices, see `QEMU and Block Devices`_. To use
``libvirt`` with Ceph Block Devices, see `Using libvirt with Ceph Block
Device`_.

.. _QEMU and Block Devices: ../../rbd/qemu-rbd
.. _Using libvirt with Ceph Block Device: ../../rbd/libvirt
