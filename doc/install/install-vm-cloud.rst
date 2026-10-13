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

On RHEL, CentOS Stream, Rocky Linux and other RPM-based distributions, run the
following command:

.. prompt:: bash $

   sudo dnf install qemu-kvm qemu-kvm-block-rbd qemu-img libvirt

To confirm that the installed QEMU supports RBD, check that ``rbd`` appears in
the list of supported formats:

.. prompt:: bash $

   qemu-img --help | grep -w rbd


Next steps
==========

To use QEMU with Ceph Block Devices, see `QEMU and Block Devices`_. To use
``libvirt`` with Ceph Block Devices, see `Using libvirt with Ceph Block
Device`_.

.. _QEMU and Block Devices: ../../rbd/qemu-rbd
.. _Using libvirt with Ceph Block Device: ../../rbd/libvirt
