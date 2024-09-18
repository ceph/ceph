===============================================
UADK Acceleration for Compression
===============================================

UADK is a framework for applications to access hardware accelerators in a
unified, secure, and efficient way. UADK is comprised of UACCE, libwd and many
other algorithm libraries.

See `Compressor UADK Support`_.


UADK in the Software Stack
==========================

UADK is a general-purpose user space accelerator framework that uses shared
virtual addressing (SVA) to provide a unified programming interface for hardware
acceleration of cryptographic and compression algorithms.

UADK includes Unified/User-space-access-intended Accelerator Framework (UACCE),
which enables hardware accelerators that support SVA to adapt to UADK.

Currently, HiSilicon Kunpeng hardware accelerators have been registered with
UACCE. Through the UADK framework, users can run cryptographic and compression
algorithms using hardware accelerators instead of CPUs, freeing up CPU computing
power and improving computing performance.

A user can access the hardware accelerators by performing user-mode operations on
the character devices, or the use of UADK can be done via frameworks that have
been enabled by others including UADK support (for example, OpenSSL* libcrypto*,
DPDK, and the Linux* Kernel Crypto Framework).

See `OpenSSL UADK Engine`_.

UADK Environment Setup
======================
UADK consists of UACCE, vendors’ drivers, and an algorithm layer. UADK requires the
hardware accelerator to support SVA, and the operating system to support IOMMU and
SVA. Hardware accelerators from different vendors are registered as different character
devices with UACCE by using kernel-mode drivers of the vendors.

::

          +----------------------------------+
          |                apps              |
          +----+------------------------+----+
               |                        |
               |                        |
       +-------+--------+       +-------+-------+
       |   scheduler    |       | alg libraries |
       +-------+--------+       +-------+-------+
               |                         |
               |                         |
               |                         |
               |                +--------+------+
               |                | vendor drivers|
               |                +-+-------------+
               |                  |
               |                  |
            +--+------------------+--+
            |         libwd          |
    User    +----+-------------+-----+
    --------------------------------------------------
    Kernel    +--+-----+   +------+
              | uacce  |   | smmu |
              +---+----+   +------+
                  |
              +---+------------------+
              | vendor kernel driver |
              +----------------------+
    --------------------------------------------------
             +----------------------+
             |   HW Accelerators    |
             +----------------------+

Configuration
=============

#. Kernel Requirement

User needs to make sure that UACCE is already supported in Linux kernel. The kernel version
should be at least v5.9 with SVA (Shared Virtual Addressing) enabled.

UACCE may be built as a module or built into the kernel. Here's an example to build UACCE
with hardware accelerators for the HiSilicon Kunpeng platform.

    .. prompt:: bash $

       CONFIG_IOMMU_SVA_LIB=y
       CONFIG_ARM_SMMU=y
       CONFIG_ARM_SMMU_V3=y
       CONFIG_ARM_SMMU_V3_SVA=y
       CONFIG_PCI_PASID=y
       CONFIG_UACCE=y
       CONFIG_CRYPTO_DEV_HISI_QM=y
       CONFIG_CRYPTO_DEV_HISI_ZIP=y

Make sure all these above kernel configurations are selected.

#. UADK enablement
If the architecture is aarch64, it will automatically download the UADK source code to build
the static library. If it runs on other architecture, user can enable it with build parameters
`-DWITH_UADK=true`

#. Manual Build UADK
As the above paragraph shows, the UADK is enabled automatically, no need to build manually.
For developer who is interested in UADK, you can refer to the below steps for building.

   .. prompt:: bash $ 

      git clone https://github.com/Linaro/uadk.git
      cd uadk
      mkdir build
      ./autogen.sh
      ./configure --prefix=$PWD/build
      make
      make install

   .. note:: Without –prefix, UADK will be installed to /usr/local/lib by
             default. If get error:"cannot find -lnuma", please install 
             the `libnuma-dev`.

#. Configure

   Edit the Ceph configuration file (usually ``ceph.conf``) to enable UADK
   support for *zlib* compression::

         uadk_compressor_enabled=true

   The default value in `global.yaml.in` for `uadk_compressor_enabled` is false.

.. _Compressor UADK Support: https://github.com/ceph/ceph/pull/58336
.. _OpenSSL UADK Engine: https://github.com/Linaro/uadk_engine
