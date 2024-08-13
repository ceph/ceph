===============================================
QAT Acceleration for Encryption and Compression
===============================================

Intel QAT (QuickAssist Technology) can provide extended accelerated encryption
and compression services by offloading the actual encryption and compression
request(s) to the hardware QuickAssist accelerators, which are more efficient
in terms of cost and power than general purpose CPUs for those specific
compute-intensive workloads.

See `QAT Support for Compression`_ and `QAT based Encryption for RGW`_.


QAT in the Software Stack 
=========================

Application developers can access QuickAssist features through the QAT API.
The QAT API is the top-level API for QuickAssist technology, and enables easy
interfacing between the customer application and the QuickAssist acceleration
driver.

The QAT API accesses the QuickAssist driver, which in turn drives the
QuickAssist Accelerator hardware. The QuickAssist driver is responsible for
exposing the acceleration services to the application software.

A user can write directly to the QAT API, or the use of QAT can be done via
frameworks that have been enabled by others including Intel (for example, zlib*,
OpenSSL* libcrypto*, and the Linux* Kernel Crypto Framework).

QAT Environment Setup
=====================
1. QuickAssist Accelerator hardware is necessary to make use of accelerated
   encryption and compression services. And QAT driver in kernel space have to
   be loaded to drive the hardware.

The driver package can be downloaded from `Intel Quickassist Technology`_.

The QATlib can be downloaded from `qatlib`_, which is used for the in-tree QAT
driver.

   .. note::
      The out-of-tree QAT driver is gradually being migrated to an intree driver+QATlib.

2. The implementation of QAT-based encryption is directly based on the QAT API,
   which is included the driver package. However, QAT support for compression
   depends on the QATzip project, which is a userspace library that builds on
   top of the QAT API. At the time of writing (July 2024), QATzip speeds up
   gzip compression and decompression.

See `QATzip`_.

Implementation
==============
1. QAT based Encryption for RGW 

`OpenSSL support for RGW encryption`_ has been merged into Ceph, and Intel also
provides one `QAT Engine`_ for OpenSSL. Theoretically, QAT-based encryption in
Ceph can be directly supported through the OpenSSl+QAT Engine.

But the QAT Engine for OpenSSL currently supports chained operations only, and
so Ceph will not be able to utilize QAT hardware feature for crypto operations
based on OpenSSL crypto plugin. As a result, one QAT plugin based on native
QAT API is added into crypto framework.

2. QAT Support for Compression

As mentioned above, QAT support for compression is based on the QATzip library
in user space, which is designed to take full advantage of the performance that
QuickAssist Technology provides. Unlike QAT-based encryption, QAT-based
compression is supported through a tool class for QAT acceleration rather than
a compressor plugin. This common tool class can transparently accelerate the
existing compression types, but only the zlib compressor is supported at the
time of writing. This means that this tool class can be used to speed up
the zlib compressor if QAT hardware is available.

Configuration
=============
1. QAT based Encryption for RGW 

Edit the Ceph configuration file to make use of QAT based crypto plugin::

    plugin crypto accelerator = crypto_qat

2. QAT Support for Compression

One CMake option have to be used to trigger QAT based compression::

    -DWITH_QATZIP=ON

   There are some prerequisites for using QATlib. Make sure that your system
   meets the `QATlib System Requirements`_ .

   * To properly use the QATlib library, the Intel VT-d and SR-IOV parameters
     must be enabled in the platform BIOS.
   * Some QATlib features require a recent kernel driver or firmware version.
     See `QATlib Kernel Driver Releases`_.
   * The supported platform contains a 4xxx Intel Communications device or
     newer.
   * The ``intel_iommu`` parameter must be enabled. Verify that this setting is
     enabled by running the following commands:

     .. prompt:: bash $

        cat /proc/cmdline | grep intel_iommu=on
        sudo sh -c 'echo "@qat - memlock 204800" >> /etc/security/limits.conf'
        sudo su -l $USER

   For configuration and Tuning see `QATlib Configuration and Tuning`_.

#. QAT-based Encryption for RGW 

   The CMake option ``WITH_QATDRV=ON`` must be set. If you build Ceph from
   source code (see: :ref:`build-ceph`), navigate to your cloned Ceph repository 
   and execute the following:

   .. prompt:: bash $ 

      cd ceph
      ./do_cmake.sh -DWITH_QATDRV=ON
      cd build
      ininja

   .. note:: The section name in QAT configuration files must be ``CEPH``,
      because the section name is set to ``CEPH`` in the Ceph crypto source code.
  
   Edit the Ceph configuration file (usually ``ceph.conf``) to make use of the
   QAT-based crypto plugin::

      plugin crypto accelerator = crypto_qat

#. QAT Support for Compression

   **For out-of-tree QAT**

   For the out-of-tree QAT driver package, before building ensure that both the QAT
   driver and `QATzip`_  have been installed. In addition to ``ICP_ROOT``,
   set the environment variable ``QZ_ROOT`` to the root directory of your QATzip
   source tree.

   The following CMake options must be configured to trigger QAT-based
   compression when building Ceph:
  
   .. prompt:: bash $

      ./do_cmake.sh -DWITH_QATDRV=ON -DWITH_QATZIP=ON -DWITH_SYSTEM_QATZIP=ON -DWITH_QATLIB=OFF

   Set an environment variable to clarify the section name of the User Process
   Instance Section in the QAT configuration files. For example: 
  
   .. prompt:: bash $

      export QAT_SECTION_NAME=CEPH

   **For in-tree QAT**

   For in-tree QAT, ensure that your system meets the `QATlib System
   Requirements`_.  QATlib can be installed from pre-built packages or from
   source code.  See `QATlib Installation`_ . After QATlib is installed, you
   can run ``cpa_sample_code`` to check if the QAT environment is OK.

   If you are using QATlib source code, the Ceph `cmake` build enables the
   qatlib and qatzip options by default. Our normal compilation
   already includes QAT-compressor-related code.

   .. prompt:: bash $

      ./do_cmake.sh

   If you are using pre-built packages installed on the system, the following
   CMake options must be configured when building Ceph:

   .. prompt:: bash $

      ./do_cmake.sh -DWITH_SYSTEM_QATLIB=ON -DWITH_SYSTEM_QATZIP=ON


   **For both out-of-tree QAT and in-tree QAT**

   Edit Ceph's central config DB or configuration file (usually ``ceph.conf``) to enable QAT
   support for *zlib* compression::

      qat compressor enabled=true

   Set the RGW compression method:

   .. prompt:: bash $

      # for storage class(STANDARD)
      radosgw-admin zone placement modify --rgw-zone=default --placement-id=default-placement --compression=zlib
      # or create a new storage class(COLD) and define data pool(default.rgw.cold.data)
      radosgw-admin zonegroup placement add --rgw-zonegroup default --placement-id default-placement --storage-class COLD
      radosgw-admin zone placement add --rgw-zone default --placement-id default-placement --storage-class COLD --compression zlib --data-pool default.rgw.cold.data

CONFIG REFERENCE
================
The following QAT-related settings can be added to the Ceph configuration file
(usually `ceph.conf`) under the ``[client.rgw.{instance-name}]`` section.

.. confval:: qat_compressor_session_max_number
.. confval:: qat_compressor_busy_polling

    qat compressor enabled=true


.. _QAT Support for Compression: https://github.com/ceph/ceph/pull/19714
.. _QAT based Encryption for RGW: https://github.com/ceph/ceph/pull/19386
.. _Intel Quickassist Technology: https://01.org/intel-quickassist-technology
.. _QATzip: https://github.com/intel/QATzip
.. _OpenSSL support for RGW encryption: https://github.com/ceph/ceph/pull/15168
.. _QAT Engine: https://github.com/intel/QAT_Engine
