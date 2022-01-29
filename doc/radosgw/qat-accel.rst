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

2. The implementation for QAT based encryption is directly base on QAT API which
   is included the driver package. But QAT support for compression depends on
   QATzip project, which is a user space library which builds on top of the QAT
   API. Currently, QATzip speeds up gzip compression and decompression at the
   time of writing.

See `QATzip`_.

Implementation
==============
1. QAT based Encryption for RGW 

`OpenSSL support for RGW encryption`_ has been merged into Ceph, and Intel also
provides one `QAT Engine`_ for OpenSSL. So, theoretically speaking, QAT based
encryption in Ceph can be directly supported through OpenSSl+QAT Engine.

But the QAT Engine for OpenSSL currently supports chained operations only, and
so Ceph will not be able to utilize QAT hardware feature for crypto operations
based on OpenSSL crypto plugin. As a result, one QAT plugin based on native
QAT API is added into crypto framework.

2. QAT Support for Compression

As mentioned above, QAT support for compression is based on QATzip library in
user space, which is designed to take full advantage of the performance provided
by QuickAssist Technology. Unlike QAT based encryption, QAT based compression
is supported through a tool class for QAT acceleration rather than a compressor
plugin. The common tool class can transparently accelerate the existing compression
types, but only zlib compressor can be supported at the time of writing. So
user is allowed to use it to speed up zlib compressor as long as the QAT
hardware is available and QAT is capable to handle it.

Configuration
=============
#. Prerequisites

   Make sure the QAT driver with version v1.7.L.4.14.0 or higher has been installed.
   Remember to set an environment variable "ICP_ROOT" for your QAT driver package
   root directory. 

   To enable the QAT based encryption and compression, user needs to modify the QAT
   configuration files. For example, for Intel QuickAssist Adapter 8970 product, revise 
   c6xx_dev0/1/2.conf in the directory ``/etc/`` and keep them the same, e.g.:

   .. code-block:: ini
        
      #...
      # User Process Instance Section
      ##############################################
      [CEPH]
      NumberCyInstances = 1
      NumberDcInstances = 1
      NumProcesses = 8
      LimitDevAccess = 1
      # Crypto - User instance #0
      Cy0Name = "SSL0"
      Cy0IsPolled = 1
      # List of core affinities
      Cy0CoreAffinity = 0
       
      # Data Compression - User instance #0
      Dc0Name = "Dc0"
      Dc0IsPolled = 1
      # List of core affinities
      Dc0CoreAffinity = 0

#. QAT based Encryption for RGW 

   The CMake option ``WITH_QAT=ON`` must be configured. If you build Ceph from
   source code (see: :ref:`build-ceph`), navigate to your cloned Ceph repository 
   and execute the following:

   .. prompt:: bash $ 

      cd ceph
      ./do_cmake.sh -DWITH_QAT=ON
      cd build
      ininja

   .. note::
     The section name of the QAT configuration files must be ``CEPH`` since 
     the section name is set as "CEPH" in Ceph cropto source code.
  
   Then, edit the Ceph configuration file to make use of QAT based crypto plugin::

      plugin crypto accelerator = crypto_qat

#. QAT Support for Compression

   Before starting, make sure both QAT driver and `QATzip`_  have been installed. Besides 
   "ICP_ROOT", remember to set the environment variable "QZ_ROOT" for the root directory
   of your QATzip source tree.

   The following CMake options have to be configured to trigger QAT based compression
   when building Ceph:
  
   .. prompt:: bash $

      ./do_cmake.sh -DWITH_QAT=ON -DWITH_QATZIP=ON

   Then, set an environment variable to clarify the section name of User Process Instance
   Section in QAT configuration files, e.g.:
  
   .. prompt:: bash $

      export QAT_SECTION_NAME=CEPH

   Next, edit the Ceph configuration file to enable QAT support for compression::

      qat compressor enabled=true


.. _QAT Support for Compression: https://github.com/ceph/ceph/pull/19714
.. _QAT based Encryption for RGW: https://github.com/ceph/ceph/pull/19386
.. _Intel Quickassist Technology: https://01.org/intel-quickassist-technology
.. _QATzip: https://github.com/intel/QATzip
.. _OpenSSL support for RGW encryption: https://github.com/ceph/ceph/pull/15168
.. _QAT Engine: https://github.com/intel/QAT_Engine
