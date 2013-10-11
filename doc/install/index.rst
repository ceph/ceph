=======================
 Installation (Manual)
=======================

.. raw:: html

	<table><colgroup><col width="50%"><col width="50%"></colgroup><tbody valign="top"><tr><td><h3>Advanced Package Tool (APT)</h3>

If you are deploying a Ceph cluster on Debian or Ubuntu distributions,
use the instructions below to install packages manually.

.. toctree::
   :maxdepth: 2

   Installing Debian/Ubuntu Packages <debian>
   Installing on Calxeda Hardware <calxeda>
   Installing QEMU <qemu-deb>
   Installing libvirt <libvirt-deb>

.. raw:: html

	</td><td><h3>Redhat Package Manager (RPM) / Yellowdog Updater, Modified (YUM) </h3>
	
If you are deploying a Ceph cluster on Red Hat(rhel6), CentOS (el6), Fedora
17-19 (f17-f19), OpenSUSE 12 (opensuse12), and SLES (sles11) distributions, use
the instructions below to install packages manually.

.. toctree::
   :maxdepth: 2

   Installing RPM Packages <rpm>
   Installing YUM Priorities <yum-priorities>
   Installing QEMU <qemu-rpm>
   Installing libvirt <libvirt-rpm>

.. raw:: html

	</td></tr><tr><td><h3>Upgrading Ceph</h3>
	
If you are upgrading Ceph from a previous release, please read the the upgrade
documentation to ensure that you follow the proper upgrade sequence.

.. toctree::
   :maxdepth: 2

   Upgrading Ceph <upgrading-ceph>
	

.. raw:: html

	</td><td><h3>Building Ceph</h3>

You can build Ceph from source by downloading a release or cloning the ``ceph``
repository at github. If you intend to build Ceph from source, please see the
build pre-requisites first. Making sure you have all the pre-requisites
will save you time.

.. toctree::
   :maxdepth: 1

	Prerequisites <build-prerequisites>
	Get a Tarball <get-tarballs>
	Set Up Git <git>
	Clone the Source <clone-source>
	Build the Source <building-ceph>
	Install CPU Profiler <cpu-profiler>
	Build a Package <build-packages>
	Contributing Code <contributing>

See the `Development`_ section for additional development details.

.. raw:: html

	</td></tr></tbody></table>
	
.. _Development: ../../dev