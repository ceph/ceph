==============
 Installation
==============

The Ceph Object Store is the foundation of all Ceph clusters, and it consists
primarily of two types of daemons: Object Storage Daemons (OSDs) and monitors.
The Ceph Object Store is based upon the concept of 
:abbr:`RADOS (Reliable Autonomic Distributed Object Store)`, which eliminates
single points of failure and delivers infinite scalability. For details on 
the architecture of Ceph and RADOS, refer to `Ceph Architecture`_. All Ceph
deployments have OSDs and monitors, so you should prepare your Ceph cluster
by focusing first on the object storage cluster.

.. raw:: html

	<table cellpadding="10"><colgroup><col width="33%"><col width="33%"><col width="33%"></colgroup><tbody valign="top"><tr><td><h3>Recommendations</h3>
	
To begin using Ceph in production, you should review our hardware
recommendations and operating system recommendations. Many of the
frequently-asked questions in our mailing list involve hardware-related
questions and how to install Ceph on various distributions. 

.. toctree::
   :maxdepth: 2

   Hardware Recommendations <hardware-recommendations>
   OS Recommendations <os-recommendations>

.. raw:: html 

	</td><td><h3>Installation</h3>

If you are deploying a Ceph cluster (that is, not developing Ceph),
install Ceph using our stable release packages. For testing, you 
may install development release and testing packages.

.. toctree::
   :maxdepth: 2

   Installing Debian/Ubuntu Packages <debian>
   Installing RPM Packages <rpm>
   Upgrading Ceph <upgrading-ceph>

.. raw:: html 

	</td><td><h3>Building Ceph from Source</h3>

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


.. raw:: html

	</td></tr></tbody></table>

.. _Ceph Architecture: ../architecture/
