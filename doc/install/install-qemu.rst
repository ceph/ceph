==============
 Install QEMU
==============

QEMU KVM can interact with Ceph Block Devices via ``librbd``, which is an
important feature for using Ceph with cloud platforms. Once you install QEMU,
see `QEMU and Block Devices`_ for usage. 


Debian Packages
===============

QEMU packages are incorporated into Ubuntu 12.04 Precise Pangolin and later
versions. To  install QEMU, execute the following:: 

	sudo apt-get install qemu
	

RPM Packages
============

To install QEMU with ``yum``, you must ensure that you have 
``yum-plugin-priorities`` installed. See `Installing YUM Priorities`_
for details.

To install QEMU, execute the following:

#. Create a ``/etc/yum.repos.d/ceph-qemu.conf`` file with the following 
   contents:: 

	[ceph-qemu]
	name=Ceph Packages for QEMU
	baseurl=http://ceph.com/packages/ceph-extras/rpm/centos6.3/$basearch
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc
	
	[ceph-qemu-noarch]
	name=Ceph QEMU noarch
	baseurl=http://ceph.com/packages/ceph-extras/rpm/centos6.3/noarch
	enabled=1
	priority=2	
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc
	
	[ceph-qemu-source]
	name=Ceph QEMU Sources
	baseurl=http://ceph.com/packages/ceph-extras/rpm/centos6.3/SRPMS
	enabled=1
	priority=2
	gpgcheck=1
	type=rpm-md
	gpgkey=https://ceph.com/git/?p=ceph.git;a=blob_plain;f=keys/release.asc

#. Update your repositories. :: 

	sudo yum update

#. Install QEMU for Ceph. :: 

	sudo yum install qemu-kvm qemu-kvm-tools qemu-img
	
#. Install additional QEMU packages (optional):: 

	sudo yum install qemu-guest-agent qemu-guest-agent-win32
	


Building QEMU
=============

To build QEMU from source, use the following procedure::

	cd {your-development-directory}
	git clone git://git.qemu.org/qemu.git
	cd qemu
	./configure --enable-rbd
	make; make install


.. _QEMU and Block Devices: ../../rbd/qemu-rbd
.. _Installing YUM Priorities: ../yum-priorities