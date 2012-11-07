===================================
 Installing Debian/Ubuntu Packages
===================================

You may install stable release packages (for stable deployments),
development release packages (for the latest features), or development
testing packages (for development and QA only).  Do not add multiple
package sources at the same time.

Add Stable Release Packages
===========================

We build Debian and Ubuntu packages for each stable release of Ceph.  These
packages are recommended for anyone deploying Ceph in a production environment.

Packages are cryptographically signed with the ``release.asc`` key.
Add our release key to your system's list of trusted keys to avoid a
security warning::

	wget -q -O- https://raw.github.com/ceph/ceph/master/keys/release.asc | sudo apt-key add -

Add our package repository to your system's list of APT sources.  
See `the Debian repository`_ for a complete list of distributions 
supported. ::

	echo deb http://ceph.com/debian/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

For the European users there is also a mirror in the Netherlands at http://eu.ceph.com/ ::

	echo deb http://eu.ceph.com/debian/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

Add Development Release Packages
================================

Our development process generates a new release of Ceph every 3-4 weeks.
These packages are faster-moving than the stable releases, as they get
new features integrated quickly, while still undergoing several weeks of QA
prior to release.

Packages are cryptographically signed with the ``release.asc`` key.
Add our release key to your system's list of trusted keys to avoid a
security warning::

	wget -q -O- https://raw.github.com/ceph/ceph/master/keys/release.asc | sudo apt-key add -

Add our package repository to your system's list of APT sources.
See `the Debian repository`_ for a complete list of distributions
supported. ::

	echo deb http://ceph.com/debian-testing/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

For the European users there is also a mirror in the Netherlands at http://eu.ceph.com/ ::

	echo deb http://eu.ceph.com/debian-testing/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

Add Development Testing Packages
================================

We automatically build Debian and Ubuntu packages for current
development branches in the Ceph source code repository.  These
packages are intended for developers and QA only.

Packages are cryptographically signed with the ``autobuild.asc`` key.
Add our autobuild key to your system's list of trusted keys to avoid a
security warning::

	wget -q -O- https://raw.github.com/ceph/ceph/master/keys/autobuild.asc | sudo apt-key add -

Add our package repository to your system's list of APT sources, but replace ``{BRANCH}`` 
with the branch you'd like to use (e.g., chef-3, wip-hack, master, stable).
We support ``oneiric`` and ``precise`` distributions. ::

	echo deb http://gitbuilder.ceph.com/ceph-deb-$(lsb_release -sc)-x86_64-basic/ref/{BRANCH} $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

Installing Packages
===================

Once you have added either release or development packages to APT, 
you should update APT's database and install Ceph::

	sudo apt-get update && sudo apt-get install ceph


.. _the Debian repository: http://ceph.com/debian/dists
