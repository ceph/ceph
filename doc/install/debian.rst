===================================
 Installing Debian/Ubuntu Packages
===================================

You may install release packages (recommended) or development 
packages (for development and QA only). Do not add both at the same time.

Add Release Packages
--------------------
We build Debian and Ubuntu packages for each stable release of Ceph.
Packages are cryptographically signed with the ``release.asc`` key.

Add our release key to your system's list of trusted keys to avoid a
security warning::

	wget -q -O- https://raw.github.com/ceph/ceph/master/keys/release.asc | sudo apt-key add -

Add our package repository to your system's list of APT sources.  
See `the Debian repository`_ for a complete list of distributions 
supported. ::

	echo deb http://ceph.com/debian/ $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

Add Development Packages
------------------------
We build Debian and Ubuntu packages for development releases of Ceph.
These packages are intended for developers and QA only. Packages are 
cryptographically signed with the ``autobuild.asc`` key.

Add our autobuild key to your system's list of trusted keys to avoid a
security warning::

	wget -q -O- https://raw.github.com/ceph/ceph/master/keys/autobuild.asc \ | sudo apt-key add -

Add our package repository to your system's list of APT sources, but replace ``{BRANCH}`` 
with the branch you'd like to use (e.g., chef-3, wip-hack, master, stable).
We support ``maveric``, ``oneiric``, and ``precise`` distributions. ::

	echo deb http://gitbuilder.ceph.com/ceph-deb-$(lsb_release -sc)-x86_64-basic/ref/{BRANCH} $(lsb_release -sc) main | sudo tee /etc/apt/sources.list.d/ceph.list

Installing Packages
-------------------
Once you have added either release or development packages to APT, 
you should update APT's database and install Ceph::

	sudo apt-get update && sudo apt-get install ceph


.. _the Debian repository: http://ceph.com/debian/dists
