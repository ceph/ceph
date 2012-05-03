===================================
 Installing Debian/Ubuntu Packages
===================================

We build Debian and Ubuntu packages for each stable release of Ceph.
Packages are cryptographically signed with the ``release.asc`` key.

Add our release key to your system's list of trusted keys to avoid a
security warning::

	wget -q -O- https://raw.github.com/ceph/ceph/master/keys/release.asc | sudo apt-key add -

Add our package repository to your system's list of APT sources::

        echo deb http://ceph.com/debian/ {DISTRO} main | sudo tee /etc/apt/sources.list.d/ceph.list

Replace ``{DISTRO}`` with the code name for Debian/Ubuntu distribution
(*e.g.,* ``precise`` for Ubuntu ``12.04``).  See `the Debian
repository <http://ceph.newdream.net/debian/dists>`_ for a full list
of distributions supported.  Replace ``{ARCH}`` with either ``amd64``
for 64-bit systems or ``i386`` for 32-bit systems.

Update APT's database::

	sudo apt-get update

Install Ceph::

        sudo apt-get install ceph

